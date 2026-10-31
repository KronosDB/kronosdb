//! Startup hardware checks.
//!
//! KronosDB's durability and latency claims assume a local disk with a fast
//! `fdatasync`, enough memory for its caches, and a few CPUs. This module
//! looks at the machine once, at startup, and does two things with what it
//! finds:
//!
//! * **Measures the disk** with a short fdatasync probe, whose p99 decides
//!   what `ack-mode = auto` means in a cluster (see [`crate::AckMode`]).
//! * **Reports suboptimal hardware** as [`Finding`]s: a data dir on tmpfs or a
//!   network filesystem, a slow fsync, macOS, too little memory for the
//!   configured caches, swap in use, a cgroup CPU limit, or too few cores.
//!   Findings are logged at WARN (ERROR for memory-backed storage) unless the
//!   node runs in dev mode, where they are logged at INFO. The server also
//!   exposes them on `/metrics` and in the admin JSON API.
//!
//! Every check is best-effort and cheap: a probe that fails or hangs, a file
//! under `/proc` that is missing, or a platform without the interface leaves
//! that fact unknown and silences the checks that need it. Nothing here can
//! fail startup. The probe is the only part that does real work, and it is
//! bounded to [`PROBE_BUDGET`] (plus [`PROBE_TIMEOUT`] if the disk hangs).

use std::fmt::Write as _;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::{AckDecision, AckMode, decide_acks};

// ─── Disk probe ──────────────────────────────────────────────────────────

/// Scratch file the probe writes in the data dir. A fixed name is safe: the
/// probe runs after the data-dir lock is taken, and truncates what it finds.
/// Context discovery ignores files in the data dir.
pub const PROBE_FILE: &str = ".disk-probe";

/// Writes timed at most. A p99 only differs from the maximum from 100
/// samples up, so one outlier does not decide the ack mode.
const PROBE_SAMPLES: usize = 100;
/// Samples taken even when the time budget is already spent, so a slow disk
/// still yields a median and a worst case.
const PROBE_MIN_SAMPLES: usize = 8;
/// One page: the smallest write a durable commit can be, and well inside the
/// 512 B – 8 KB range the fio numbers in the bench notes cover.
const PROBE_BLOCK: usize = 4096;
/// Stop sampling after this long (once [`PROBE_MIN_SAMPLES`] are in). A fast
/// disk finishes all samples in a few milliseconds; at 4 ms per sync this
/// still allows ~75, and macOS (~10 ms) ~30.
pub const PROBE_BUDGET: Duration = Duration::from_millis(300);
/// Give up waiting after this long. A sync that never returns (a dying
/// network mount) must not hold startup; the probe thread is left behind.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// Warn about slow fsync when the probe's p99 reaches this. etcd's own guidance
/// is a WAL fsync p99 under 10 ms; below 5 ms a disk is merely not fast,
/// which `ack-mode = auto` already accounts for.
pub const SLOW_FSYNC_P99: Duration = Duration::from_millis(5);

/// What the fdatasync probe measured.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskProbe {
    /// Timed write+fdatasync pairs.
    pub samples: usize,
    pub p50: Duration,
    pub p99: Duration,
    pub max: Duration,
    /// Wall time of the whole probe, setup included.
    pub elapsed: Duration,
}

/// Measures write+fdatasync latency on the filesystem that holds `dir`, which
/// is where the segments go. Uses the engine's own sync call (`fdatasync` on
/// Linux, `F_FULLFSYNC` on macOS) and its preallocation, over random data so
/// compressing or deduplicating storage cannot flatter the result. Returns
/// the reason on failure or timeout.
pub fn probe_disk(dir: &Path) -> Result<DiskProbe, String> {
    probe_disk_with(dir, PROBE_SAMPLES, PROBE_BUDGET, PROBE_TIMEOUT)
}

fn probe_disk_with(
    dir: &Path,
    samples: usize,
    budget: Duration,
    timeout: Duration,
) -> Result<DiskProbe, String> {
    let path = dir.join(PROBE_FILE);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name("disk-probe".into())
        .spawn(move || {
            let _ = tx.send(run_probe(&path, samples, budget).map_err(|e| e.to_string()));
        })
        .map_err(|e| format!("could not start the probe thread: {e}"))?;
    match rx.recv_timeout(timeout) {
        Ok(result) => result,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Err(format!(
            "a write and fdatasync did not finish within {}",
            fmt_duration(timeout)
        )),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            Err("the probe thread died".to_string())
        }
    }
}

struct RemoveOnDrop(PathBuf);

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn run_probe(path: &Path, max_samples: usize, budget: Duration) -> std::io::Result<DiskProbe> {
    let started = Instant::now();
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    let _cleanup = RemoveOnDrop(path.to_path_buf());
    // Same as a segment: preallocated up front, then written front to back,
    // so the timed syncs include whatever the filesystem does to convert
    // reserved space on first write. The allocation itself is made durable
    // before the clock starts.
    crate::segment::writer::preallocate(&file, (max_samples * PROBE_BLOCK) as u64)
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    file.sync_all()?;

    let mut block = vec![0u8; PROBE_BLOCK];
    let mut rng = (started.elapsed().as_nanos() as u64) ^ (u64::from(std::process::id()) << 32) | 1;
    let mut times = Vec::with_capacity(max_samples);
    let clock = Instant::now();
    while times.len() < max_samples {
        // Fresh bytes every write (xorshift64*), outside the timed region.
        for chunk in block.chunks_mut(8) {
            rng ^= rng >> 12;
            rng ^= rng << 25;
            rng ^= rng >> 27;
            let word = rng.wrapping_mul(0x2545_f491_4f6c_dd1d).to_le_bytes();
            chunk.copy_from_slice(&word[..chunk.len()]);
        }
        let t = Instant::now();
        file.write_all(&block)?;
        crate::segment::writer::fdatasync(&file)?;
        times.push(t.elapsed());
        if times.len() >= PROBE_MIN_SAMPLES && clock.elapsed() >= budget {
            break;
        }
    }
    Ok(summarize(times, started.elapsed()))
}

fn summarize(mut times: Vec<Duration>, elapsed: Duration) -> DiskProbe {
    times.sort_unstable();
    DiskProbe {
        samples: times.len(),
        p50: percentile(&times, 500),
        p99: percentile(&times, 990),
        max: times.last().copied().unwrap_or_default(),
        elapsed,
    }
}

/// Nearest-rank percentile of a sorted slice; `per_mille` is 500 for the
/// median, 990 for p99. Integer math: a float 0.99 * 100 is not exactly 99.
fn percentile(sorted: &[Duration], per_mille: usize) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let rank = (sorted.len() * per_mille)
        .div_ceil(1000)
        .clamp(1, sorted.len());
    sorted[rank - 1]
}

// ─── Filesystem ──────────────────────────────────────────────────────────

/// How a filesystem relates to durability and fsync cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsKind {
    /// tmpfs, ramfs: contents vanish at restart. Nothing is durable.
    Memory,
    /// NFS, CIFS/SMB, FUSE, virtiofs, 9p, ceph, …: fsync goes to a remote or
    /// userspace implementation with its own semantics and latency.
    Network,
    /// A disk filesystem (xfs, ext4, btrfs, zfs, apfs, overlay over one, …).
    Local,
    Unknown,
}

impl FsKind {
    pub fn as_str(self) -> &'static str {
        match self {
            FsKind::Memory => "memory",
            FsKind::Network => "network",
            FsKind::Local => "local",
            FsKind::Unknown => "unknown",
        }
    }
}

/// The filesystem under the data dir.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filesystem {
    /// Type name as the OS gives it: `xfs`, `tmpfs`, `nfs4`, `fuse.s3fs`, …
    pub name: String,
    pub kind: FsKind,
    /// Mount point (Linux, from mountinfo).
    pub mount_point: Option<String>,
    /// Device or remote the filesystem comes from (Linux, from mountinfo).
    pub source: Option<String>,
}

impl Filesystem {
    fn unknown() -> Self {
        Filesystem {
            name: "unknown".to_string(),
            kind: FsKind::Unknown,
            mount_point: None,
            source: None,
        }
    }
}

/// Classifies a filesystem type name (mountinfo, or macOS `f_fstypename`).
pub fn classify_fs_name(name: &str) -> FsKind {
    let lower = name.to_ascii_lowercase();
    match lower.as_str() {
        "tmpfs" | "ramfs" | "devtmpfs" => return FsKind::Memory,
        "xfs" | "ext2" | "ext3" | "ext4" | "btrfs" | "zfs" | "f2fs" | "jfs" | "reiserfs"
        | "overlay" | "vfat" | "exfat" | "ntfs" | "ntfs3" | "apfs" | "hfs" | "msdos" => {
            return FsKind::Local;
        }
        _ => {}
    }
    const NETWORK_PREFIXES: &[&str] = &[
        "nfs",
        "cifs",
        "smb",
        "fuse",
        "virtiofs",
        "9p",
        "ceph",
        "glusterfs",
        "lustre",
        "gpfs",
        "afs",
        "afpfs",
        "webdav",
        "macfuse",
        "osxfuse",
        "vboxsf",
        "vmhgfs",
        "ncpfs",
    ];
    if NETWORK_PREFIXES.iter().any(|p| lower.starts_with(p)) {
        return FsKind::Network;
    }
    FsKind::Unknown
}

/// Linux `statfs` magic numbers (`linux/magic.h`).
pub fn fs_from_magic(magic: u64) -> Option<(&'static str, FsKind)> {
    Some(match magic {
        0x0102_1994 => ("tmpfs", FsKind::Memory),
        0x8584_58f6 => ("ramfs", FsKind::Memory),
        0x6969 => ("nfs", FsKind::Network),
        0x517b => ("smb", FsKind::Network),
        0xff53_4d42 => ("cifs", FsKind::Network),
        0xfe53_4d42 => ("smb2", FsKind::Network),
        0x6573_5546 => ("fuse", FsKind::Network),
        0x00c3_6400 => ("ceph", FsKind::Network),
        0x0102_1997 => ("9p", FsKind::Network),
        0x0bd0_0bd0 => ("lustre", FsKind::Network),
        0x4750_4653 => ("gpfs", FsKind::Network),
        0x5346_414f => ("afs", FsKind::Network),
        0x786f_4256 => ("vboxsf", FsKind::Network),
        0x564c => ("ncpfs", FsKind::Network),
        0x5846_5342 => ("xfs", FsKind::Local),
        0xef53 => ("ext4", FsKind::Local),
        0x9123_683e => ("btrfs", FsKind::Local),
        0x2fc1_2fc1 => ("zfs", FsKind::Local),
        0xf2f5_2010 => ("f2fs", FsKind::Local),
        0x794c_7630 => ("overlay", FsKind::Local),
        _ => return None,
    })
}

/// One line of `/proc/self/mountinfo`, reduced to what we use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountEntry {
    pub mount_point: String,
    pub fs_type: String,
    pub source: String,
}

/// Parses `/proc/self/mountinfo`. Format (proc(5)):
/// `36 35 98:0 /root /mount/point opts [optional…] - fstype source superopts`.
/// Lines that do not parse are skipped.
pub fn parse_mountinfo(text: &str) -> Vec<MountEntry> {
    text.lines()
        .filter_map(|line| {
            let (head, tail) = line.split_once(" - ")?;
            let mount_point = head.split(' ').nth(4)?;
            let mut tail = tail.split(' ');
            let fs_type = tail.next()?;
            let source = tail.next().unwrap_or("");
            Some(MountEntry {
                mount_point: unescape_mount(mount_point),
                fs_type: fs_type.to_string(),
                source: unescape_mount(source),
            })
        })
        .collect()
}

/// mountinfo escapes space, tab, newline and backslash as `\040` style octal.
fn unescape_mount(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\'
            && i + 3 < bytes.len()
            && bytes[i + 1..i + 4]
                .iter()
                .all(|b| (b'0'..=b'7').contains(b))
        {
            let value = (bytes[i + 1] - b'0') as u32 * 64
                + (bytes[i + 2] - b'0') as u32 * 8
                + (bytes[i + 3] - b'0') as u32;
            out.push(value as u8);
            i += 4;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The mount that holds `path`: the deepest mount point that is a path
/// prefix of it. Later entries win a tie, because a mount stacked on the same
/// point hides the earlier one.
pub fn mount_for<'a>(mounts: &'a [MountEntry], path: &Path) -> Option<&'a MountEntry> {
    let mut best: Option<(&MountEntry, usize)> = None;
    for mount in mounts {
        let point = Path::new(&mount.mount_point);
        if path.starts_with(point) {
            let depth = point.components().count();
            if best.is_none_or(|(_, d)| depth >= d) {
                best = Some((mount, depth));
            }
        }
    }
    best.map(|(m, _)| m)
}

/// `statfs` on the path: filesystem name and kind straight from the kernel.
#[cfg(target_os = "linux")]
fn statfs_fs(path: &Path) -> Option<(String, FsKind)> {
    use std::os::unix::ffi::OsStrExt;
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    // SAFETY: statfs fills the zeroed struct for a valid NUL-terminated path.
    let mut st: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statfs(c_path.as_ptr(), &mut st) } != 0 {
        return None;
    }
    // f_type is a signed word of varying width; every magic fits in 32 bits.
    let magic = (st.f_type as u64) & 0xffff_ffff;
    match fs_from_magic(magic) {
        Some((name, kind)) => Some((name.to_string(), kind)),
        None => Some((format!("magic 0x{magic:x}"), FsKind::Unknown)),
    }
}

#[cfg(target_os = "macos")]
fn statfs_fs(path: &Path) -> Option<(String, FsKind)> {
    use std::os::unix::ffi::OsStrExt;
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    // SAFETY: statfs fills the zeroed struct for a valid NUL-terminated path.
    let mut st: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statfs(c_path.as_ptr(), &mut st) } != 0 {
        return None;
    }
    // SAFETY: f_fstypename is a NUL-terminated array filled by the kernel.
    let name = unsafe { std::ffi::CStr::from_ptr(st.f_fstypename.as_ptr()) }
        .to_string_lossy()
        .into_owned();
    let kind = classify_fs_name(&name);
    Some((name, kind))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn statfs_fs(_path: &Path) -> Option<(String, FsKind)> {
    None
}

/// Identifies the filesystem under `path`. The kernel's answer (`statfs`)
/// decides the kind; mountinfo adds the precise type name (`nfs4`,
/// `fuse.s3fs`) and the mount, but only when its classification agrees, so a
/// bind mount or container path quirk cannot mislabel the filesystem.
pub fn detect_filesystem(path: &Path) -> Filesystem {
    let real = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let mounts = std::fs::read_to_string("/proc/self/mountinfo")
        .map(|text| parse_mountinfo(&text))
        .unwrap_or_default();
    let mount = mount_for(&mounts, &real);
    match statfs_fs(&real) {
        Some((name, kind)) => {
            let agreeing =
                mount.filter(|m| kind != FsKind::Unknown && classify_fs_name(&m.fs_type) == kind);
            Filesystem {
                name: agreeing.map_or(name, |m| m.fs_type.clone()),
                kind,
                mount_point: agreeing.map(|m| m.mount_point.clone()),
                source: agreeing.map(|m| m.source.clone()),
            }
        }
        None => match mount {
            Some(m) => Filesystem {
                name: m.fs_type.clone(),
                kind: classify_fs_name(&m.fs_type),
                mount_point: Some(m.mount_point.clone()),
                source: Some(m.source.clone()),
            },
            None => Filesystem::unknown(),
        },
    }
}

// ─── Memory and CPU ──────────────────────────────────────────────────────

/// Swap in use at startup, in bytes, that is worth a warning. A few pages
/// parked by an idle daemon are normal; tens of megabytes mean the machine
/// is short of memory.
pub const SWAP_IN_USE_BYTES: u64 = 64 << 20;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemoryInfo {
    /// Physical memory of the machine.
    pub host_total_bytes: Option<u64>,
    /// Memory limit of this process's cgroup (container), if one is set.
    pub cgroup_limit_bytes: Option<u64>,
    /// `MemAvailable` at startup (Linux).
    pub available_bytes: Option<u64>,
    pub swap_total_bytes: Option<u64>,
    pub swap_used_bytes: Option<u64>,
}

impl MemoryInfo {
    /// What the process can actually use: the smaller of the machine and the
    /// cgroup limit.
    pub fn effective_total_bytes(&self) -> Option<u64> {
        match (self.host_total_bytes, self.cgroup_limit_bytes) {
            (Some(host), Some(limit)) => Some(host.min(limit)),
            (host, limit) => host.or(limit),
        }
    }
}

/// Parses `/proc/meminfo` (values in kB).
pub fn parse_meminfo(text: &str) -> MemoryInfo {
    let mut info = MemoryInfo::default();
    let (mut swap_total, mut swap_free) = (None, None);
    for line in text.lines() {
        let Some((key, rest)) = line.split_once(':') else {
            continue;
        };
        let bytes = rest
            .split_whitespace()
            .next()
            .and_then(|n| n.parse::<u64>().ok())
            .map(|kb| kb.saturating_mul(1024));
        match key {
            "MemTotal" => info.host_total_bytes = bytes,
            "MemAvailable" => info.available_bytes = bytes,
            "SwapTotal" => swap_total = bytes,
            "SwapFree" => swap_free = bytes,
            _ => {}
        }
    }
    info.swap_total_bytes = swap_total;
    info.swap_used_bytes = match (swap_total, swap_free) {
        (Some(total), Some(free)) => Some(total.saturating_sub(free)),
        _ => None,
    };
    info
}

/// cgroup memory limit file (`memory.max` v2, `memory.limit_in_bytes` v1):
/// `max` or a value near `i64::MAX` means unlimited.
pub fn parse_limit_bytes(text: &str) -> Option<u64> {
    let value = text.trim().parse::<u64>().ok()?;
    (value < 1 << 60).then_some(value)
}

/// cgroup v2 `cpu.max`: `<quota> <period>` in µs, or `max <period>`.
pub fn parse_cpu_max(text: &str) -> Option<f64> {
    let mut parts = text.split_whitespace();
    let quota = parts.next()?.parse::<f64>().ok()?;
    let period = parts.next()?.parse::<f64>().ok()?;
    (quota > 0.0 && period > 0.0).then(|| quota / period)
}

/// cgroup v1 `cpu.cfs_quota_us` / `cpu.cfs_period_us`; a quota of -1 is
/// unlimited.
pub fn parse_cfs(quota: &str, period: &str) -> Option<f64> {
    let quota = quota.trim().parse::<f64>().ok()?;
    let period = period.trim().parse::<f64>().ok()?;
    (quota > 0.0 && period > 0.0).then(|| quota / period)
}

/// The cgroup v2 directory of this process, from `/proc/self/cgroup`'s
/// `0::<path>` line.
pub fn cgroup_v2_dir(self_cgroup: &str) -> Option<PathBuf> {
    let path = self_cgroup
        .lines()
        .find_map(|line| line.strip_prefix("0::"))?;
    Some(Path::new("/sys/fs/cgroup").join(path.trim().trim_start_matches('/')))
}

/// The tightest value of a cgroup v2 limit file over this cgroup and its
/// ancestors (a parent's limit binds its children too).
fn cgroup_v2_min<T: PartialOrd + Copy>(file: &str, parse: impl Fn(&str) -> Option<T>) -> Option<T> {
    let root = Path::new("/sys/fs/cgroup");
    let dir = cgroup_v2_dir(&std::fs::read_to_string("/proc/self/cgroup").ok()?)?;
    let mut best: Option<T> = None;
    for ancestor in dir.ancestors() {
        if !ancestor.starts_with(root) {
            break;
        }
        let Some(value) = std::fs::read_to_string(ancestor.join(file))
            .ok()
            .and_then(|text| parse(&text))
        else {
            continue;
        };
        best = Some(best.map_or(value, |b| if value < b { value } else { b }));
    }
    best
}

fn read_trimmed(path: &str) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

fn cgroup_memory_limit() -> Option<u64> {
    cgroup_v2_min("memory.max", parse_limit_bytes).or_else(|| {
        read_trimmed("/sys/fs/cgroup/memory/memory.limit_in_bytes")
            .and_then(|text| parse_limit_bytes(&text))
    })
}

fn cgroup_cpu_limit() -> Option<f64> {
    cgroup_v2_min("cpu.max", parse_cpu_max).or_else(|| {
        ["/sys/fs/cgroup/cpu", "/sys/fs/cgroup/cpu,cpuacct"]
            .iter()
            .find_map(|dir| {
                let quota = read_trimmed(&format!("{dir}/cpu.cfs_quota_us"))?;
                let period = read_trimmed(&format!("{dir}/cpu.cfs_period_us"))?;
                parse_cfs(&quota, &period)
            })
    })
}

#[cfg(target_os = "macos")]
fn host_memory_bytes_sysctl() -> Option<u64> {
    let mut value: u64 = 0;
    let mut len = std::mem::size_of::<u64>();
    // SAFETY: sysctlbyname writes at most `len` bytes into `value`.
    let rc = unsafe {
        libc::sysctlbyname(
            c"hw.memsize".as_ptr(),
            (&mut value as *mut u64).cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    (rc == 0).then_some(value)
}

#[cfg(not(target_os = "macos"))]
fn host_memory_bytes_sysctl() -> Option<u64> {
    None
}

fn detect_memory() -> MemoryInfo {
    let mut info = std::fs::read_to_string("/proc/meminfo")
        .map(|text| parse_meminfo(&text))
        .unwrap_or_default();
    if info.host_total_bytes.is_none() {
        info.host_total_bytes = host_memory_bytes_sysctl();
    }
    info.cgroup_limit_bytes = cgroup_memory_limit();
    info
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct CpuInfo {
    /// CPUs this process may run on (`available_parallelism`, which already
    /// honors affinity and cgroup quotas on Linux).
    pub available: usize,
    /// CPUs the machine has online.
    pub host: Option<usize>,
    /// cgroup CPU quota, in CPUs (2.0 = two CPUs' worth of time per period).
    pub cgroup_limit: Option<f64>,
}

impl CpuInfo {
    /// The CPU time the process can really get.
    pub fn effective(&self) -> f64 {
        let available = self.available.max(1) as f64;
        self.cgroup_limit.map_or(available, |l| l.min(available))
    }
}

fn detect_cpu() -> CpuInfo {
    let host = {
        // SAFETY: sysconf with a valid name has no preconditions.
        let n = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) };
        (n > 0).then_some(n as usize)
    };
    CpuInfo {
        available: std::thread::available_parallelism().map_or(1, |n| n.get()),
        host,
        cgroup_limit: cgroup_cpu_limit(),
    }
}

// ─── Findings ────────────────────────────────────────────────────────────

/// How loudly a finding is logged outside dev mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Warning,
    /// The server cannot do its job here (nothing is durable): ERROR level.
    Critical,
}

impl Severity {
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Warning => "warning",
            Severity::Critical => "critical",
        }
    }
}

/// Every check there is. The set and each check's severity are fixed, so
/// metrics have stable series.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Check {
    MemoryFilesystem,
    NetworkFilesystem,
    SlowFsync,
    ProbeFailed,
    PlatformMacos,
    LowMemory,
    SwapInUse,
    CpuLimit,
    FewCores,
}

impl Check {
    pub const ALL: [Check; 9] = [
        Check::MemoryFilesystem,
        Check::NetworkFilesystem,
        Check::SlowFsync,
        Check::ProbeFailed,
        Check::PlatformMacos,
        Check::LowMemory,
        Check::SwapInUse,
        Check::CpuLimit,
        Check::FewCores,
    ];

    pub fn id(self) -> &'static str {
        match self {
            Check::MemoryFilesystem => "memory_filesystem",
            Check::NetworkFilesystem => "network_filesystem",
            Check::SlowFsync => "slow_fsync",
            Check::ProbeFailed => "probe_failed",
            Check::PlatformMacos => "platform_macos",
            Check::LowMemory => "low_memory",
            Check::SwapInUse => "swap_in_use",
            Check::CpuLimit => "cpu_limit",
            Check::FewCores => "few_cores",
        }
    }

    pub fn severity(self) -> Severity {
        match self {
            Check::MemoryFilesystem => Severity::Critical,
            _ => Severity::Warning,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    pub check: Check,
    /// What is wrong and what to do about it.
    pub message: String,
}

/// What was observed. [`evaluate`] turns it into findings; tests build it by
/// hand.
#[derive(Debug, Clone)]
pub struct Facts {
    pub platform: &'static str,
    pub data_dir: PathBuf,
    pub probe: Result<DiskProbe, String>,
    pub filesystem: Filesystem,
    pub memory: MemoryInfo,
    pub cpu: CpuInfo,
    /// The block cache budget: the heap the caches may grow to.
    pub cache_budget_bytes: u64,
}

/// Fewer CPUs than this is "few cores": the writer, the fsync thread, the
/// connection runtime and Raft all want one.
pub const MIN_CPUS: f64 = 2.0;

/// Applies the checks to the facts. Unknown facts raise nothing.
pub fn evaluate(facts: &Facts) -> Vec<Finding> {
    let mut findings = Vec::new();
    let mut add = |check: Check, message: String| findings.push(Finding { check, message });
    let fs = &facts.filesystem;

    match fs.kind {
        FsKind::Memory => add(
            Check::MemoryFilesystem,
            format!(
                "the data dir {} is on {}, which lives in memory: nothing written there \
                 survives a restart or power loss, whatever the ack mode says. Point \
                 data-dir at a persistent local disk.",
                facts.data_dir.display(),
                fs.name,
            ),
        ),
        FsKind::Network => add(
            Check::NetworkFilesystem,
            format!(
                "the data dir {} is on {}, a network or userspace filesystem: fsync \
                 latency and durability are whatever the remote end provides, and the \
                 store is built for local-disk semantics. Use a local disk (NVMe) and \
                 let quorum replication and backup carry durability.",
                facts.data_dir.display(),
                fs.name,
            ),
        ),
        FsKind::Local | FsKind::Unknown => {}
    }

    match &facts.probe {
        Ok(probe) if probe.p99 >= SLOW_FSYNC_P99 => add(
            Check::SlowFsync,
            format!(
                "fdatasync is slow on {}: p50 {}, p99 {} (warning from {}). Every durable \
                 append waits for it, so append latency and throughput are bounded by the \
                 disk; local NVMe syncs in about 30 µs.",
                fs.name,
                fmt_duration(probe.p50),
                fmt_duration(probe.p99),
                fmt_duration(SLOW_FSYNC_P99),
            ),
        ),
        Ok(_) => {}
        Err(reason) => add(
            Check::ProbeFailed,
            format!(
                "could not measure the disk ({reason}); ack-mode = auto keeps written \
                 acks in clusters. If this is a hung or failing disk, expect appends to \
                 fail or stall too."
            ),
        ),
    }

    if facts.platform == "macos" {
        add(
            Check::PlatformMacos,
            "running on macOS: every commit is an F_FULLFSYNC, several to tens of \
             milliseconds against about 30 µs on Linux NVMe, so throughput here says \
             nothing about production. Use macOS for development only."
                .to_string(),
        );
    }

    if let Some(total) = facts.memory.effective_total_bytes()
        && facts.cache_budget_bytes.saturating_mul(2) > total
    {
        let limited = facts.memory.cgroup_limit_bytes == Some(total);
        add(
            Check::LowMemory,
            format!(
                "{} of memory{} for a block cache budget of {} (more than half): \
                 the cache can crowd out the page cache that serves reads and holds the \
                 mapped segment indexes. Add memory, or lower block-cache-bytes.",
                fmt_bytes(total),
                if limited { " (cgroup limit)" } else { "" },
                fmt_bytes(facts.cache_budget_bytes),
            ),
        );
    }

    if let Some(used) = facts.memory.swap_used_bytes
        && used >= SWAP_IN_USE_BYTES
    {
        add(
            Check::SwapInUse,
            format!(
                "{} of swap is in use: pages of the caches and segments can be swapped \
                 out, which turns a read into a disk wait. Disable swap on database \
                 nodes or add memory.",
                fmt_bytes(used),
            ),
        );
    }

    if let Some(limit) = facts.cpu.cgroup_limit
        && facts.cpu.host.is_none_or(|host| limit < host as f64)
    {
        add(
            Check::CpuLimit,
            format!(
                "a cgroup CPU limit of {} CPUs applies{}: once the quota is spent, every \
                 thread is paused until the next 100 ms period, which shows up as latency \
                 spikes. Request CPU without a limit.",
                fmt_cpus(limit),
                facts
                    .cpu
                    .host
                    .map_or(String::new(), |h| format!(" on a {h}-CPU machine")),
            ),
        );
    }

    let effective = facts.cpu.effective();
    if effective < MIN_CPUS {
        add(
            Check::FewCores,
            format!(
                "only {} CPU available: the writer, the fsync thread, the network \
                 runtime and Raft compete for it. Run with at least {}.",
                fmt_cpus(effective),
                MIN_CPUS as usize,
            ),
        );
    }

    findings
}

/// Log level for a finding: INFO for everything in dev mode, otherwise WARN,
/// or ERROR for [`Severity::Critical`].
pub fn log_level(severity: Severity, dev: bool) -> tracing::Level {
    match (dev, severity) {
        (true, _) => tracing::Level::INFO,
        (false, Severity::Warning) => tracing::Level::WARN,
        (false, Severity::Critical) => tracing::Level::ERROR,
    }
}

// ─── The report ──────────────────────────────────────────────────────────

pub struct CheckInput<'a> {
    pub data_dir: &'a Path,
    /// Heap the caches may grow to (the block cache).
    pub cache_budget_bytes: usize,
    pub ack_mode: AckMode,
    pub dev: bool,
}

#[derive(Debug, Clone)]
pub struct HardwareReport {
    pub dev: bool,
    pub facts: Facts,
    pub ack: AckDecision,
    pub findings: Vec<Finding>,
}

/// Runs every check against this machine. Call after the data dir exists and
/// is locked, before the first append. Takes at most about [`PROBE_BUDGET`]
/// (or [`PROBE_TIMEOUT`] when the disk hangs). Never fails.
pub fn check(input: &CheckInput) -> HardwareReport {
    let facts = Facts {
        platform: std::env::consts::OS,
        data_dir: input.data_dir.to_path_buf(),
        probe: probe_disk(input.data_dir),
        filesystem: detect_filesystem(input.data_dir),
        memory: detect_memory(),
        cpu: detect_cpu(),
        cache_budget_bytes: input.cache_budget_bytes as u64,
    };
    HardwareReport::new(facts, input.ack_mode, input.dev)
}

impl HardwareReport {
    pub fn new(facts: Facts, ack_mode: AckMode, dev: bool) -> Self {
        let ack = decide_acks(ack_mode, facts.probe.as_ref().ok().map(|p| p.p99));
        let findings = evaluate(&facts);
        HardwareReport {
            dev,
            facts,
            ack,
            findings,
        }
    }

    /// Whether `check` is among the findings.
    pub fn has(&self, check: Check) -> bool {
        self.findings.iter().any(|f| f.check == check)
    }

    /// The startup log: what was measured and decided (INFO), then each
    /// finding at the level [`log_level`] picks.
    pub fn log(&self) {
        let facts = &self.facts;
        match &facts.probe {
            Ok(p) => tracing::info!(
                samples = p.samples,
                p50_us = p.p50.as_micros() as u64,
                p99_us = p.p99.as_micros() as u64,
                max_us = p.max.as_micros() as u64,
                probe_ms = p.elapsed.as_millis() as u64,
                filesystem = %facts.filesystem.name,
                "disk probe: fdatasync p50 {} p99 {} max {} over {} writes in {} on {} ({})",
                fmt_duration(p.p50),
                fmt_duration(p.p99),
                fmt_duration(p.max),
                p.samples,
                fmt_duration(p.elapsed),
                facts.filesystem.name,
                facts.data_dir.display(),
            ),
            Err(reason) => tracing::info!(
                filesystem = %facts.filesystem.name,
                "disk probe: not measured on {} ({reason})",
                facts.data_dir.display(),
            ),
        }
        tracing::info!(
            platform = facts.platform,
            cpus = fmt_cpus(facts.cpu.effective()),
            memory_bytes = facts.memory.effective_total_bytes().unwrap_or(0),
            swap_used_bytes = facts.memory.swap_used_bytes.unwrap_or(0),
            cache_budget_bytes = facts.cache_budget_bytes,
            "hardware: {} {} CPUs, memory {}, swap in use {}, cache budgets {}",
            facts.platform,
            fmt_cpus(facts.cpu.effective()),
            facts
                .memory
                .effective_total_bytes()
                .map_or("unknown".to_string(), fmt_bytes),
            facts
                .memory
                .swap_used_bytes
                .map_or("unknown".to_string(), fmt_bytes),
            fmt_bytes(facts.cache_budget_bytes),
        );
        let mode = match self.ack.configured {
            AckMode::Auto => "auto",
            AckMode::Written => "written",
            AckMode::Durable => "durable",
        };
        tracing::info!(
            ack_mode = mode,
            single_voter = self.ack.single_voter.as_str(),
            multi_voter = self.ack.multi_voter.as_str(),
            "ack mode: {mode}: {} acks on a single voter, {} acks at 2+ voters; {}",
            self.ack.single_voter.as_str(),
            self.ack.multi_voter.as_str(),
            self.ack.reason,
        );

        for finding in &self.findings {
            let (check, message) = (finding.check.id(), finding.message.as_str());
            match log_level(finding.check.severity(), self.dev) {
                tracing::Level::ERROR => {
                    tracing::error!(check, "hardware: {check}: {message}")
                }
                tracing::Level::WARN => tracing::warn!(check, "hardware: {check}: {message}"),
                _ => tracing::info!(check, "hardware: {check}: {message}"),
            }
        }
        if self.findings.is_empty() {
            tracing::info!("hardware: no findings");
        } else if self.dev {
            tracing::info!(
                findings = self.findings.len(),
                "hardware: {} finding(s) logged at info because dev mode is on",
                self.findings.len()
            );
        } else {
            tracing::warn!(
                findings = self.findings.len(),
                "hardware: {} finding(s) above: this machine is below the recommended \
                 production setup. On a development machine, set dev = true (--dev, \
                 KRONOSDB_DEV=1) to log them at info instead.",
                self.findings.len()
            );
        }
    }
}

// ─── Formatting ──────────────────────────────────────────────────────────

/// `26µs`, `1.9ms`, `2.50s`.
pub fn fmt_duration(d: Duration) -> String {
    let us = d.as_micros();
    if us < 1000 {
        format!("{us}µs")
    } else if us < 1_000_000 && us.is_multiple_of(1000) {
        format!("{}ms", us / 1000)
    } else if us < 1_000_000 {
        format!("{:.1}ms", us as f64 / 1000.0)
    } else {
        format!("{:.2}s", d.as_secs_f64())
    }
}

/// `512 MiB`, `1.1 GiB`.
pub fn fmt_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    let mut out = String::new();
    if unit == 0 || value.fract() == 0.0 {
        let _ = write!(out, "{value:.0} {}", UNITS[unit]);
    } else {
        let _ = write!(out, "{value:.1} {}", UNITS[unit]);
    }
    out
}

fn fmt_cpus(cpus: f64) -> String {
    if cpus.fract() == 0.0 {
        format!("{cpus:.0}")
    } else {
        format!("{cpus:.1}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }
    fn us(n: u64) -> Duration {
        Duration::from_micros(n)
    }

    fn probe(p50: Duration, p99: Duration) -> DiskProbe {
        DiskProbe {
            samples: 100,
            p50,
            p99,
            max: p99,
            elapsed: ms(5),
        }
    }

    /// A machine with nothing to complain about.
    fn healthy() -> Facts {
        Facts {
            platform: "linux",
            data_dir: PathBuf::from("/var/lib/kronosdb"),
            probe: Ok(probe(us(26), us(190))),
            filesystem: Filesystem {
                name: "xfs".into(),
                kind: FsKind::Local,
                mount_point: Some("/".into()),
                source: Some("/dev/nvme0n1".into()),
            },
            memory: MemoryInfo {
                host_total_bytes: Some(64 << 30),
                cgroup_limit_bytes: None,
                available_bytes: Some(60 << 30),
                swap_total_bytes: Some(0),
                swap_used_bytes: Some(0),
            },
            cpu: CpuInfo {
                available: 8,
                host: Some(8),
                cgroup_limit: None,
            },
            cache_budget_bytes: (1 << 30) + (128 << 20),
        }
    }

    fn checks(facts: &Facts) -> Vec<Check> {
        evaluate(facts).into_iter().map(|f| f.check).collect()
    }

    // ── probe ──

    #[test]
    fn percentile_is_nearest_rank() {
        let hundred: Vec<_> = (1..=100).map(Duration::from_micros).collect();
        assert_eq!(percentile(&hundred, 500), us(50));
        assert_eq!(percentile(&hundred, 990), us(99));
        // Fewer than 100 samples: p99 is the maximum.
        let eight: Vec<_> = (1..=8).map(Duration::from_micros).collect();
        assert_eq!(percentile(&eight, 990), us(8));
        assert_eq!(percentile(&eight, 500), us(4));
        assert_eq!(percentile(&[us(7)], 990), us(7));
        assert_eq!(percentile(&[], 990), Duration::ZERO);
    }

    #[test]
    fn probe_measures_and_cleans_up() {
        let dir = tempfile::tempdir().unwrap();
        let p = probe_disk(dir.path()).expect("probe");
        assert!(p.samples >= PROBE_MIN_SAMPLES && p.samples <= PROBE_SAMPLES);
        assert!(p.p50 <= p.p99 && p.p99 <= p.max);
        assert!(p.elapsed >= p.p50);
        assert!(
            !dir.path().join(PROBE_FILE).exists(),
            "scratch file left behind"
        );
    }

    #[test]
    fn probe_stops_at_its_time_budget_but_keeps_a_minimum() {
        let dir = tempfile::tempdir().unwrap();
        let p = probe_disk_with(dir.path(), 100, Duration::ZERO, PROBE_TIMEOUT).expect("probe");
        assert_eq!(p.samples, PROBE_MIN_SAMPLES);
    }

    #[test]
    fn probe_reports_an_unusable_dir() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist");
        assert!(probe_disk(&missing).is_err());
    }

    #[test]
    fn probe_overwrites_a_stale_scratch_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(PROBE_FILE), vec![1u8; 10_000_000]).unwrap();
        probe_disk(dir.path()).expect("probe");
        assert!(!dir.path().join(PROBE_FILE).exists());
    }

    // ── filesystem ──

    const MOUNTINFO: &str = "\
22 28 0:21 / /sys rw,nosuid,nodev,noexec,relatime shared:7 - sysfs sysfs rw
28 1 253:0 / / rw,relatime shared:1 - xfs /dev/mapper/fedora-root rw,attr2
40 28 0:35 / /dev/shm rw,nosuid,nodev shared:3 - tmpfs tmpfs rw
81 28 0:60 / /mnt/with\\040space rw,relatime - ext4 /dev/sdb1 rw
90 28 0:61 / /workspaces rw,relatime shared:9 - xfs /dev/mapper/vg-ws rw
91 90 0:62 / /workspaces/nfs rw,relatime shared:10 - nfs4 10.0.0.2:/export rw,vers=4.2
92 90 0:63 / /workspaces/fuse rw,relatime - fuse.s3fs s3fs rw
garbage line
";

    #[test]
    fn mountinfo_parses_fields_and_escapes() {
        let mounts = parse_mountinfo(MOUNTINFO);
        assert_eq!(mounts.len(), 7, "the garbage line is skipped");
        let root = &mounts[1];
        assert_eq!(
            (
                root.mount_point.as_str(),
                root.fs_type.as_str(),
                root.source.as_str()
            ),
            ("/", "xfs", "/dev/mapper/fedora-root")
        );
        assert_eq!(mounts[3].mount_point, "/mnt/with space");
        assert_eq!(mounts[5].fs_type, "nfs4");
        assert_eq!(mounts[5].source, "10.0.0.2:/export");
    }

    #[test]
    fn mount_for_picks_the_deepest_mount() {
        let mounts = parse_mountinfo(MOUNTINFO);
        let fs = |p: &str| mount_for(&mounts, Path::new(p)).map(|m| m.fs_type.as_str());
        assert_eq!(fs("/var/lib/kronosdb"), Some("xfs"));
        assert_eq!(fs("/dev/shm/kronos"), Some("tmpfs"));
        assert_eq!(fs("/mnt/with space/data"), Some("ext4"));
        assert_eq!(fs("/workspaces/repo/data"), Some("xfs"));
        assert_eq!(fs("/workspaces/nfs/data"), Some("nfs4"));
        assert_eq!(fs("/workspaces/fuse"), Some("fuse.s3fs"));
        // /workspaces/nfsX is not under the /workspaces/nfs mount.
        assert_eq!(fs("/workspaces/nfsX/data"), Some("xfs"));
    }

    #[test]
    fn mount_for_prefers_the_later_of_two_stacked_mounts() {
        let text = "\
1 0 0:1 / /data rw - ext4 /dev/a rw
2 1 0:2 / /data rw - tmpfs tmpfs rw
";
        let mounts = parse_mountinfo(text);
        assert_eq!(
            mount_for(&mounts, Path::new("/data/x")).unwrap().fs_type,
            "tmpfs"
        );
    }

    #[test]
    fn fs_names_classify() {
        for (name, kind) in [
            ("tmpfs", FsKind::Memory),
            ("ramfs", FsKind::Memory),
            ("nfs", FsKind::Network),
            ("nfs4", FsKind::Network),
            ("cifs", FsKind::Network),
            ("smb3", FsKind::Network),
            ("smbfs", FsKind::Network),
            ("fuse.s3fs", FsKind::Network),
            ("fuseblk", FsKind::Network),
            ("virtiofs", FsKind::Network),
            ("9p", FsKind::Network),
            ("macfuse", FsKind::Network),
            ("xfs", FsKind::Local),
            ("ext4", FsKind::Local),
            ("btrfs", FsKind::Local),
            ("overlay", FsKind::Local),
            ("apfs", FsKind::Local),
            ("TMPFS", FsKind::Memory),
            ("somethingnew", FsKind::Unknown),
        ] {
            assert_eq!(classify_fs_name(name), kind, "{name}");
        }
    }

    #[test]
    fn statfs_magics_classify() {
        assert_eq!(fs_from_magic(0x0102_1994), Some(("tmpfs", FsKind::Memory)));
        assert_eq!(fs_from_magic(0x8584_58f6), Some(("ramfs", FsKind::Memory)));
        assert_eq!(fs_from_magic(0x6969), Some(("nfs", FsKind::Network)));
        assert_eq!(fs_from_magic(0xff53_4d42), Some(("cifs", FsKind::Network)));
        assert_eq!(fs_from_magic(0xfe53_4d42), Some(("smb2", FsKind::Network)));
        assert_eq!(fs_from_magic(0x6573_5546), Some(("fuse", FsKind::Network)));
        assert_eq!(fs_from_magic(0x5846_5342), Some(("xfs", FsKind::Local)));
        assert_eq!(fs_from_magic(0xef53), Some(("ext4", FsKind::Local)));
        assert_eq!(fs_from_magic(0x794c_7630), Some(("overlay", FsKind::Local)));
        assert_eq!(fs_from_magic(0xdead_beef), None);
    }

    #[test]
    fn detect_filesystem_identifies_the_temp_dir() {
        let dir = tempfile::tempdir().unwrap();
        let fs = detect_filesystem(dir.path());
        // Whatever the host is, it is named and is not memory-backed unless
        // the test runner's temp dir really is a tmpfs.
        assert!(!fs.name.is_empty());
        if cfg!(target_os = "macos") {
            assert_eq!(fs.kind, FsKind::Local, "{fs:?}");
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn detect_filesystem_sees_tmpfs_on_dev_shm() {
        let shm = Path::new("/dev/shm");
        if !shm.is_dir() {
            return;
        }
        let fs = detect_filesystem(shm);
        assert_eq!(fs.kind, FsKind::Memory, "{fs:?}");
        assert_eq!(fs.name, "tmpfs");
    }

    // ── memory and cpu ──

    #[test]
    fn meminfo_parses() {
        let text = "MemTotal:       16384000 kB\nMemFree:  100 kB\nMemAvailable:   8192000 kB\n\
                    SwapTotal:       2097152 kB\nSwapFree:        2000000 kB\n";
        let m = parse_meminfo(text);
        assert_eq!(m.host_total_bytes, Some(16_384_000 * 1024));
        assert_eq!(m.available_bytes, Some(8_192_000 * 1024));
        assert_eq!(m.swap_total_bytes, Some(2_097_152 * 1024));
        assert_eq!(m.swap_used_bytes, Some(97_152 * 1024));
        assert_eq!(parse_meminfo("nonsense"), MemoryInfo::default());
    }

    #[test]
    fn cgroup_files_parse() {
        assert_eq!(parse_limit_bytes("max\n"), None);
        assert_eq!(parse_limit_bytes("9223372036854771712\n"), None);
        assert_eq!(parse_limit_bytes("4294967296\n"), Some(4 << 30));
        assert_eq!(parse_limit_bytes(""), None);
        assert_eq!(parse_cpu_max("max 100000\n"), None);
        assert_eq!(parse_cpu_max("200000 100000\n"), Some(2.0));
        assert_eq!(parse_cpu_max("150000 100000"), Some(1.5));
        assert_eq!(parse_cpu_max("garbage"), None);
        assert_eq!(parse_cfs("-1\n", "100000\n"), None);
        assert_eq!(parse_cfs("50000\n", "100000\n"), Some(0.5));
        assert_eq!(
            cgroup_v2_dir("0::/kubepods/pod1/ctr\n"),
            Some(PathBuf::from("/sys/fs/cgroup/kubepods/pod1/ctr"))
        );
        assert_eq!(
            cgroup_v2_dir("0::/\n"),
            Some(PathBuf::from("/sys/fs/cgroup"))
        );
        assert_eq!(cgroup_v2_dir("12:memory:/x\n"), None);
    }

    #[test]
    fn effective_memory_and_cpu_take_the_tighter_limit() {
        let m = MemoryInfo {
            host_total_bytes: Some(64 << 30),
            cgroup_limit_bytes: Some(2 << 30),
            ..Default::default()
        };
        assert_eq!(m.effective_total_bytes(), Some(2 << 30));
        assert_eq!(MemoryInfo::default().effective_total_bytes(), None);
        let cpu = CpuInfo {
            available: 4,
            host: Some(16),
            cgroup_limit: Some(2.5),
        };
        assert_eq!(cpu.effective(), 2.5);
    }

    #[test]
    fn detection_runs_here() {
        // Best-effort on every platform: no panic, and a CPU count.
        let cpu = detect_cpu();
        assert!(cpu.available >= 1);
        let memory = detect_memory();
        if cfg!(any(target_os = "linux", target_os = "macos")) {
            assert!(memory.host_total_bytes.is_some());
        }
    }

    // ── findings ──

    #[test]
    fn healthy_machine_has_no_findings() {
        assert!(checks(&healthy()).is_empty());
    }

    #[test]
    fn memory_filesystem_is_critical() {
        let mut facts = healthy();
        facts.filesystem = Filesystem {
            name: "tmpfs".into(),
            kind: FsKind::Memory,
            mount_point: None,
            source: None,
        };
        let findings = evaluate(&facts);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].check, Check::MemoryFilesystem);
        assert_eq!(findings[0].check.severity(), Severity::Critical);
        assert!(findings[0].message.contains("tmpfs"));
        assert!(findings[0].message.contains("/var/lib/kronosdb"));
    }

    #[test]
    fn network_filesystem_warns() {
        let mut facts = healthy();
        facts.filesystem.name = "nfs4".into();
        facts.filesystem.kind = FsKind::Network;
        assert_eq!(checks(&facts), vec![Check::NetworkFilesystem]);
    }

    #[test]
    fn slow_fsync_starts_at_five_ms_p99() {
        let mut facts = healthy();
        facts.probe = Ok(probe(ms(3), us(4_999)));
        assert!(
            checks(&facts).is_empty(),
            "a middling disk is not a warning"
        );
        facts.probe = Ok(probe(ms(3), ms(5)));
        assert_eq!(checks(&facts), vec![Check::SlowFsync]);
        // The median alone does not trigger it; the tail does.
        facts.probe = Ok(probe(ms(8), ms(4)));
        assert!(checks(&facts).is_empty());
    }

    #[test]
    fn failed_probe_is_a_finding() {
        let mut facts = healthy();
        facts.probe = Err("timed out".into());
        let findings = evaluate(&facts);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].check, Check::ProbeFailed);
        assert!(findings[0].message.contains("timed out"));
    }

    #[test]
    fn macos_is_flagged() {
        let mut facts = healthy();
        facts.platform = "macos";
        assert_eq!(checks(&facts), vec![Check::PlatformMacos]);
    }

    #[test]
    fn low_memory_compares_budgets_with_the_tighter_of_host_and_cgroup() {
        let mut facts = healthy();
        // Defaults budget 1.125 GiB: fine at 4 GiB (28%), flagged at 2 GiB.
        facts.memory.cgroup_limit_bytes = Some(4 << 30);
        assert!(checks(&facts).is_empty());
        facts.memory.cgroup_limit_bytes = Some(2 << 30);
        let findings = evaluate(&facts);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].check, Check::LowMemory);
        assert!(findings[0].message.contains("cgroup limit"));
        // A smaller budget makes the same memory enough.
        facts.cache_budget_bytes = 512 << 20;
        assert!(checks(&facts).is_empty());
        // Unknown memory raises nothing.
        facts.memory = MemoryInfo::default();
        facts.cache_budget_bytes = u64::MAX;
        assert!(checks(&facts).is_empty());
    }

    #[test]
    fn swap_in_use_needs_real_usage() {
        let mut facts = healthy();
        facts.memory.swap_used_bytes = Some(4 << 20);
        assert!(checks(&facts).is_empty());
        facts.memory.swap_used_bytes = Some(SWAP_IN_USE_BYTES);
        assert_eq!(checks(&facts), vec![Check::SwapInUse]);
        facts.memory.swap_used_bytes = None;
        assert!(checks(&facts).is_empty());
    }

    #[test]
    fn cpu_limit_below_the_machine_is_flagged() {
        let mut facts = healthy();
        facts.cpu.cgroup_limit = Some(4.0);
        facts.cpu.available = 4;
        assert_eq!(checks(&facts), vec![Check::CpuLimit]);
        // A quota at or above the machine's CPUs never throttles.
        facts.cpu.cgroup_limit = Some(8.0);
        facts.cpu.available = 8;
        assert!(checks(&facts).is_empty());
    }

    #[test]
    fn few_cores_is_flagged_below_two() {
        let mut facts = healthy();
        facts.cpu = CpuInfo {
            available: 1,
            host: Some(1),
            cgroup_limit: None,
        };
        assert_eq!(checks(&facts), vec![Check::FewCores]);
        // A 1.5 CPU quota on a big machine is both a limit and few cores.
        facts.cpu = CpuInfo {
            available: 2,
            host: Some(32),
            cgroup_limit: Some(1.5),
        };
        assert_eq!(checks(&facts), vec![Check::CpuLimit, Check::FewCores]);
        facts.cpu = CpuInfo {
            available: 2,
            host: Some(2),
            cgroup_limit: None,
        };
        assert!(checks(&facts).is_empty());
    }

    #[test]
    fn check_ids_are_unique_and_complete() {
        let ids: std::collections::HashSet<_> = Check::ALL.iter().map(|c| c.id()).collect();
        assert_eq!(ids.len(), Check::ALL.len());
    }

    // ── dev-mode routing ──

    #[test]
    fn dev_mode_logs_everything_at_info() {
        for check in Check::ALL {
            assert_eq!(log_level(check.severity(), true), tracing::Level::INFO);
        }
    }

    #[test]
    fn outside_dev_mode_findings_are_loud() {
        assert_eq!(
            log_level(Check::SlowFsync.severity(), false),
            tracing::Level::WARN
        );
        assert_eq!(
            log_level(Check::PlatformMacos.severity(), false),
            tracing::Level::WARN
        );
        assert_eq!(
            log_level(Check::MemoryFilesystem.severity(), false),
            tracing::Level::ERROR
        );
    }

    // ── report ──

    #[test]
    fn report_decides_acks_from_the_probe() {
        let report = HardwareReport::new(healthy(), AckMode::Auto, false);
        assert_eq!(report.ack.multi_voter, crate::AckKind::Durable);
        let mut slow = healthy();
        slow.probe = Ok(probe(ms(2), ms(4)));
        let report = HardwareReport::new(slow, AckMode::Auto, false);
        assert_eq!(report.ack.multi_voter, crate::AckKind::Written);
        assert!(report.findings.is_empty());
        let mut failed = healthy();
        failed.probe = Err("nope".into());
        let report = HardwareReport::new(failed, AckMode::Auto, false);
        assert_eq!(report.ack.multi_voter, crate::AckKind::Written);
        assert!(report.has(Check::ProbeFailed));
    }

    #[test]
    fn check_runs_the_whole_thing_and_logs_without_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let report = check(&CheckInput {
            data_dir: dir.path(),
            cache_budget_bytes: (1 << 30) + (128 << 20),
            ack_mode: AckMode::Auto,
            dev: true,
        });
        assert!(report.facts.probe.is_ok(), "{:?}", report.facts.probe);
        assert_eq!(report.facts.cache_budget_bytes, (1 << 30) + (128 << 20));
        report.log();
    }

    #[test]
    fn formatting() {
        assert_eq!(fmt_duration(us(26)), "26µs");
        assert_eq!(fmt_duration(us(1900)), "1.9ms");
        assert_eq!(fmt_duration(ms(5)), "5ms");
        assert_eq!(fmt_duration(Duration::from_millis(2500)), "2.50s");
        assert_eq!(fmt_bytes(512 << 20), "512 MiB");
        assert_eq!(fmt_bytes((1 << 30) + (128 << 20)), "1.1 GiB");
        assert_eq!(fmt_bytes(0), "0 B");
        assert_eq!(fmt_cpus(8.0), "8");
        assert_eq!(fmt_cpus(1.5), "1.5");
    }
}

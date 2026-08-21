use axum::extract::{Query, State};
use axum::response::Html;

use crate::admin::AdminState;
use crate::admin::layout;

use super::{DetailParams, MessagePage};

const CFG: MessagePage = MessagePage {
    topic: "commands",
    id_prefix: "cmd",
    detail_target: "command-detail",
    title: "Registered Command Handlers",
    empty_copy: "No command handlers registered",
    badge_accent: "bg-k-gold-d text-k-gold",
    show_load_factor: true,
    show_mode_badge: false,
};

// ── Page handler ───────────────────────────────────────────────────

pub async fn page(State(state): State<AdminState>) -> Html<String> {
    let mut details = state.messaging.all_command_details();
    details.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.bus.cmp(&b.bus)));

    let content = super::message_page_html(&CFG, &details);

    Html(layout::layout(
        "commands",
        "Commands",
        &state.config.node_name,
        &state.contexts.list_contexts(),
        &content,
    ))
}

// ── Fragments ──────────────────────────────────────────────────────

pub async fn commands_fragment(State(state): State<AdminState>) -> Html<String> {
    let mut details = state.messaging.all_command_details();
    details.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.bus.cmp(&b.bus)));
    Html(super::master_list_fragment_html(&CFG, &details))
}

pub async fn command_detail_fragment(
    State(state): State<AdminState>,
    Query(params): Query<DetailParams>,
) -> Html<String> {
    let details = state.messaging.all_command_details();
    let detail = details
        .iter()
        .find(|d| d.name == params.name && d.bus == params.bus);
    Html(super::detail_fragment_html(
        &CFG,
        detail,
        &params.name,
        &params.bus,
    ))
}

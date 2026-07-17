package io.kronosdb.connector.grpc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

/**
 * gRPC interceptor that attaches the KronosDB context and optional auth token
 * as metadata headers on every outgoing call.
 */
public class ContextMetadata implements ClientInterceptor {

    static final Metadata.Key<String> CONTEXT_KEY =
            Metadata.Key.of("kronosdb-context", Metadata.ASCII_STRING_MARSHALLER);
    static final Metadata.Key<String> TOKEN_KEY =
            Metadata.Key.of("kronosdb-token", Metadata.ASCII_STRING_MARSHALLER);

    private final String context;
    private final String token;

    public ContextMetadata(String context, String token) {
        this.context = context;
        this.token = token;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                headers.put(CONTEXT_KEY, context);
                if (token != null && !token.isEmpty()) {
                    headers.put(TOKEN_KEY, token);
                }
                super.start(responseListener, headers);
            }
        };
    }
}

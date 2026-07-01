import 'dart:async';

import 'package:opentelemetry/api.dart';
import 'package:rpc_dart/rpc_dart.dart';
import 'package:rpc_dart_opentelemetry/rpc_dart_opentelemetry.dart';

/// RPC interceptor that stamps the active OTel span with the authenticated
/// user id.
///
/// Must be placed **after** the token-verify interceptor (so the user id is
/// already present in [RpcContext]) and **after** [OtelRpcInterceptor] (so the
/// span is already created and stored under [OtelRpcKeys.span]).
///
/// ```dart
/// endpoint
///   ..addInterceptor(obs.rpcInterceptor)         // creates span
///   ..addInterceptor(tokenVerifyInterceptor)      // sets userId in context
///   ..addInterceptor(UserIdSpanInterceptor(       // stamps span
///       userIdKey: RhyoliteAuthKeys.userId,
///     ));
/// ```
class UserIdSpanInterceptor implements IRpcInterceptor {
  /// The [RpcContext] key under which the user id string is stored.
  final Object userIdKey;

  /// OTel span attribute name. Defaults to `app.user.id`.
  final String spanAttribute;

  const UserIdSpanInterceptor({
    required this.userIdKey,
    this.spanAttribute = 'app.user.id',
  });

  @override
  Future<TResponse> interceptUnary<TRequest, TResponse>(
    RpcMiddlewareContext call,
    TRequest request,
    RpcUnaryNext<TRequest, TResponse> next,
  ) async {
    _stamp(call.context);
    return next(call.context, request);
  }

  @override
  FutureOr<Stream<TResponse>> interceptServerStream<TRequest, TResponse>(
    RpcMiddlewareContext call,
    TRequest request,
    RpcServerStreamNext<TRequest, TResponse> next,
  ) {
    _stamp(call.context);
    return next(call.context, request);
  }

  @override
  Future<TResponse> interceptClientStream<TRequest, TResponse>(
    RpcMiddlewareContext call,
    Stream<TRequest> requests,
    RpcClientStreamNext<TRequest, TResponse> next,
  ) async {
    _stamp(call.context);
    return next(call.context, requests);
  }

  @override
  FutureOr<Stream<TResponse>> interceptBidirectionalStream<TRequest, TResponse>(
    RpcMiddlewareContext call,
    Stream<TRequest> requests,
    RpcBidirectionalStreamNext<TRequest, TResponse> next,
  ) {
    _stamp(call.context);
    return next(call.context, requests);
  }

  void _stamp(RpcContext context) {
    final userId = context.getValue(userIdKey);
    if (userId is! String) return;
    final span = context.getValue(OtelRpcKeys.span) as Span?;
    span?.setAttribute(Attribute.fromString(spanAttribute, userId));
  }
}

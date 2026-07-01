import 'dart:io';

import 'package:opentelemetry/api.dart';
import 'package:opentelemetry/sdk.dart';
import 'package:rpc_dart/rpc_dart.dart';
import 'package:rpc_dart_opentelemetry/rpc_dart_opentelemetry.dart';

import 'business_metrics.dart';

/// Central observability setup for a Rhyolite service.
///
/// Reads [OTEL_ENDPOINT] env var to locate the OTel Collector.
/// If not set — runs in no-op mode (no export, zero overhead).
///
/// Usage:
/// ```dart
/// final obs = await RhyoliteObservability.init(serviceName: 'rhyolite-account');
/// endpoint.addInterceptor(obs.rpcInterceptor);
/// obs.business.recordRegistration(plan: 'pro');
/// // on shutdown:
/// await obs.shutdown();
/// ```
class RhyoliteObservability {
  /// env var: OTLP/HTTP collector endpoint, e.g. http://otel-collector:4318
  static const _envKey = 'OTEL_ENDPOINT';

  final IRpcInterceptor rpcInterceptor;
  final BusinessMetrics business;
  final TracerProviderBase? _tracerProvider;

  RhyoliteObservability._({
    required this.rpcInterceptor,
    required this.business,
    required TracerProviderBase? tracerProvider,
  }) : _tracerProvider = tracerProvider;

  static Future<RhyoliteObservability> init({
    required String serviceName,
    String? version,
  }) async {
    final endpoint = Platform.environment[_envKey];

    if (endpoint == null || endpoint.isEmpty) {
      stderr.writeln(
        '[observability] $serviceName: $_envKey is not set — running without OTel export.',
      );
      return _noop(serviceName);
    }

    final resource = Resource([
      Attribute.fromString('service.name', serviceName),
      if (version != null) Attribute.fromString('service.version', version),
    ]);

    final tracerProvider = TracerProviderBase(
      resource: resource,
      processors: [
        BatchSpanProcessor(CollectorExporter(Uri.parse('$endpoint/v1/traces'))),
      ],
    );

    registerGlobalTracerProvider(tracerProvider);

    final tracer = tracerProvider.getTracer(
      serviceName,
      version: version ?? '0.0.0',
    );
    print('[observability] $serviceName: exporting traces to $endpoint');

    return RhyoliteObservability._(
      rpcInterceptor: OtelRpcInterceptor(tracer: tracer),
      business: BusinessMetrics(tracer),
      tracerProvider: tracerProvider,
    );
  }

  Future<void> shutdown() async {
    _tracerProvider?.shutdown();
  }

  static RhyoliteObservability _noop(String serviceName) {
    final tracerProvider = TracerProviderBase(processors: []);
    final tracer = tracerProvider.getTracer(serviceName);
    return RhyoliteObservability._(
      rpcInterceptor: OtelRpcInterceptor(tracer: tracer),
      business: BusinessMetrics(tracer),
      tracerProvider: null,
    );
  }
}

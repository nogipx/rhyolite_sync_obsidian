import 'package:opentelemetry/api.dart';

/// Business-level event recording via OTel spans.
///
/// Each method creates a short-lived span that flows through the OTel pipeline.
/// The collector's spanmetrics processor turns these into Prometheus counters
/// automatically — no separate metrics exporter needed on the Dart side.
class BusinessMetrics {
  final Tracer _tracer;

  const BusinessMetrics(this._tracer);

  /// Record a new user registration.
  void recordRegistration({String? plan}) {
    _record('user.registered', [
      if (plan != null) Attribute.fromString('app.user.plan', plan),
    ]);
  }

  /// Record a successful email verification (and trial activation).
  void recordEmailVerified() {
    _record('user.email_verified', []);
  }

  /// Record a successful sign-in.
  void recordSignIn() {
    _record('user.signed_in', []);
  }

  /// Record a new vault created.
  void recordVaultCreated() {
    _record('vault.created', []);
  }

  /// Record a new sync WebSocket connection.
  void recordSyncSession() {
    _record('sync.session_started', []);
  }

  /// Record a completed payment.
  void recordPayment({required String plan, required int amountRub}) {
    _record('payment.completed', [
      Attribute.fromString('app.payment.plan', plan),
      Attribute.fromInt('app.payment.amount_rub', amountRub),
    ]);
  }

  void _record(String name, List<Attribute> attributes) {
    final span = _tracer.startSpan(
      name,
      kind: SpanKind.internal,
      attributes: attributes,
    );
    span
      ..setStatus(StatusCode.ok)
      ..end();
  }
}

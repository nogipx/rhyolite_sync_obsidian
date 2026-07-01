/// Server-side responder library for Δ-state CRDT file sync.
///
/// Pure library: contains the gRPC-over-WebSocket responder classes
/// that implement the wire contract defined in `rhyolite_sync`. The
/// runnable server binary and infrastructure wiring (Postgres/MinIO/auth)
/// live in `rhyolite_sync_server_runtime` + the edition packages
/// (`rhyolite_sync_server_managed`, `rhyolite_sync_server_selfhost`).
library;

export 'src/state_sync_responder.dart';
export 'src/history_responder.dart';
export 'src/rhyolite_blob_responder.dart';

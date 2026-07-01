import 'package:rhyolite_sync/rhyolite_sync.dart';
import 'package:rpc_dart/rpc_dart.dart';
import 'package:rpc_data/rpc_data.dart';

/// Append-only history log of file changes per vault.
///
/// Events are written server-side as a side-effect of successful
/// StateSync.putStates — see StateSyncResponder._writeHistoryEvent. This
/// responder only exposes READ + USER-TRIGGERED DELETE; there is no
/// automatic retention. Users explicitly choose what to keep.
class HistoryResponder extends HistoryContractResponder {
  static String _historyCollection(String vaultId) => '${vaultId}_history';
  static String _headsCollection(String vaultId) => '${vaultId}_history_heads';
  static String _stateMetaCollection() => 'state_meta';
  static String _epochKey(String vaultId) => 'state_epoch_$vaultId';

  HistoryResponder({required IDataClient client}) : _client = client;

  final IDataClient _client;

  /// Heads older than this are considered abandoned: they're dropped
  /// from the response AND deleted from storage on the next
  /// `getHistoryHeads` sweep. Must stay in lockstep with the client's
  /// stale-head threshold (see `StateSyncEngine._staleHeadAge`) so
  /// both sides agree on which devices are in the causal-stability
  /// quorum.
  static const Duration _staleHeadAge = Duration(days: 90);

  // ---------------------------------------------------------------------------
  // GET
  // ---------------------------------------------------------------------------

  @override
  Future<HistoryGetResponse> getHistory(
    HistoryGetRequest request, {
    RpcContext? context,
  }) async {
    final currentEpoch = await _loadEpoch(request.vaultId);

    final col = _historyCollection(request.vaultId);
    final all = await _client.listAllRecords(collection: col);

    final filtered = all.where((r) {
      if (request.fileId != null && r.payload['fileId'] != request.fileId) {
        return false;
      }
      final hlc = r.payload['hlcPacked'] as String? ?? '';
      if (request.fromHlcPacked != null) {
        if (hlc.compareTo(request.fromHlcPacked!) <= 0) return false;
      }
      if (request.beforeHlcPacked != null) {
        if (hlc.compareTo(request.beforeHlcPacked!) >= 0) return false;
      }
      return true;
    }).toList();

    filtered.sort((a, b) {
      final ha = a.payload['hlcPacked'] as String? ?? '';
      final hb = b.payload['hlcPacked'] as String? ?? '';
      return request.ascending ? ha.compareTo(hb) : hb.compareTo(ha);
    });

    final limited = filtered.take(request.limit).toList();

    return HistoryGetResponse(
      events: limited.map(_recordToEvent).toList(),
      epoch: currentEpoch,
    );
  }

  @override
  Future<HistoryDeleteEventsResponse> deleteEvents(
    HistoryDeleteEventsRequest request, {
    RpcContext? context,
  }) async {

    if (request.eventIds.isEmpty) {
      return const HistoryDeleteEventsResponse(deleted: 0);
    }

    final col = _historyCollection(request.vaultId);
    var deleted = 0;
    try {
      deleted = await _client.bulkDelete(
        collection: col,
        ids: request.eventIds,
      );
    } catch (e) {
      context?.log.warning('history.deleteEvents: bulkDelete failed: $e');
    }

    context?.log.info(
      'history.deleteEvents vault=${request.vaultId} '
      'requested=${request.eventIds.length} deleted=$deleted',
    );
    return HistoryDeleteEventsResponse(deleted: deleted);
  }

  // ---------------------------------------------------------------------------
  // HEADS (per-device watermarks for safe cleanup)
  // ---------------------------------------------------------------------------

  @override
  Future<ReportHistoryHeadResponse> reportHistoryHead(
    ReportHistoryHeadRequest request, {
    RpcContext? context,
  }) async {

    final col = _headsCollection(request.vaultId);
    final now = DateTime.now().toUtc().millisecondsSinceEpoch;
    final payload = <String, dynamic>{
      'deviceId': request.deviceId,
      'headSeq': request.headSeq,
      'updatedAtMs': now,
      if (request.frontierPacked.isNotEmpty)
        'frontierPacked': request.frontierPacked,
    };

    // Upsert by deviceId. Tolerate a CAS race by reading then deciding,
    // and never let a stale head clobber a more recent one.
    for (var attempt = 0; attempt < 5; attempt++) {
      try {
        final existing = await _client.get(
          collection: col,
          id: request.deviceId,
        );
        if (existing == null) {
          await _client.create(
            collection: col,
            id: request.deviceId,
            payload: payload,
          );
        } else {
          final prevSeq = (existing.payload['headSeq'] as int?) ?? 0;
          if (request.headSeq < prevSeq) {
            // The reporter actually regressed (e.g. fresh install after
            // a wipe) — refuse to write a lower value.
            return const ReportHistoryHeadResponse();
          }
          await _client.update(
            collection: col,
            id: request.deviceId,
            expectedVersion: existing.version,
            payload: payload,
          );
        }
        return const ReportHistoryHeadResponse();
      } catch (e) {
        final msg = e.toString().toLowerCase();
        final retriable =
            msg.contains('conflict') ||
            msg.contains('not newer') ||
            msg.contains('already exists');
        if (!retriable || attempt == 4) rethrow;
        await Future<void>.delayed(Duration(milliseconds: 5 * (1 << attempt)));
      }
    }
    return const ReportHistoryHeadResponse();
  }

  @override
  Future<GetHistoryHeadsResponse> getHistoryHeads(
    GetHistoryHeadsRequest request, {
    RpcContext? context,
  }) async {

    final col = _headsCollection(request.vaultId);
    final records = await _client.listAllRecords(collection: col);

    // Sweep stale rows: anything not updated within [_staleHeadAge] is
    // considered abandoned. Deletes are fire-and-forget — if one fails
    // (race with a concurrent reportHistoryHead, transient DB error)
    // it gets retried on the next call. Surfacing the failure to the
    // caller would punish a read for a janitorial side effect.
    final cutoff = DateTime.now().toUtc().millisecondsSinceEpoch -
        _staleHeadAge.inMilliseconds;
    final fresh = <DataRecord>[];
    final stale = <DataRecord>[];
    for (final r in records) {
      final updated = (r.payload['updatedAtMs'] as int?) ?? 0;
      (updated >= cutoff ? fresh : stale).add(r);
    }
    if (stale.isNotEmpty) {
      // Don't await: cleanup is best-effort, response shouldn't wait.
      // Errors per row are swallowed to let the others through.
      // ignore: unawaited_futures
      Future<void>(() async {
        for (final r in stale) {
          try {
            await _client.delete(collection: col, id: r.id);
          } catch (e) {
            context?.log.warning(
              'heads sweep: failed to delete ${r.id}: $e',
            );
          }
        }
      });
    }

    return GetHistoryHeadsResponse(
      heads: fresh
          .map(
            (r) => DeviceHead(
              deviceId: r.id,
              headSeq: (r.payload['headSeq'] as int?) ?? 0,
              updatedAtMs: (r.payload['updatedAtMs'] as int?) ?? 0,
              frontierPacked:
                  (r.payload['frontierPacked'] as String?) ?? '',
            ),
          )
          .toList(),
    );
  }

  HistoryEvent _recordToEvent(DataRecord r) => HistoryEvent(
    eventId: r.id,
    fileId: r.payload['fileId'] as String,
    blobRef: r.payload['blobRef'] as String,
    hlcPacked: r.payload['hlcPacked'] as String,
    operation: HistoryOperationCodec.parse(r.payload['operation'] as String),
    encryptedMeta: r.payload['encryptedMeta'] as String,
    createdAtMs: r.payload['createdAtMs'] as int,
    contextPacked: (r.payload['contextPacked'] as String?) ?? '',
    serverSeq: (r.payload['serverSeq'] as int?) ?? 0,
    chunks: (r.payload['chunks'] as List?)?.cast<String>() ?? const [],
  );

  // ---------------------------------------------------------------------------
  // Meta / auth (shared shape with StateSyncResponder)
  // ---------------------------------------------------------------------------

  Future<int> _loadEpoch(String vaultId) async {
    final record = await _client.get(
      collection: _stateMetaCollection(),
      id: _epochKey(vaultId),
    );
    if (record == null) return 0;
    return (record.payload['epoch'] as int?) ?? 0;
  }

}

import 'package:convergent/convergent.dart';
import 'package:rhyolite_sync/rhyolite_sync.dart';
import 'package:rpc_blob/rpc_blob.dart';
import 'package:rpc_dart/rpc_dart.dart';
import 'package:rpc_data/rpc_data.dart';
import 'package:rpc_notify/rpc_notify.dart';
import 'dart:math' as math;

import 'package:uuid/uuid.dart';

/// Δ-state CRDT per-file sync responder (doc §3-§5).
///
/// Each file is an `MvRegister<FileState>` materialised over many records
/// in `<vaultId>_file_state`. Each record is exactly one TaggedValue
/// keyed by `${fileId}__${hlcPacked}`. Server stays opaque to file
/// contents — only plain HLC, CausalContext, blobRef, chunks and
/// tombstone are used for the merge algorithm.
///
/// Concurrency control is coordination-free: putStates implements
/// `MvRegister.join` (doc §5.1) — there is no OCC. The vault-wide
/// monotonic [serverSeq] survives as the pull cursor.
class StateSyncResponder extends StateSyncContractResponder {
  /// Optional keyspace qualifier. Empty = the default (notes) keyspace,
  /// preserving the historical collection names. A non-empty value (e.g.
  /// 'config') isolates a second keyspace under the SAME vaultId: state
  /// collection, seq/epoch meta keys and the notify topic are all suffixed,
  /// so it never collides with the notes keyspace and reuses the same vault
  /// ownership.
  String _qualifiedVault(String vaultId) =>
      _namespace.isEmpty ? vaultId : '${vaultId}_$_namespace';

  String _stateCollection(String vaultId) =>
      '${_qualifiedVault(vaultId)}_file_state';
  static String _metaCollection() => 'state_meta';
  String _seqKey(String vaultId) => 'state_seq_${_qualifiedVault(vaultId)}';
  String _epochKey(String vaultId) => 'state_epoch_${_qualifiedVault(vaultId)}';
  String _blobCollection(String vaultId) =>
      '${_qualifiedVault(vaultId)}_blobs';
  String _historyCollection(String vaultId) =>
      '${_qualifiedVault(vaultId)}_history';
  String _headsCollection(String vaultId) =>
      '${_qualifiedVault(vaultId)}_history_heads';

  static String _recordId(String fileId, String hlcPacked) =>
      '${fileId}__$hlcPacked';

  StateSyncResponder({
    required IDataClient client,
    IBlobClient? blobClient,
    INotifyRepository? notifyRepository,
    String namespace = '',
    bool historyEnabled = true,
    super.serviceNameOverride,
  })  : _client = client,
        _blobClient = blobClient,
        _notify = notifyRepository,
        _namespace = namespace,
        _historyEnabled = historyEnabled;

  final IDataClient _client;
  final IBlobClient? _blobClient;
  final INotifyRepository? _notify;

  /// Keyspace qualifier; see [_qualifiedVault]. Defaults to '' (notes).
  final String _namespace;

  /// When false, no history events are written and the `_history` /
  /// `_history_heads` collections are never touched. Notes keep history
  /// (true); the settings keyspace runs without it to stay within a single
  /// collection.
  final bool _historyEnabled;

  // ---------------------------------------------------------------------------
  // PUT (push) — MvRegister.join per item
  // ---------------------------------------------------------------------------

  @override
  Future<StatePutResponse> putStates(
    StatePutRequest request, {
    RpcContext? context,
  }) async {
    final currentEpoch = await _loadEpoch(request.vaultId);

    // Epoch gate: stale clients pushing into a wiped vault must be rejected
    // wholesale before any writes happen.
    if (request.expectedEpoch != null &&
        request.expectedEpoch != currentEpoch) {
      context?.log.info(
        'state.put epoch_mismatch vault=${request.vaultId} '
        'client=${request.expectedEpoch} server=$currentEpoch — rejected',
      );
      return StatePutResponse(
        results: const [],
        cursor: await _loadSeq(request.vaultId),
        epoch: currentEpoch,
        epochMismatch: true,
      );
    }

    if (request.items.isEmpty) {
      return StatePutResponse(
        results: const [],
        cursor: await _loadSeq(request.vaultId),
        epoch: currentEpoch,
      );
    }

    final col = _stateCollection(request.vaultId);
    final results = <StatePutResult>[];
    var anyWrite = false;

    // Reserve ONE seq range for the whole batch instead of one seq per
    // item. Per-item join may insert 0 or 1 records (insert is skipped
    // when an existing TaggedValue with the same hlc is already there —
    // idempotent retry). Unused seqs leave harmless gaps.
    final baseSeq = await _reserveSeqs(request.vaultId, request.items.length);

    for (var i = 0; i < request.items.length; i++) {
      // Cooperative cancellation: client-sent x-client-cancelled frame
      // populates the responder's token. Idempotent on retry — any
      // items that did persist before the throw are dominated by the
      // client's next push via MvRegister.join.
      context?.cancellationToken?.throwIfCancelled();
      final item = request.items[i];
      final seq = baseSeq + i + 1;
      final result = await _putOne(
        vaultId: request.vaultId,
        col: col,
        item: item,
        seq: seq,
      );
      results.add(result);
      anyWrite = true;
    }

    final finalCursor = await _loadSeq(request.vaultId);

    if (anyWrite) {
      _notify?.publish('vault:${_qualifiedVault(request.vaultId)}', {
        'type': 'state_update',
        'cursor': finalCursor,
        if (request.sourceClientId != null)
          'sourceClientId': request.sourceClientId,
      });
    }

    context?.log.info(
      'state.put vault=${request.vaultId} items=${request.items.length} '
      'cursor=$finalCursor epoch=$currentEpoch',
    );

    return StatePutResponse(
      results: results,
      cursor: finalCursor,
      epoch: currentEpoch,
    );
  }

  /// MvRegister.join for one incoming TaggedValue (doc §5.1).
  ///
  /// 1. Load existing TaggedValues for `item.fileId`.
  /// 2. Drop any existing whose hlc is contained in `item.contextPacked`
  ///    AND whose hlc differs from the incoming hlc.
  /// 3. Insert the incoming value (or no-op if a record with the same
  ///    `(fileId, hlc)` already exists — idempotent retry).
  Future<StatePutResult> _putOne({
    required String vaultId,
    required String col,
    required StatePutItem item,
    required int seq,
  }) async {
    final existing = await _client.list(
      collection: col,
      filter: RecordFilter(equals: {'fileId': item.fileId}),
      options: const QueryOptions(limit: 1024),
    );

    final incomingContext = CausalContext.unpack(item.contextPacked);

    // Drop dominated values.
    for (final r in existing.records) {
      final existingHlcStr = r.payload['hlcPacked'] as String?;
      if (existingHlcStr == null) continue;
      if (existingHlcStr == item.hlcPacked) continue; // same value
      final existingHlc = Hlc.unpack(existingHlcStr);
      if (incomingContext.contains(existingHlc)) {
        try {
          await _client.delete(collection: col, id: r.id);
        } catch (_) {
          // Concurrent join may have already removed it — fine.
        }
      }
    }

    final recordId = _recordId(item.fileId, item.hlcPacked);
    final payload = {
      'fileId': item.fileId,
      'encryptedState': item.encryptedState,
      if (item.blobRef.isNotEmpty) 'blobRef': item.blobRef,
      'hlcPacked': item.hlcPacked,
      if (item.contextPacked.isNotEmpty) 'contextPacked': item.contextPacked,
      'serverSeq': seq,
      if (item.tombstone) 'tombstone': true,
      if (item.chunks.isNotEmpty) 'chunks': item.chunks,
    };

    int finalSeq = seq;
    HistoryOperation? op;
    try {
      await _client.create(collection: col, id: recordId, payload: payload);
      // Decide op flavour by whether this fileId had any prior records.
      final hadPrior = existing.records.isNotEmpty;
      op = item.tombstone
          ? HistoryOperation.delete
          : (hadPrior ? HistoryOperation.modify : HistoryOperation.create);
    } catch (_) {
      // Likely a retry — record already exists with the same id. Treat as
      // idempotent: return the existing serverSeq so the client doesn't
      // misinterpret a gap.
      final existingRec = await _client.get(collection: col, id: recordId);
      if (existingRec != null) {
        finalSeq = (existingRec.payload['serverSeq'] as int?) ?? seq;
      }
    }

    if (_historyEnabled && op != null) {
      await _writeHistoryEvent(vaultId, item, op, finalSeq);
    }

    return StatePutResult(fileId: item.fileId, serverSeq: finalSeq);
  }

  /// Append-only history event for [item]. Idempotent attempts are
  /// best-effort; failures here do not roll back the state write.
  Future<void> _writeHistoryEvent(
    String vaultId,
    StatePutItem item,
    HistoryOperation op,
    int seq,
  ) async {
    try {
      await _client.create(
        collection: _historyCollection(vaultId),
        id: const Uuid().v4(),
        payload: {
          'fileId': item.fileId,
          'blobRef': item.blobRef,
          'hlcPacked': item.hlcPacked,
          if (item.contextPacked.isNotEmpty) 'contextPacked': item.contextPacked,
          'operation': op.wire,
          'encryptedMeta': item.encryptedState,
          'serverSeq': seq,
          'createdAtMs': DateTime.now().toUtc().millisecondsSinceEpoch,
          if (item.chunks.isNotEmpty) 'chunks': item.chunks,
        },
      );
    } catch (_) {
      // Best-effort.
    }
  }

  // ---------------------------------------------------------------------------
  // GET (pull) — one StateRecord per surviving TaggedValue
  // ---------------------------------------------------------------------------

  @override
  Future<StateGetResponse> getStates(
    StateGetRequest request, {
    RpcContext? context,
  }) async {
    final currentEpoch = await _loadEpoch(request.vaultId);
    final currentSeq = await _loadSeq(request.vaultId);

    if (request.sinceCursor >= currentSeq) {
      return StateGetResponse(
        records: const [],
        cursor: currentSeq,
        epoch: currentEpoch,
      );
    }

    final col = _stateCollection(request.vaultId);
    // Push the cursor filter into the query so only the delta since the
    // client's cursor is materialised — NOT the whole collection. Loading the
    // entire keyspace per pull (the old listAllRecords path) made a caught-up
    // client still pull every record into memory, which OOM-killed the server
    // when records were large. `_rangeExpression` casts serverSeq to numeric,
    // so this is a correct numeric `> sinceCursor`.
    //
    // A single unbounded fetch (no keyset pagination): the Postgres adapter
    // orders a jsonb field lexicographically, not numerically, so paging by
    // `serverSeq > lastMax` would return non-contiguous pages and drop records.
    // The delta is normally small; the high limit only guards against an absurd
    // backlog (and is still bounded by the delta, never the whole collection).
    final resp = await _client.list(
      collection: col,
      filter: RecordFilter(
        range: {
          'serverSeq': RangeFilter(
            min: request.sinceCursor,
            includeMin: false,
          ),
        },
      ),
      options: const QueryOptions(limit: 1 << 30),
    );

    final matching = resp.records.toList()
      ..sort((a, b) => (a.payload['serverSeq'] as int)
          .compareTo(b.payload['serverSeq'] as int));

    final records = matching
        .map((r) => StateRecord(
              fileId: r.payload['fileId'] as String,
              encryptedState: r.payload['encryptedState'] as String,
              blobRef: (r.payload['blobRef'] as String?) ?? '',
              hlcPacked: r.payload['hlcPacked'] as String,
              contextPacked: (r.payload['contextPacked'] as String?) ?? '',
              serverSeq: r.payload['serverSeq'] as int,
              tombstone: (r.payload['tombstone'] as bool?) ?? false,
              chunks:
                  (r.payload['chunks'] as List?)?.cast<String>() ?? const [],
            ))
        .toList();

    context?.log.info(
      'state.get vault=${request.vaultId} since=${request.sinceCursor} '
      'records=${records.length} cursor=$currentSeq epoch=$currentEpoch',
    );

    return StateGetResponse(
      records: records,
      cursor: currentSeq,
      epoch: currentEpoch,
    );
  }

  // ---------------------------------------------------------------------------
  // WIPE
  // ---------------------------------------------------------------------------

  @override
  Future<StateWipeResponse> wipeVault(
    StateWipeRequest request, {
    RpcContext? context,
  }) async {

    final wipes = <Future<void>>[
      _client
          .deleteCollection(collection: _stateCollection(request.vaultId))
          .then((_) {}, onError: (e) {
        context?.log.warning('wipe: state collection delete error: $e');
      }),
      _client
          .delete(
            collection: _metaCollection(),
            id: _seqKey(request.vaultId),
          )
          .then((_) {}, onError: (_) {}),
      if (_blobClient != null)
        _blobClient
            .deleteCollection(_blobCollection(request.vaultId))
            .then((_) {}, onError: (e) {
          context?.log.warning('wipe: blob collection delete error: $e');
        }),
      if (_historyEnabled)
        _client
            .deleteCollection(
              collection: _historyCollection(request.vaultId),
            )
            .then((_) {}, onError: (e) {
          context?.log.warning('wipe: history collection delete error: $e');
        }),
      if (_historyEnabled)
        _client
            .deleteCollection(
              collection: _headsCollection(request.vaultId),
            )
            .then((_) {}, onError: (e) {
          context?.log.warning('wipe: heads collection delete error: $e');
        }),
    ];
    await Future.wait(wipes);

    final newEpoch = await _incrementEpoch(request.vaultId);

    _notify?.publish('vault:${_qualifiedVault(request.vaultId)}', {
      'type': 'state_wiped',
      'epoch': newEpoch,
      if (request.sourceClientId != null)
        'sourceClientId': request.sourceClientId,
    });

    context?.log.info(
      'state.wipe vault=${request.vaultId} new_epoch=$newEpoch',
    );
    return StateWipeResponse(epoch: newEpoch);
  }

  // ---------------------------------------------------------------------------
  // Meta helpers (seq + epoch)
  // ---------------------------------------------------------------------------

  Future<int> _loadSeq(String vaultId) async {
    final record = await _client.get(
      collection: _metaCollection(),
      id: _seqKey(vaultId),
    );
    if (record == null) return 0;
    return (record.payload['seq'] as int?) ?? 0;
  }

  Future<int> _reserveSeqs(String vaultId, int count) async {
    if (count <= 0) return await _loadSeq(vaultId);
    const maxAttempts = 64;
    final col = _metaCollection();
    final key = _seqKey(vaultId);
    final rand = math.Random();

    for (var attempt = 0; attempt < maxAttempts; attempt++) {
      final existing = await _client.get(collection: col, id: key);
      final current = (existing?.payload['seq'] as int?) ?? 0;
      final next = current + count;
      try {
        if (existing == null) {
          await _client.create(
            collection: col,
            id: key,
            payload: {'seq': next},
          );
        } else {
          await _client.update(
            collection: col,
            id: key,
            expectedVersion: existing.version,
            payload: {'seq': next},
          );
        }
        return current;
      } catch (e) {
        final msg = e.toString().toLowerCase();
        final conflict = msg.contains('conflict') ||
            msg.contains('already exists') ||
            msg.contains('version');
        if (!conflict) rethrow;
        final cap = math.min(200, 1 << math.min(attempt, 7));
        await Future<void>.delayed(Duration(milliseconds: rand.nextInt(cap + 1)));
      }
    }
    throw RpcException(
      'state.put: failed to reserve seq for vault=$vaultId after $maxAttempts attempts',
    );
  }

  Future<int> _loadEpoch(String vaultId) async {
    final record = await _client.get(
      collection: _metaCollection(),
      id: _epochKey(vaultId),
    );
    if (record == null) return 0;
    return (record.payload['epoch'] as int?) ?? 0;
  }

  Future<int> _incrementEpoch(String vaultId) async {
    const maxAttempts = 16;
    final col = _metaCollection();
    final key = _epochKey(vaultId);
    for (var attempt = 0; attempt < maxAttempts; attempt++) {
      final existing = await _client.get(collection: col, id: key);
      final current = (existing?.payload['epoch'] as int?) ?? 0;
      final next = current + 1;
      try {
        if (existing == null) {
          await _client.create(
            collection: col,
            id: key,
            payload: {'epoch': next},
          );
        } else {
          await _client.update(
            collection: col,
            id: key,
            expectedVersion: existing.version,
            payload: {'epoch': next},
          );
        }
        return next;
      } catch (e) {
        final msg = e.toString().toLowerCase();
        final conflict = msg.contains('conflict') ||
            msg.contains('already exists') ||
            msg.contains('version');
        if (!conflict) rethrow;
        await Future<void>.delayed(Duration(milliseconds: 5 * (attempt + 1)));
      }
    }
    throw RpcException(
      'state.wipeVault: failed to bump epoch for vault=$vaultId',
    );
  }

}

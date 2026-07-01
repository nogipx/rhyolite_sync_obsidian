// Integration smoke per doc §14.4: two simulated clients editing in
// parallel, merging via one in-memory server, end state identical.
//
// Bypasses SyncEngine + WebSocket + cipher impl + disk I/O so the test
// focuses on the CRDT pipeline: applyLocal → push → server join →
// pull → applyRemote → (resolver) → re-push. If convergence holds here,
// the surrounding plumbing can only break it through transport bugs.

import 'dart:convert';
import 'dart:typed_data';

import 'package:convergent/convergent.dart';
import 'package:rhyolite_sync/rhyolite_sync.dart';
import 'package:rhyolite_sync/src/crypto/i_vault_cipher.dart';
import 'package:rhyolite_sync/src/local/local_blob_store.dart';
import 'package:rhyolite_sync/src/sync_v3/file_state.dart';
import 'package:rhyolite_sync/src/sync_v3/file_state_store.dart';
import 'package:rhyolite_sync/src/sync_v3/state_conflict_resolver.dart';
import 'package:rhyolite_sync_server/src/state_sync_responder.dart';
import 'package:rpc_blob/rpc_blob.dart';
import 'package:rpc_data/rpc_data.dart';
import 'package:test/test.dart';

const _v = 'vault-int';

class _IdentityCipher implements IVaultCipher {
  @override
  Future<Uint8List> encrypt(Uint8List plaintext) async => plaintext;
  @override
  Future<Uint8List> decrypt(Uint8List ciphertext) async => ciphertext;
}

/// One simulated client: its own [FileStateStore], shared cipher, talks
/// to the same [StateSyncResponder] as its peer.
class _Client {
  _Client({
    required this.store,
    required this.cipher,
    required this.blobStore,
    required this.label,
  });

  final FileStateStore store;
  final IVaultCipher cipher;
  final LocalBlobStore blobStore;
  final String label;

  /// Produce a new local write — fresh HLC under [FileStateStore.applyLocal].
  void write(String fileId, {required String blobRef, String path = 'note.md'}) {
    final hlc = store.nextHlc();
    store.applyLocal(FileState(
      fileId: fileId,
      path: path,
      blobRef: blobRef,
      sizeBytes: blobRef.length,
      hlc: hlc,
    ));
  }

  void delete(String fileId) {
    final current = store.get(fileId);
    if (current == null) return;
    final hlc = store.nextHlc();
    store.applyLocal(current.copyWith(
      hlc: hlc,
      tombstone: true,
      blobRef: '',
      sizeBytes: 0,
    ));
  }

  Future<void> push(StateSyncResponder server) async {
    final items = <StatePutItem>[];
    for (final fileId in store.fileIds) {
      final reg = store.registerFor(fileId);
      if (reg == null || reg.hasConflict) continue;
      final tv = reg.values.first;
      final state = tv.value;
      final synced = store.lastSyncedBlobRefFor(fileId);
      final neverPushed = synced == null;
      final isNew = neverPushed && !state.tombstone;
      final isModified = synced != null && synced != state.blobRef;
      final isTombstoneToCommit = state.tombstone && synced != null;
      if (!(isNew || isModified || isTombstoneToCommit)) continue;
      final wire = utf8.encode(jsonEncode(state.toWirePayload()));
      final enc = await cipher.encrypt(Uint8List.fromList(wire));
      items.add(StatePutItem(
        fileId: fileId,
        encryptedState: base64Encode(enc),
        blobRef: state.tombstone ? '' : state.blobRef,
        hlcPacked: state.hlc.pack(),
        tombstone: state.tombstone,
        contextPacked: tv.context.pack(),
        chunks: state.chunks,
      ));
    }
    if (items.isEmpty) return;
    await server.putStates(StatePutRequest(
      vaultId: _v,
      items: items,
      sourceClientId: label,
    ));
    for (final item in items) {
      store.recordSyncedBlobRef(item.fileId, item.blobRef);
    }
    // Deliberately do NOT advance cursor after push — response.cursor
    // includes records from other devices we have not pulled yet.
  }

  Future<void> pull(StateSyncResponder server) async {
    final response = await server.getStates(StateGetRequest(
      vaultId: _v,
      sinceCursor: store.serverCursor,
    ));
    final byFile = <String, List<StateRecord>>{};
    for (final r in response.records) {
      byFile.putIfAbsent(r.fileId, () => []).add(r);
    }
    for (final entry in byFile.entries) {
      final tagged = <TaggedValue<FileState>>[];
      for (final r in entry.value) {
        final plain = await cipher.decrypt(base64Decode(r.encryptedState));
        final wire = FileState.wirePayloadFromBytes(plain);
        final state = FileState(
          fileId: r.fileId,
          path: wire['path'] as String,
          blobRef: (wire['blobRef'] as String?) ?? '',
          sizeBytes: (wire['sizeBytes'] as int?) ?? 0,
          hlc: Hlc.unpack(r.hlcPacked),
          tombstone: (wire['tombstone'] as bool?) ?? r.tombstone,
          chunks: r.chunks,
        );
        final ctx = r.contextPacked.isEmpty
            ? const CausalContext.empty()
            : CausalContext.unpack(r.contextPacked);
        tagged.add(TaggedValue<FileState>(state, state.hlc, context: ctx));
      }
      // Disable HLC skew defence in tests so they don't depend on wall
      // clock proximity to the synthetic HLCs.
      store.applyRemote(entry.key, tagged, maxClockSkewMs: null);
    }
    store.setServerCursor(response.cursor);
  }

  /// Resolve any multi-value register via the application resolver and
  /// seal it with applyLocal. Returns the list of fileIds it touched.
  Future<List<String>> resolveConflicts() async {
    final resolver = StateConflictResolver(
      store: store,
      blobStore: blobStore,
      vaultId: _v,
      nodeId: store.deviceId,
    );
    final touched = <String>[];
    for (final fileId in store.fileIds.toList()) {
      final reg = store.registerFor(fileId);
      if (reg == null || !reg.hasConflict) continue;
      final outcome = await resolver.resolve(reg.allValues);
      switch (outcome) {
        case StateMergeMerged(:final merged):
          store.applyLocal(merged.copyWith(hlc: store.nextHlc()));
        case StateMergeConflictCopy(:final winner):
          store.applyLocal(winner.copyWith(hlc: store.nextHlc()));
        case StateMergeWinnerOnlyLossy(:final winner):
          // Convergence test uses per-client in-memory blob stores, so
          // the OTHER client's blob is unreachable when the resolver
          // runs. Convergence still holds (both sides agree on the
          // winner) even though the loser's bytes are unrecoverable.
          store.applyLocal(winner.copyWith(hlc: store.nextHlc()));
      }
      touched.add(fileId);
    }
    return touched;
  }
}

Future<_Client> _newClient(
  IDataClient client,
  IVaultCipher cipher,
  String label,
) async {
  final store = FileStateStore(client: client, vaultId: _v);
  await store.load();
  return _Client(
    store: store,
    cipher: cipher,
    blobStore: LocalBlobStore(InMemoryBlobRepository()),
    label: label,
  );
}

void main() {
  late StateSyncResponder server;
  late _Client a;
  late _Client b;

  setUp(() async {
    // One server. Two clients, each with its own local IDataClient.
    final serverEnv = await DataServiceFactory.inMemory();
    addTearDown(serverEnv.dispose);
    server = StateSyncResponder(client: serverEnv.client);

    final aEnv = await DataServiceFactory.inMemory();
    addTearDown(aEnv.dispose);
    final bEnv = await DataServiceFactory.inMemory();
    addTearDown(bEnv.dispose);

    final cipher = _IdentityCipher();
    a = await _newClient(aEnv.client, cipher, 'A');
    b = await _newClient(bEnv.client, cipher, 'B');
  });

  Map<String, String> _snapshot(_Client c) {
    // {fileId -> blobRef|TOMB|CONFLICT}
    final m = <String, String>{};
    for (final fileId in c.store.fileIds) {
      final reg = c.store.registerFor(fileId)!;
      if (reg.hasConflict) {
        m[fileId] = 'CONFLICT:${reg.values.map((v) => v.value.blobRef).toSet().join(",")}';
      } else {
        final s = reg.singleValue!;
        m[fileId] = s.tombstone ? 'TOMB' : s.blobRef;
      }
    }
    return m;
  }

  group('Two-client convergence — sequential causal flow', () {
    test('A writes → B pulls → B sees A\'s file', () async {
      a.write('f1', blobRef: 'sha-A1');
      await a.push(server);

      await b.pull(server);

      expect(b.store.get('f1')?.blobRef, 'sha-A1');
      expect(b.store.registerFor('f1')!.hasConflict, isFalse);
    });

    test('A writes → B pulls → B modifies → A pulls → both see B\'s edit',
        () async {
      a.write('f1', blobRef: 'sha-A1');
      await a.push(server);
      await b.pull(server);

      b.write('f1', blobRef: 'sha-B2');
      await b.push(server);
      await a.pull(server);

      expect(a.store.get('f1')?.blobRef, 'sha-B2');
      expect(b.store.get('f1')?.blobRef, 'sha-B2');
      // Server's register for f1 should be collapsed to one value because
      // B's context dominates A's hlc.
      expect(_snapshot(a), _snapshot(b));
    });

    test('idempotent re-pull does not change state', () async {
      a.write('f1', blobRef: 'sha-A1');
      await a.push(server);
      await b.pull(server);
      final before = _snapshot(b);

      await b.pull(server); // re-pull, no new records since cursor
      await b.pull(server);

      expect(_snapshot(b), before);
    });
  });

  group('Two-client convergence — concurrent edits (true CRDT case)', () {
    test('both write f1 without seeing each other → multi-value register',
        () async {
      a.write('f1', blobRef: 'sha-A1');
      b.write('f1', blobRef: 'sha-B1');

      await a.push(server);
      await b.push(server);

      await a.pull(server);
      await b.pull(server);

      // Both must see the same 2-value register.
      expect(a.store.hasConflict('f1'), isTrue);
      expect(b.store.hasConflict('f1'), isTrue);
      expect(
        a.store.currentValues('f1').map((s) => s.blobRef).toSet(),
        equals(b.store.currentValues('f1').map((s) => s.blobRef).toSet()),
      );
      expect(
        a.store.currentValues('f1').map((s) => s.blobRef).toSet(),
        {'sha-A1', 'sha-B1'},
      );
    });

    test('after both resolve, blobRef winner is identical on both replicas',
        () async {
      a.write('f1', blobRef: 'sha-A1');
      b.write('f1', blobRef: 'sha-B1');
      await a.push(server);
      await b.push(server);
      await a.pull(server);
      await b.pull(server);

      // Each client resolves independently. Resolver is deterministic on
      // the same input set (no base ref → LWW on hlc).
      await a.resolveConflicts();
      await b.resolveConflicts();

      // Both pick the LWW-winner content. They each stamp it with a fresh
      // local HLC so the FileState objects aren't equal, but blobRef must
      // match — that is what content-convergence means.
      expect(a.store.get('f1')?.blobRef, equals(b.store.get('f1')?.blobRef),
          reason: 'deterministic resolver must agree on content');
    });

    test('eventual convergence — both register collapse after enough rounds',
        () async {
      a.write('f1', blobRef: 'sha-A1');
      b.write('f1', blobRef: 'sha-B1');

      // Fixed-point: push/pull/resolve until both clients have the same
      // single-value register. Concurrent resolve produces new TaggedValues
      // each round that the OTHER side hasn't seen yet, so a single round
      // is not enough.
      for (var round = 0; round < 5; round++) {
        await a.push(server);
        await b.push(server);
        await a.pull(server);
        await b.pull(server);
        await a.resolveConflicts();
        await b.resolveConflicts();
        if (_snapshot(a) == _snapshot(b) &&
            !a.store.hasConflict('f1') &&
            !b.store.hasConflict('f1')) {
          break;
        }
      }

      // Final convergence: both end with one canonical value, same content.
      expect(a.store.hasConflict('f1'), isFalse,
          reason: 'A must converge to single value');
      expect(b.store.hasConflict('f1'), isFalse,
          reason: 'B must converge to single value');
      expect(_snapshot(a), _snapshot(b));
    });

    test('concurrent edits on DIFFERENT files just merge cleanly', () async {
      a.write('fA', blobRef: 'sha-A');
      b.write('fB', blobRef: 'sha-B');

      await a.push(server);
      await b.push(server);

      await a.pull(server);
      await b.pull(server);

      expect(_snapshot(a), {'fA': 'sha-A', 'fB': 'sha-B'});
      expect(_snapshot(b), {'fA': 'sha-A', 'fB': 'sha-B'});
    });
  });

  group('Two-client convergence — tombstone vs edit', () {
    test('A deletes while B edits → add-wins, both end identical', () async {
      // Initial state seeded on both.
      a.write('f1', blobRef: 'sha-init');
      await a.push(server);
      await b.pull(server);

      // Concurrent: A deletes, B edits.
      a.delete('f1');
      b.write('f1', blobRef: 'sha-B-new');

      await a.push(server);
      await b.push(server);
      await a.pull(server);
      await b.pull(server);

      // Both register multi-value (tombstone + edit).
      expect(a.store.hasConflict('f1'), isTrue);
      expect(b.store.hasConflict('f1'), isTrue);

      await a.resolveConflicts();
      await b.resolveConflicts();

      // Resolver: tombstone vs edit → edit wins (add-wins).
      expect(a.store.get('f1')?.tombstone, isFalse);
      expect(b.store.get('f1')?.tombstone, isFalse);
      expect(a.store.get('f1')?.blobRef, 'sha-B-new');
      expect(b.store.get('f1')?.blobRef, 'sha-B-new');
    });
  });

  group('Two-client convergence — many writes, random delivery', () {
    test('20 writes interleaved across A/B → identical final snapshot',
        () async {
      // Mixed pattern of writes and syncs. The bus order is fixed in this
      // test (deterministic) but each step exercises the join in a fresh
      // configuration.
      for (var i = 0; i < 10; i++) {
        a.write('f-shared', blobRef: 'A-$i');
        await a.push(server);
        await b.pull(server);

        b.write('f-shared', blobRef: 'B-$i');
        await b.push(server);
        await a.pull(server);
      }

      // After 20 round-trips, neither side has a conflict (every write
      // dominated the previous one causally).
      expect(a.store.hasConflict('f-shared'), isFalse);
      expect(b.store.hasConflict('f-shared'), isFalse);
      expect(_snapshot(a), _snapshot(b));
    });

    test('burst of concurrent writes eventually converges', () async {
      for (var i = 0; i < 5; i++) {
        a.write('f-$i', blobRef: 'A-$i');
        b.write('f-$i', blobRef: 'B-$i');
      }
      await a.push(server);
      await b.push(server);
      await a.pull(server);
      await b.pull(server);

      // After first round-trip every fileId is in conflict on both sides
      // with the same 2 values.
      for (var i = 0; i < 5; i++) {
        expect(a.store.hasConflict('f-$i'), isTrue);
        expect(
          a.store.currentValues('f-$i').map((s) => s.blobRef).toSet(),
          b.store.currentValues('f-$i').map((s) => s.blobRef).toSet(),
        );
      }

      // Fixed-point convergence — concurrent resolve produces new
      // TaggedValues neither side has seen, so more than one round needed.
      for (var round = 0; round < 5; round++) {
        await a.resolveConflicts();
        await b.resolveConflicts();
        await a.push(server);
        await b.push(server);
        await a.pull(server);
        await b.pull(server);
        if (_snapshot(a) == _snapshot(b) &&
            !List.generate(5, (i) => a.store.hasConflict('f-$i'))
                .any((c) => c)) {
          break;
        }
      }

      expect(_snapshot(a), _snapshot(b));
      for (var i = 0; i < 5; i++) {
        expect(a.store.hasConflict('f-$i'), isFalse);
      }
    });
  });
}

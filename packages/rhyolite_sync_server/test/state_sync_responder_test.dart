import 'package:convergent/convergent.dart';
import 'package:rpc_data/rpc_data.dart';
import 'package:rhyolite_sync/rhyolite_sync.dart';
import 'package:rhyolite_sync_server/src/state_sync_responder.dart';
import 'package:test/test.dart';

const _v = 'v1';

Hlc _hlc(int ms, String node) => Hlc(ms, 0, node);

StatePutItem _put(
  String fileId, {
  String state = 'enc',
  required Hlc hlc,
  Map<String, Hlc> context = const {},
  bool tombstone = false,
  String blobRef = 'sha-1',
}) =>
    StatePutItem(
      fileId: fileId,
      encryptedState: state,
      blobRef: tombstone ? '' : blobRef,
      hlcPacked: hlc.pack(),
      tombstone: tombstone,
      contextPacked: CausalContext.from(context).pack(),
    );

void main() {
  group('StateSyncResponder.putStates — Δ-state CRDT join', () {
    test('first put creates a record and assigns serverSeq=1', () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final r = StateSyncResponder(client: env.client);

      final response = await r.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1', hlc: _hlc(100, 'A'))],
      ));
      expect(response.results.single.fileId, 'f1');
      expect(response.results.single.serverSeq, 1);
      expect(response.cursor, 1);
      expect(response.epoch, 0);
    });

    test('concurrent writes by two devices both survive (multi-value)',
        () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final r = StateSyncResponder(client: env.client);

      // A writes without seeing anything.
      await r.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1', state: 'A', hlc: _hlc(100, 'A'))],
      ));
      // B writes ALSO without seeing A → concurrent.
      await r.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1', state: 'B', hlc: _hlc(110, 'B'))],
      ));

      final pull = await r.getStates(
        StateGetRequest(vaultId: _v, sinceCursor: 0),
      );
      // Both TaggedValues for f1 survive.
      final f1 = pull.records.where((x) => x.fileId == 'f1').toList();
      expect(f1.length, 2);
      expect(f1.map((x) => x.encryptedState).toSet(), {'A', 'B'});
    });

    test('write that has seen prior values supersedes them (dominated drop)',
        () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final r = StateSyncResponder(client: env.client);

      final hA = _hlc(100, 'A');
      final hB = _hlc(110, 'B');
      await r.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1', state: 'A', hlc: hA)],
      ));
      await r.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1', state: 'B', hlc: hB)],
      ));
      // C writes having seen BOTH A and B.
      final hC = _hlc(200, 'C');
      await r.putStates(StatePutRequest(
        vaultId: _v,
        items: [
          _put('f1', state: 'C', hlc: hC, context: {'A': hA, 'B': hB}),
        ],
      ));

      final pull = await r.getStates(
        StateGetRequest(vaultId: _v, sinceCursor: 0),
      );
      final f1 = pull.records.where((x) => x.fileId == 'f1').toList();
      expect(f1.length, 1, reason: 'A and B should be dropped by C');
      expect(f1.single.encryptedState, 'C');
    });

    test('idempotent on retry — same (fileId, hlc) is not duplicated',
        () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final r = StateSyncResponder(client: env.client);

      final hA = _hlc(100, 'A');
      final first = await r.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1', state: 'A', hlc: hA)],
      ));
      final retry = await r.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1', state: 'A', hlc: hA)],
      ));

      expect(retry.results.single.serverSeq, first.results.single.serverSeq,
          reason: 'idempotent retry must report the original serverSeq');

      final pull = await r.getStates(
        StateGetRequest(vaultId: _v, sinceCursor: 0),
      );
      expect(pull.records.where((x) => x.fileId == 'f1').length, 1);
    });

    test('join is commutative across delivery orderings', () async {
      // Two devices each issue one write concurrently. Apply in (A,B) and
      // (B,A); the final register state must be identical.
      final hA = _hlc(100, 'A');
      final hB = _hlc(110, 'B');

      Future<Set<String>> finalStates(List<StatePutItem> order) async {
        final env = await DataServiceFactory.inMemory();
        addTearDown(env.dispose);
        final r = StateSyncResponder(client: env.client);
        for (final i in order) {
          await r.putStates(StatePutRequest(vaultId: _v, items: [i]));
        }
        final pull = await r.getStates(
          StateGetRequest(vaultId: _v, sinceCursor: 0),
        );
        return pull.records
            .where((x) => x.fileId == 'f1')
            .map((x) => x.encryptedState)
            .toSet();
      }

      final a = _put('f1', state: 'A', hlc: hA);
      final b = _put('f1', state: 'B', hlc: hB);
      expect(await finalStates([a, b]), equals(await finalStates([b, a])));
    });
  });

  group('StateSyncResponder.putStates — seq reservation under contention',
      () {
    test('16 concurrent batches × 4 items succeed without reservation failure',
        () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final r = StateSyncResponder(client: env.client);

      Future<StatePutResponse> batch(int i) => r.putStates(StatePutRequest(
            vaultId: _v,
            items: List.generate(
              4,
              (k) => _put(
                'f-$i-$k',
                hlc: _hlc(1000 + i * 10 + k, 'A$i'),
                blobRef: 'sha-$i-$k',
              ),
            ),
          ));

      final responses = await Future.wait(List.generate(16, batch));

      final allSeqs = <int>[];
      for (final resp in responses) {
        for (final result in resp.results) {
          allSeqs.add(result.serverSeq);
        }
      }
      expect(allSeqs.length, 64);
      expect(allSeqs.toSet().length, 64, reason: 'every seq must be unique');
    });
  });

  group('StateSyncResponder.getStates — pull-by-cursor', () {
    test('returns records with serverSeq > sinceCursor ordered by seq',
        () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final r = StateSyncResponder(client: env.client);

      await r.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1', state: 'A', hlc: _hlc(100, 'A'))],
      ));
      await r.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f2', state: 'B', hlc: _hlc(110, 'A'))],
      ));
      await r.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f3', state: 'C', hlc: _hlc(120, 'A'))],
      ));

      final got = await r.getStates(
        StateGetRequest(vaultId: _v, sinceCursor: 1),
      );
      expect(got.records.map((r) => r.fileId).toList(), ['f2', 'f3']);
      expect(got.cursor, 3);
    });

    test('returns empty when up to date', () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final r = StateSyncResponder(client: env.client);

      await r.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1', state: 'A', hlc: _hlc(100, 'A'))],
      ));
      final got = await r.getStates(
        StateGetRequest(vaultId: _v, sinceCursor: 1),
      );
      expect(got.records, isEmpty);
      expect(got.cursor, 1);
    });

    test('superseding write replaces the prior record on pull', () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final r = StateSyncResponder(client: env.client);

      final hA1 = _hlc(100, 'A');
      await r.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1', state: 'A', hlc: hA1)],
      ));
      // A writes again, having seen its own prior.
      final hA2 = _hlc(200, 'A');
      await r.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1', state: 'A2', hlc: hA2, context: {'A': hA1})],
      ));

      final pull = await r.getStates(
        StateGetRequest(vaultId: _v, sinceCursor: 0),
      );
      final f1 = pull.records.where((x) => x.fileId == 'f1').toList();
      expect(f1.length, 1);
      expect(f1.single.encryptedState, 'A2');
    });
  });

  group('StateSyncResponder — epoch + wipe', () {
    test('wipe deletes records and bumps epoch', () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final r = StateSyncResponder(client: env.client);

      await r.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1', state: 'A', hlc: _hlc(100, 'A'))],
      ));
      var pull = await r.getStates(
        StateGetRequest(vaultId: _v, sinceCursor: 0),
      );
      expect(pull.records.length, 1);
      expect(pull.epoch, 0);

      final wipe = await r.wipeVault(StateWipeRequest(vaultId: _v));
      expect(wipe.epoch, 1);

      pull = await r.getStates(StateGetRequest(vaultId: _v, sinceCursor: 0));
      expect(pull.records, isEmpty);
      expect(pull.epoch, 1);
    });

    test('put with stale expectedEpoch is rejected, no writes', () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final r = StateSyncResponder(client: env.client);

      await r.wipeVault(StateWipeRequest(vaultId: _v)); // epoch 0 → 1
      final stale = await r.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1', state: 'A', hlc: _hlc(100, 'A'))],
        expectedEpoch: 0,
      ));
      expect(stale.epochMismatch, isTrue);
      expect(stale.epoch, 1);

      final pull = await r.getStates(
        StateGetRequest(vaultId: _v, sinceCursor: 0),
      );
      expect(pull.records, isEmpty,
          reason: 'rejected put must not write any records');
    });
  });
}

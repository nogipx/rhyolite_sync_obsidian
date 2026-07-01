import 'package:convergent/convergent.dart';
import 'package:rpc_data/rpc_data.dart';
import 'package:rhyolite_sync/rhyolite_sync.dart';
import 'package:rhyolite_sync_server/src/history_responder.dart';
import 'package:rhyolite_sync_server/src/state_sync_responder.dart';
import 'package:test/test.dart';

const _v = 'v1';

Hlc _hlc(String packed) => Hlc.unpack(packed);

StatePutItem _put(
  String fileId, {
  String state = 'enc',
  String hlc = '100-0-A',
  Map<String, String> context = const {},
  bool tombstone = false,
  String blobRef = 'sha-1',
}) =>
    StatePutItem(
      fileId: fileId,
      encryptedState: state,
      blobRef: tombstone ? '' : blobRef,
      hlcPacked: hlc,
      tombstone: tombstone,
      contextPacked: CausalContext.from(
        {for (final e in context.entries) e.key: _hlc(e.value)},
      ).pack(),
    );

Future<({StateSyncResponder state, HistoryResponder history})> _setup(
  IDataClient client,
) async {
  final state = StateSyncResponder(client: client);
  final history = HistoryResponder(client: client);
  return (state: state, history: history);
}

void main() {
  group('history written as side-effect of putStates', () {
    test('create operation writes one history event', () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final s = await _setup(env.client);

      await s.state.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1', blobRef: 'sha-A')],
      ));

      final h = await s.history.getHistory(HistoryGetRequest(vaultId: _v));
      expect(h.events.length, 1);
      expect(h.events.first.fileId, 'f1');
      expect(h.events.first.blobRef, 'sha-A');
      expect(h.events.first.operation, HistoryOperation.create);
    });

    test('modify operation: second causally-dominating write produces modify',
        () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final s = await _setup(env.client);

      await s.state.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1', blobRef: 'sha-A', hlc: '100-0-A')],
      ));
      await s.state.putStates(StatePutRequest(
        vaultId: _v,
        items: [
          _put(
            'f1',
            blobRef: 'sha-B',
            hlc: '200-0-A',
            context: {'A': '100-0-A'},
          ),
        ],
      ));

      final h = await s.history.getHistory(HistoryGetRequest(vaultId: _v));
      expect(h.events.length, 2);
      final ops = h.events.map((e) => e.operation).toSet();
      expect(ops, {HistoryOperation.create, HistoryOperation.modify});
    });

    test('tombstone operation: delete event', () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final s = await _setup(env.client);

      await s.state.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1', blobRef: 'sha-A', hlc: '100-0-A')],
      ));
      await s.state.putStates(StatePutRequest(
        vaultId: _v,
        items: [
          _put(
            'f1',
            tombstone: true,
            hlc: '200-0-A',
            context: {'A': '100-0-A'},
          ),
        ],
      ));

      final delEvent = (await s.history.getHistory(HistoryGetRequest(
        vaultId: _v,
        fileId: 'f1',
      )))
          .events
          .firstWhere((e) => e.operation == HistoryOperation.delete);
      expect(delEvent.blobRef, '');
      expect(delEvent.hlcPacked, '200-0-A');
    });

    test('idempotent retry does NOT duplicate the history event', () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final s = await _setup(env.client);

      // Same (fileId, hlc) put twice — second one is a no-op on the state
      // record. The history event is best-effort, so the second attempt is
      // tolerated even if it writes a duplicate; the important guarantee
      // is that the state row stays singular.
      await s.state.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1', blobRef: 'sha-A', hlc: '100-0-A')],
      ));
      await s.state.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1', blobRef: 'sha-A', hlc: '100-0-A')],
      ));

      final h = await s.history.getHistory(HistoryGetRequest(vaultId: _v));
      // First put produced one event. The retry skips the insert (id already
      // exists) and therefore writes no second event either.
      expect(h.events.length, 1);
    });

    test('hlcPacked carries through to event', () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final s = await _setup(env.client);
      await s.state.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1', hlc: '999-0-NodeX')],
      ));
      final h = await s.history.getHistory(HistoryGetRequest(vaultId: _v));
      expect(h.events.single.hlcPacked, '999-0-NodeX');
    });
  });

  group('HistoryResponder.getHistory', () {
    test('filters by fileId, fromHlc, beforeHlc', () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final s = await _setup(env.client);

      await s.state.putStates(StatePutRequest(
        vaultId: _v,
        items: [
          _put('fA', hlc: '100-0-A', blobRef: 'shaA1'),
          _put('fB', hlc: '110-0-A', blobRef: 'shaB1'),
        ],
      ));
      await s.state.putStates(StatePutRequest(
        vaultId: _v,
        items: [
          _put(
            'fA',
            hlc: '200-0-A',
            blobRef: 'shaA2',
            context: {'A': '100-0-A'},
          ),
          _put(
            'fB',
            hlc: '210-0-A',
            blobRef: 'shaB2',
            context: {'A': '110-0-A'},
          ),
        ],
      ));

      final onlyA = await s.history
          .getHistory(HistoryGetRequest(vaultId: _v, fileId: 'fA'));
      expect(onlyA.events.every((e) => e.fileId == 'fA'), isTrue);
      expect(onlyA.events.length, 2);

      final beforeMid = await s.history.getHistory(HistoryGetRequest(
        vaultId: _v,
        beforeHlcPacked: '150-0-A',
      ));
      expect(beforeMid.events.length, 2);
    });

    test('default ordering is newest first', () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final s = await _setup(env.client);
      await s.state.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1', hlc: '100-0-A', blobRef: 'sha1')],
      ));
      await s.state.putStates(StatePutRequest(
        vaultId: _v,
        items: [
          _put(
            'f1',
            hlc: '300-0-A',
            blobRef: 'sha3',
            context: {'A': '100-0-A'},
          ),
        ],
      ));
      await s.state.putStates(StatePutRequest(
        vaultId: _v,
        items: [
          _put(
            'f1',
            hlc: '200-0-A',
            blobRef: 'sha2',
            context: {'A': '300-0-A'},
          ),
        ],
      ));
      final h = await s.history.getHistory(HistoryGetRequest(vaultId: _v));
      expect(h.events.map((e) => e.hlcPacked).toList(),
          ['300-0-A', '200-0-A', '100-0-A']);
    });
  });

  group('HistoryResponder.deleteEvents', () {
    test('deletes by id, returns count', () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final s = await _setup(env.client);
      await s.state.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1')],
      ));
      final h = await s.history.getHistory(HistoryGetRequest(vaultId: _v));
      final eventId = h.events.single.eventId;

      final r = await s.history.deleteEvents(
        HistoryDeleteEventsRequest(vaultId: _v, eventIds: [eventId]),
      );
      expect(r.deleted, 1);
      final after = await s.history.getHistory(HistoryGetRequest(vaultId: _v));
      expect(after.events, isEmpty);
    });

    test('unknown ids count zero, no error', () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final s = await _setup(env.client);
      final r = await s.history.deleteEvents(
        HistoryDeleteEventsRequest(
          vaultId: _v,
          eventIds: ['nonexistent-1', 'nonexistent-2'],
        ),
      );
      expect(r.deleted, 0);
    });

    test('empty list is a no-op', () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final s = await _setup(env.client);
      final r = await s.history.deleteEvents(
        HistoryDeleteEventsRequest(vaultId: _v, eventIds: const []),
      );
      expect(r.deleted, 0);
    });
  });

  group('HistoryResponder heads', () {
    test('reportHistoryHead upserts, getHistoryHeads returns it', () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final s = await _setup(env.client);

      await s.history.reportHistoryHead(ReportHistoryHeadRequest(
        vaultId: _v,
        deviceId: 'd1',
        headSeq: 5,
      ));
      await s.history.reportHistoryHead(ReportHistoryHeadRequest(
        vaultId: _v,
        deviceId: 'd2',
        headSeq: 10,
      ));
      final got = await s.history
          .getHistoryHeads(GetHistoryHeadsRequest(vaultId: _v));
      expect(got.heads.length, 2);
      final byId = {for (final h in got.heads) h.deviceId: h};
      expect(byId['d1']?.headSeq, 5);
      expect(byId['d2']?.headSeq, 10);
    });

    test('reportHistoryHead bumps existing record forward', () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final s = await _setup(env.client);

      await s.history.reportHistoryHead(ReportHistoryHeadRequest(
        vaultId: _v,
        deviceId: 'd1',
        headSeq: 5,
      ));
      await s.history.reportHistoryHead(ReportHistoryHeadRequest(
        vaultId: _v,
        deviceId: 'd1',
        headSeq: 12,
      ));
      final got = await s.history
          .getHistoryHeads(GetHistoryHeadsRequest(vaultId: _v));
      expect(got.heads.single.headSeq, 12);
    });

    test('reportHistoryHead refuses to regress (lower seq is ignored)',
        () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final s = await _setup(env.client);

      await s.history.reportHistoryHead(ReportHistoryHeadRequest(
        vaultId: _v,
        deviceId: 'd1',
        headSeq: 50,
      ));
      await s.history.reportHistoryHead(ReportHistoryHeadRequest(
        vaultId: _v,
        deviceId: 'd1',
        headSeq: 10,
      ));
      final got = await s.history
          .getHistoryHeads(GetHistoryHeadsRequest(vaultId: _v));
      expect(got.heads.single.headSeq, 50);
    });

    test('reportHistoryHead persists frontierPacked when supplied',
        () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final s = await _setup(env.client);

      const frontier = 'A=1-2-A;B=3-4-B';
      await s.history.reportHistoryHead(ReportHistoryHeadRequest(
        vaultId: _v,
        deviceId: 'd1',
        headSeq: 5,
        frontierPacked: frontier,
      ));
      final got = await s.history
          .getHistoryHeads(GetHistoryHeadsRequest(vaultId: _v));
      expect(got.heads.single.frontierPacked, frontier);
    });

    test('frontierPacked defaults to empty when client omits it', () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final s = await _setup(env.client);

      await s.history.reportHistoryHead(ReportHistoryHeadRequest(
        vaultId: _v,
        deviceId: 'd1',
        headSeq: 5,
      ));
      final got = await s.history
          .getHistoryHeads(GetHistoryHeadsRequest(vaultId: _v));
      expect(got.heads.single.frontierPacked, '');
    });

    test('getHistoryHeads omits + sweeps stale rows (older than 90d)',
        () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final s = await _setup(env.client);

      // Stale row planted directly with an old updatedAtMs.
      const headsCol = '${_v}_history_heads';
      final staleMs = DateTime.now()
              .subtract(const Duration(days: 100))
              .toUtc()
              .millisecondsSinceEpoch;
      await env.client.create(
        collection: headsCol,
        id: 'old-device',
        payload: {
          'deviceId': 'old-device',
          'headSeq': 1,
          'updatedAtMs': staleMs,
        },
      );
      // A fresh row goes through the normal API path.
      await s.history.reportHistoryHead(ReportHistoryHeadRequest(
        vaultId: _v,
        deviceId: 'fresh-device',
        headSeq: 2,
      ));

      // First call: stale row excluded from the response.
      final got = await s.history
          .getHistoryHeads(GetHistoryHeadsRequest(vaultId: _v));
      expect(got.heads.length, 1);
      expect(got.heads.single.deviceId, 'fresh-device');

      // Sweep is fire-and-forget — let it land.
      await Future<void>.delayed(const Duration(milliseconds: 50));
      final remaining =
          await env.client.listAllRecords(collection: headsCol);
      expect(remaining.map((r) => r.id).toSet(), {'fresh-device'});
    });
  });

  group('wipeVault clears history', () {
    test('after wipe, getHistory is empty', () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final s = await _setup(env.client);
      await s.state.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1'), _put('f2', hlc: '110-0-A', blobRef: 'sha-B')],
      ));
      var h = await s.history.getHistory(HistoryGetRequest(vaultId: _v));
      expect(h.events.length, 2);

      await s.state.wipeVault(StateWipeRequest(vaultId: _v));

      h = await s.history.getHistory(HistoryGetRequest(vaultId: _v));
      expect(h.events, isEmpty);
    });
  });
}

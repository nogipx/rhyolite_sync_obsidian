import 'package:convergent/convergent.dart';
import 'package:rpc_data/rpc_data.dart';
import 'package:rpc_notify/rpc_notify.dart';
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
}) =>
    StatePutItem(
      fileId: fileId,
      encryptedState: state,
      blobRef: 'sha-1',
      hlcPacked: hlc.pack(),
      tombstone: false,
      contextPacked: CausalContext.from(context).pack(),
    );

/// Records every published topic so we can assert keyspace isolation.
class _FakeNotify implements INotifyRepository {
  final List<String> topics = [];

  @override
  void publish(String topic, Map<String, dynamic> payload) =>
      topics.add(topic);

  @override
  void publishTo(String clientId, String topic, Map<String, dynamic> payload) =>
      topics.add(topic);

  @override
  Stream<NotifyEvent> subscribe(String clientId, String topic) =>
      const Stream.empty();

  @override
  void unsubscribe(String clientId, String topic) {}

  @override
  List<String> activeTopics() => const [];

  @override
  int subscriberCount(String topic) => 0;

  @override
  Future<void> dispose() async {}
}

Future<List<dynamic>> _records(dynamic client, String collection) async {
  try {
    return await client.listAllRecords(collection: collection) as List;
  } catch (_) {
    // A never-created collection is equivalent to empty.
    return const [];
  }
}

void main() {
  group('StateSyncResponder — namespace isolation', () {
    test('config namespace and notes share a vaultId but never collide',
        () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);

      // Same client, same vaultId, two keyspaces.
      final notes = StateSyncResponder(client: env.client);
      final cfg = StateSyncResponder(
        client: env.client,
        namespace: 'config',
        historyEnabled: false,
      );

      await notes.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1', state: 'NOTE', hlc: _hlc(100, 'A'))],
      ));
      await cfg.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('app.json', state: 'CFG', hlc: _hlc(100, 'A'))],
      ));

      final notesPull =
          await notes.getStates(StateGetRequest(vaultId: _v, sinceCursor: 0));
      final cfgPull =
          await cfg.getStates(StateGetRequest(vaultId: _v, sinceCursor: 0));

      // Neither keyspace sees the other's records.
      expect(notesPull.records.map((r) => r.fileId).toList(), ['f1']);
      expect(cfgPull.records.map((r) => r.fileId).toList(), ['app.json']);

      // Cursors are independent — both keyspaces are at seq 1.
      expect(notesPull.cursor, 1);
      expect(cfgPull.cursor, 1);

      // Records physically land in distinct collections.
      expect((await _records(env.client, '${_v}_file_state')).length, 1);
      expect((await _records(env.client, '${_v}_config_file_state')).length, 1);
    });

    test('config keyspace advances its own cursor independently', () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);

      final notes = StateSyncResponder(client: env.client);
      final cfg = StateSyncResponder(
        client: env.client,
        namespace: 'config',
        historyEnabled: false,
      );

      // Three notes writes, one config write.
      for (var i = 0; i < 3; i++) {
        await notes.putStates(StatePutRequest(
          vaultId: _v,
          items: [_put('f$i', hlc: _hlc(100 + i, 'A'))],
        ));
      }
      final cfgResp = await cfg.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('app.json', hlc: _hlc(500, 'A'))],
      ));

      expect(cfgResp.cursor, 1, reason: 'config seq is not polluted by notes');
      final notesPull =
          await notes.getStates(StateGetRequest(vaultId: _v, sinceCursor: 0));
      expect(notesPull.cursor, 3);
    });
  });

  group('StateSyncResponder — optional history', () {
    test('historyEnabled:false writes no history records', () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);

      final cfg = StateSyncResponder(
        client: env.client,
        namespace: 'config',
        historyEnabled: false,
      );
      await cfg.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('app.json', hlc: _hlc(100, 'A'))],
      ));

      expect(await _records(env.client, '${_v}_config_history'), isEmpty);
    });

    test('default responder (history on) still writes history', () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);

      final notes = StateSyncResponder(client: env.client);
      await notes.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1', hlc: _hlc(100, 'A'))],
      ));

      expect(await _records(env.client, '${_v}_history'), isNotEmpty);
    });
  });

  group('StateSyncResponder — notify topic qualification', () {
    test('config keyspace publishes to vault:<id>_config', () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final fake = _FakeNotify();

      final cfg = StateSyncResponder(
        client: env.client,
        namespace: 'config',
        historyEnabled: false,
        notifyRepository: fake,
      );
      await cfg.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('app.json', hlc: _hlc(100, 'A'))],
      ));

      expect(fake.topics, contains('vault:${_v}_config'));
      expect(fake.topics, isNot(contains('vault:$_v')));
    });

    test('notes keyspace still publishes to vault:<id>', () async {
      final env = await DataServiceFactory.inMemory();
      addTearDown(env.dispose);
      final fake = _FakeNotify();

      final notes =
          StateSyncResponder(client: env.client, notifyRepository: fake);
      await notes.putStates(StatePutRequest(
        vaultId: _v,
        items: [_put('f1', hlc: _hlc(100, 'A'))],
      ));

      expect(fake.topics, contains('vault:$_v'));
    });
  });
}

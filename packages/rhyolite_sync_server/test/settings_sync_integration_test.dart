import 'dart:convert';
import 'dart:typed_data';

import 'package:rpc_data/rpc_data.dart';
import 'package:rhyolite_sync/rhyolite_sync.dart';
import 'package:rhyolite_sync_server/src/state_sync_responder.dart';
import 'package:test/test.dart';

// A real vaultId is always a UUID (validated by VaultConfig); the fileId
// derivation uuid.v5(vaultId, resourceId) requires it to be a valid UUID.
const _vault = '00000000-0000-4000-8000-000000000001';

/// Identity cipher — the server stores opaque bytes; encryption is orthogonal
/// to the convergence logic under test.
class _NoCipher implements IVaultCipher {
  @override
  Future<Uint8List> encrypt(Uint8List plaintext) async => plaintext;
  @override
  Future<Uint8List> decrypt(Uint8List ciphertext) async => ciphertext;
}

Uint8List jb(Object? value) =>
    Uint8List.fromList(utf8.encode(jsonEncode(value)));

Object? renderJson(SettingsSync s, String id) {
  final bytes = s.renderResource(id);
  return bytes == null ? null : jsonDecode(utf8.decode(bytes));
}

void main() {
  late InMemoryDataServiceEnvironment serverEnv;
  late StateSyncResponder server;

  // Wire a SettingsSync with its own local store against the shared server.
  Future<SettingsSync> device(
    Map<String, SettingsCrdtKind> registry,
  ) async {
    final env = await DataServiceFactory.inMemory();
    addTearDown(env.dispose);
    final store = SettingsStore(client: env.client, vaultId: _vault);
    return SettingsSync(
      remote: server,
      store: store,
      cipher: _NoCipher(),
      vaultId: _vault,
      kindOf: (id) => registry[id],
    );
  }

  setUp(() async {
    serverEnv = await DataServiceFactory.inMemory();
    server = StateSyncResponder(
      client: serverEnv.client,
      namespace: 'config',
      historyEnabled: false,
    );
  });

  tearDown(() async {
    await serverEnv.dispose();
  });

  const reg = {'appearance.json': SettingsCrdtKind.fieldMap};

  test('a push is visible to another device on pull', () async {
    final a = await device(reg);
    final b = await device(reg);
    await a.start();
    await b.start();

    await a.applyLocalChange('appearance.json', jb({'theme': 'dark'}));

    final changed = await b.pull();
    expect(changed, contains('appearance.json'));
    expect(renderJson(b, 'appearance.json'), {'theme': 'dark'});
  });

  test('concurrent edits to different keys converge on both devices',
      () async {
    final a = await device(reg);
    final b = await device(reg);
    await a.start();
    await b.start();

    // Common base.
    await a.applyLocalChange('appearance.json', jb({'theme': 'dark'}));
    await b.pull();

    // Concurrent: A adds fontSize, B adds accent.
    await a.applyLocalChange(
        'appearance.json', jb({'theme': 'dark', 'fontSize': 14}));
    await b.applyLocalChange(
        'appearance.json', jb({'theme': 'dark', 'accent': 'red'}));

    // Let it settle (pull rounds propagate the write-back compaction).
    for (var i = 0; i < 3; i++) {
      await a.pull();
      await b.pull();
    }

    const expected = {'theme': 'dark', 'fontSize': 14, 'accent': 'red'};
    expect(renderJson(a, 'appearance.json'), expected);
    expect(renderJson(b, 'appearance.json'), expected);
  });

  test('concurrent edit to the SAME key resolves identically (LWW)',
      () async {
    final a = await device(reg);
    final b = await device(reg);
    await a.start();
    await b.start();

    await a.applyLocalChange('appearance.json', jb({'theme': 'dark'}));
    await b.pull();

    await a.applyLocalChange('appearance.json', jb({'theme': 'light'}));
    await b.applyLocalChange('appearance.json', jb({'theme': 'solarized'}));

    for (var i = 0; i < 3; i++) {
      await a.pull();
      await b.pull();
    }

    final ra = renderJson(a, 'appearance.json');
    final rb = renderJson(b, 'appearance.json');
    expect(ra, rb, reason: 'both devices must converge to the same value');
  });

  test('OrSet plugin list: concurrent enables both survive', () async {
    const r = {'community-plugins': SettingsCrdtKind.orSet};
    final a = await device(r);
    final b = await device(r);
    await a.start();
    await b.start();

    await a.applyLocalChange('community-plugins', jb(['dataview']));
    await b.pull();

    await a.applyLocalChange('community-plugins', jb(['dataview', 'templater']));
    await b.applyLocalChange('community-plugins', jb(['dataview', 'calendar']));

    for (var i = 0; i < 3; i++) {
      await a.pull();
      await b.pull();
    }

    expect(renderJson(a, 'community-plugins'),
        ['calendar', 'dataview', 'templater']);
    expect(renderJson(b, 'community-plugins'),
        ['calendar', 'dataview', 'templater']);
  });

  test('state survives a restart (reload from store)', () async {
    final env = await DataServiceFactory.inMemory();
    addTearDown(env.dispose);

    final a = await device(reg);
    await a.start();
    await a.applyLocalChange('appearance.json', jb({'theme': 'dark'}));

    // Fresh SettingsSync over the SAME server, simulating another device that
    // pulls the pushed state.
    final b = await device(reg);
    await b.start();
    expect(renderJson(b, 'appearance.json'), {'theme': 'dark'});
  });

  test('source signature is recorded (incl. on no-op) and persists a restart',
      () async {
    final env = await DataServiceFactory.inMemory();
    addTearDown(env.dispose);
    SettingsSync make() => SettingsSync(
          remote: server,
          store: SettingsStore(client: env.client, vaultId: _vault),
          cipher: _NoCipher(),
          vaultId: _vault,
          kindOf: (id) => reg[id],
        );

    final a = make();
    await a.start();
    await a.applyLocalChange(
      'appearance.json',
      jb({'theme': 'dark'}),
      sourceSig: 'sig-1',
    );
    expect(a.sourceSigOf('appearance.json'), 'sig-1');

    // Same bytes, new signature, no CRDT delta — the signature still updates so
    // the next scan can skip this version.
    await a.applyLocalChange(
      'appearance.json',
      jb({'theme': 'dark'}),
      sourceSig: 'sig-2',
    );
    expect(a.sourceSigOf('appearance.json'), 'sig-2');

    // Restart: a fresh SettingsSync over the SAME store reloads the signature.
    final b = make();
    await b.start();
    expect(b.sourceSigOf('appearance.json'), 'sig-2');
  });

  test('restoreFromServer discards local state and re-downloads everything',
      () async {
    final a = await device(reg);
    final b = await device(reg);
    await a.start();
    await b.start();

    await a.applyLocalChange('appearance.json', jb({'theme': 'dark'}));
    await b.pull();
    expect(renderJson(b, 'appearance.json'), {'theme': 'dark'});

    // b is already in sync (cursor advanced past the record). Restore must
    // still re-materialise the resource from the server from scratch — it
    // wipes local state + resets the cursor, so the resource comes back as
    // "changed" and renders the server value.
    final changed = await b.restoreFromServer();
    expect(changed, contains('appearance.json'));
    expect(renderJson(b, 'appearance.json'), {'theme': 'dark'});
  });

  test('wipeServerAndLocal + re-push makes this device authoritative',
      () async {
    final a = await device(reg);
    final b = await device(reg);
    await a.start();
    await b.start();

    // Common history with an extra field both devices have seen.
    await a.applyLocalChange(
        'appearance.json', jb({'theme': 'dark', 'accent': 'red'}));
    await b.pull();

    // a resets: wipe the server keyspace + local store, then re-seed with a
    // clean value (the platform layer would re-scan disk; here we mimic it).
    await a.wipeServerAndLocal();
    await a.applyLocalChange('appearance.json', jb({'theme': 'light'}));

    // b downloads from server → sees ONLY a's post-reset value, with the old
    // 'accent' field gone (the pre-reset record was wiped).
    await b.restoreFromServer();
    expect(renderJson(b, 'appearance.json'), {'theme': 'light'});
  });

  test('orphan resources (no longer in the registry) are purged on start',
      () async {
    final env = await DataServiceFactory.inMemory();
    addTearDown(env.dispose);

    // Build A knows plugin code; it syncs appearance.json + a main.js.
    final a = SettingsSync(
      remote: server,
      store: SettingsStore(client: env.client, vaultId: _vault),
      cipher: _NoCipher(),
      vaultId: _vault,
      kindOf: (id) => const {
        'appearance.json': SettingsCrdtKind.fieldMap,
        'plugins/x/main.js': SettingsCrdtKind.wholeFile,
      }[id],
    );
    await a.start();
    await a.applyLocalChange('appearance.json', jb({'theme': 'dark'}));
    await a.applyLocalChange(
      'plugins/x/main.js',
      Uint8List.fromList([1, 2, 3]),
    );

    // Build B no longer recognises plugin code: the orphan row must be dropped
    // from the store on start, while live resources stay.
    final store = SettingsStore(client: env.client, vaultId: _vault);
    final b = SettingsSync(
      remote: server,
      store: store,
      cipher: _NoCipher(),
      vaultId: _vault,
      kindOf: (id) => reg[id],
    );
    await b.start();

    expect(store.resourceIds, contains('appearance.json'));
    expect(store.resourceIds, isNot(contains('plugins/x/main.js')));
    expect(renderJson(b, 'appearance.json'), {'theme': 'dark'});
  });
}

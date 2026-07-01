import 'package:postgres/postgres.dart';
import 'package:rpc_dart/rpc_dart.dart';
import 'package:rpc_dart_framework/rpc_dart_framework.dart';
import 'package:rpc_data_postgres/rpc_data_postgres.dart';
import 'package:rpc_notify/rpc_notify.dart';
import 'package:rpc_notify_postgres/rpc_notify_postgres.dart';

class PostgresModule extends RpcModule {
  @override
  String get name => 'PostgresModule';

  late Endpoint _endpoint;
  late String _schema;
  late SslMode _sslMode;
  Pool? _pool;
  PostgresNotifyRepository? _notify;

  @override
  void configureWithEnv(RpcContainer container, RpcEnvConfig env) {
    _endpoint = Endpoint(
      host: env['PG_HOST'] ?? 'localhost',
      port: env.getInt('PG_PORT') ?? 5432,
      database: env['PG_DATABASE'] ?? 'rhyolite',
      username: env['PG_USER'] ?? 'postgres',
      password: env['PG_PASSWORD'],
    );
    _schema = env['PG_SCHEMA'] ?? 'sync';
    _sslMode = _parseSslMode(env['PG_SSL_MODE']);
  }

  /// Maps `PG_SSL_MODE` to a [SslMode]. Defaults to `disable` to keep
  /// local/dev and in-cluster trusted networks working out of the box;
  /// set `require` or `verify-full` when talking to a managed Postgres
  /// over an untrusted link.
  static SslMode _parseSslMode(String? raw) {
    switch (raw?.trim().toLowerCase()) {
      case 'require':
        return SslMode.require;
      case 'verify-full':
      case 'verify_full':
      case 'verifyfull':
        return SslMode.verifyFull;
      case null:
      case '':
      case 'disable':
        return SslMode.disable;
      default:
        return SslMode.disable;
    }
  }

  @override
  Future<void> onStart(RpcContainer container) async {
    _pool = Pool.withEndpoints(
      [_endpoint],
      settings: PoolSettings(
        maxConnectionCount: 1,
        sslMode: _sslMode,
        maxConnectionAge: Duration(minutes: 9),
      ),
    );

    final adapter = await PostgresDataStorageAdapter.withPool(
      _pool!,
      schema: _schema,
      enableChangeJournal: false,
    );
    container.registerSingleton<IDataClient>(
      IDataClient.repository(
        repository: PostgresDataRepository(storage: adapter),
      ),
    );

    _notify = await PostgresNotifyRepository.connect(
      endpoint: _endpoint,
      settings: ConnectionSettings(sslMode: _sslMode),
    );
    container.registerSingleton<INotifyRepository>(_notify!);
  }

  @override
  Future<void> onStop() async {
    await _notify?.dispose();
    await _pool?.close();
  }

  @override
  Future<RpcHealthStatus?> checkHealth() async {
    try {
      await _pool?.execute('SELECT 1');
      return RpcHealthStatus.healthy(component: name, message: 'ok');
    } catch (e) {
      return RpcHealthStatus.unhealthy(
        component: name,
        message: 'ping failed: $e',
      );
    }
  }
}

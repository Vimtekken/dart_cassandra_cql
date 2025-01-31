part of dart_cassandra_cql.client;

class Client {
  final ConnectionPool connectionPool;
  final Map<String, PreparedResultMessage?> preparedQueries =
      Map<String, PreparedResultMessage?>();

  /// Create a new client and a [SimpleConnectionPool] to the supplied [hosts] optionally using
  /// the supplied [poolConfig]. If [poolConfig] is not specified, a default configuration will be used instead.
  /// If a [defaultKeyspace] is provided, it will be auto selected during the handshake phase of each pool connection
  factory Client.fromHostList(List<String> hosts,
      {String? defaultKeyspace, PoolConfiguration? poolConfig}) {
    final connectionPool = SimpleConnectionPool.fromHostList(
        hosts, poolConfig == null ? PoolConfiguration() : poolConfig,
        defaultKeyspace: defaultKeyspace);
    return new Client.withPool(connectionPool,
        defaultKeyspace: defaultKeyspace);
  }

  ///Create a new client with an already setup [connectionPool]. If a [defaultKeyspace]
  ///is provided, it will be auto-selected during the handshake phase of each pool connection.
  Client.withPool(this.connectionPool, {String? defaultKeyspace});

  /// Execute a [Query] or [BatchQuery] and return back a [Future<ResultMessage>]. Depending on
  /// the query type the [ResultMessage] will be an instance of [RowsResultMessage], [VoidResultMessage],
  /// [SetKeyspaceResultMessage] or [SchemaChangeResultMessage]. The optional [pageSize] and [pagingState]
  /// params may be supplied to enable pagination when performing single queries.
  Future<ResultMessage?> execute(QueryInterface query,
      {int? pageSize: null, Uint8List? pagingState: null}) {
    return query is BatchQuery
        ? _executeBatch(query)
        : _executeSingle(query as Query,
            pageSize: pageSize, pagingState: pagingState);
  }

  /// Execute a select query and return back a [Iterable] of [Map<String, Object?>] with the
  /// result rows.
  Future<Iterable<Map<String, Object?>>?> query(Query query) async {
    // Run query and return back
    return (await _executeSingle(query))?.rows; // @note XXX
  }

  /// Lazily execute a select query and return back a [Stream] object which emits one [Map<String, Object]
  /// event per result row. The client uses cassandra's pagination API to load additional result pages on
  /// demand. The result page size is controlled by the [pageSize] parameter (defaults to 100 rows).
  Stream<Map<String, Object?>> stream(Query query, {int pageSize: 100}) {
    return ResultStream(_executeSingle, query, pageSize).stream;
  }

  /// Terminate any opened connections and perform a clean shutdown. If the [drain] flag is set to true,
  /// all pool connections will be drained prior to being disconnected and a [Future] will be returned
  /// that will complete when all connections are drained. If [drain] is false then the returned [Future]
  /// will be already completed.
  Future shutdown(
      {bool drain: true, Duration drainTimeout: const Duration(seconds: 5)}) {
    return connectionPool.disconnect(drain: drain, drainTimeout: drainTimeout);
  }

  /// Prepare the given query and return back a [Future] with a [PreparedResultMessage]
  Future<PreparedResultMessage?>? _prepare(Query query) async {
    // If the query is preparing/already prepared, return its future
    if (preparedQueries.containsKey(query.query)) {
      clientLogger.fine('query already prepared');
      return preparedQueries[query.query];
    }

    // Queue for preparation and return back a future
    clientLogger.fine('Getting connection for query preparation');
    final Connection connection = await connectionPool.getConnection();
    clientLogger.fine('Prepare query');
    final PreparedResultMessage? resultMessage =
        await connection.prepare(query);
    preparedQueries[query.query] = resultMessage;
    return resultMessage;
  }

  Future<ResultMessage?> _executeUnprepared(Query query,
      {int? pageSize: null, Uint8List? pagingState: null}) async {
    try {
      final Connection connection = await connectionPool.getConnection();
      final ResultMessage? result = await connection.execute(query,
          pageSize: pageSize, pagingState: pagingState);
      return result;
    } on ConnectionLostException {
      clientLogger.info('_executeUnprepared : Connection Lost Exception');
      _executeUnprepared(query, pageSize: pageSize, pagingState: pagingState);
    } on StreamReservationException {
      clientLogger.info('_executeUnprepared : Stream Reservation Exception');
      _executeUnprepared(query, pageSize: pageSize, pagingState: pagingState);
    }
  }

  Future<ResultMessage?> _prepareAndExecute(Query query,
      {int? pageSize: null, Uint8List? pagingState: null}) async {
    final PreparedResultMessage? preparedResult = await _prepare(query);

    try {
      final Connection conn = await connectionPool.getConnectionToHost(
          preparedResult!.host, preparedResult.port);
      final ResultMessage? result = await conn.execute(query,
          preparedResult: preparedResult,
          pageSize: pageSize,
          pagingState: pagingState);
      return result;
    } on ConnectionLostException {
      clientLogger.info('_executeUnprepared : Connection Lost Exception');
      return _prepareAndExecute(query,
          pageSize: pageSize, pagingState: pagingState);
    } on StreamReservationException {
      clientLogger.info('_executeUnprepared : Stream Reservation Exception');
      return _prepareAndExecute(query,
          pageSize: pageSize, pagingState: pagingState);
    } on NoHealthyConnectionsException {
      clientLogger
          .info('_executeUnprepared : No Healthy Connections Exception');
      preparedQueries.remove(query.query);
      return _prepareAndExecute(query,
          pageSize: pageSize, pagingState: pagingState);
    }
  }

  /// Execute a single [query] with optional [pageSize] and [pagingState] data
  /// and return back a [Future<ResultMessage>]
  Future<ResultMessage?> _executeSingle(Query query,
      {int? pageSize: null, Uint8List? pagingState: null}) async {
    // If this is a normal query, pick the next available pool connection and execute it
    if (!query.prepared) {
      return _executeUnprepared(query,
          pageSize: pageSize, pagingState: pagingState);
    } else {
      return _prepareAndExecute(query,
          pageSize: pageSize, pagingState: pagingState);
    }
  }

  /// Execute a batch [query] and return back a [Future<ResultMessage>]
  Future<ResultMessage?> _executeBatch(BatchQuery query) async {
    return (await connectionPool.getConnection()).executeBatch(query);
  }
}

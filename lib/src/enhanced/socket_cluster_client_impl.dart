part of enhanced_croupier;

class _SocketClusterClientImpl 
    with _ConnectionManager 
    implements SocketClusterClient {

  // Interface properties
  @override
  String get hostname => _hostname;
  final String _hostname;

  @override
  bool get secure => _secure;
  final bool _secure;

  @override
  int get port => _port;
  final int _port;

  @override
  String get path => _path;
  final String _path;

  @override
  Map<String, String>? get query => _query;
  final Map<String, String>? _query;

  @override
  String get url => _url;
  final String _url;

  @override
  int connectTimeout;

  @override
  int ackTimeout;

  @override
  ReconnectPolicy reconnectPolicy;

  @override
  List<String>? protocols;

  @override
  Map<String, dynamic>? headers;

  @override
  String? get id => _id;
  String? _id;

  @override
  String? get clientId => _clientId;
  final String? _clientId;

  @override
  ConnectionState get state => _state;
  ConnectionState _state = ConnectionState.closed;

  @override
  AuthenticationState get authState => _authState;
  AuthenticationState _authState = AuthenticationState.unauthenticated;

  @override
  String? get authToken => _authToken;
  String? _authToken;

  @override
  set authToken(String? token) {
    _authToken = token;
  }

  @override
  int? get closeCode => _websocket?.closeCode;

  @override
  String? get closeReason => _websocket?.closeReason;

  // Core components
  _NativeWebSocket? _websocket;
  final _MessageQueue _messageQueue = _MessageQueue();
  _MultiplexedStream? _eventsMultiplex;
  _MultiplexedStream? _receiveMultiplex;
  _MultiplexedStream? _invokeMultiplex;

  // Internal state
  final Map<int, Completer<dynamic>> _expectedResponses = {};
  int _cid = 0;
  bool _isDisposed = false;

  _SocketClusterClientImpl({
    required String hostname,
    bool secure = false,
    int? port,
    String path = '/socketcluster/',
    Map<String, String>? query,
    this.connectTimeout = 20000,
    this.ackTimeout = 10000,
    this.reconnectPolicy = const ReconnectPolicy(),
    this.protocols,
    this.headers,
    String? authToken,
    String? clientId,
  })  : _hostname = hostname,
        _secure = secure,
        _port = port ?? (secure ? 443 : 80),
        _path = path,
        _query = query,
        _url = Uri(
          scheme: secure ? 'wss' : 'ws',
          host: hostname,
          port: port ?? (secure ? 443 : 80),
          path: path,
          queryParameters: query,
        ).toString(),
        _authToken = authToken,
        _clientId = clientId {
    
    initializeConnectionManager();
    _initializeStreams();
  }

  void _initializeStreams() {
    _eventsMultiplex = _MultiplexedStream(allowGlobalChannel: false);
    _receiveMultiplex = _MultiplexedStream();
    _invokeMultiplex = _MultiplexedStream();
  }

  @override
  bool get isConnected => _websocket?.isConnected ?? false;

  @override
  bool get isConnecting => _websocket?.isConnecting ?? false;

  @override
  Future connect() async {
    if (_isDisposed) throw StateError('Client has been disposed');
    
    if (isConnected || isConnecting) return;
    
    handleManualConnect();
    await performConnect();
  }

  @override
  Future<void> performConnect() async {
    _state = ConnectionState.connecting;
    _emit(SCEvent.connecting);

    try {
      _websocket = _NativeWebSocket(
        url: url,
        protocols: protocols,
        headers: headers,
        connectTimeout: connectTimeout,
      );

      _websocket!.onMessage.listen(_onMessage);
      _websocket!.onError.listen(_onError);
      _websocket!.onClose.listen((_) => _onClose());

      await _websocket!.connect();
      await _performHandshake();
      
    } catch (e) {
      _state = ConnectionState.closed;
      handleConnectionLost();
      rethrow;
    }
  }

  Future<void> _performHandshake() async {
    try {
      final response = await invoke(
        '#handshake',
        {'authToken': _authToken},
        Options(force: true, noTimeout: true),
      );

      _id = response['id'];
      _authState = response['isAuthenticated'] == true
          ? AuthenticationState.authenticated
          : AuthenticationState.unauthenticated;

      if (_authState == AuthenticationState.unauthenticated) {
        _authToken = null;
      }

      _state = ConnectionState.open;
      _emit(SCEvent.ready);
      _flushMessageQueue();
      
    } catch (e) {
      await close(1011, 'Handshake failed');
      handleConnectionLost();
      rethrow;
    }
  }

  void _onMessage(String message) {
    _emit('message', message);

    if (message.isEmpty) {
      _websocket?.send(''); // Ping response
      return;
    }

    if (message.startsWith('[')) {
      // Handle message array
      try {
        final List messages = jsonDecode(message);
        for (final msg in messages) {
          _processMessage(msg);
        }
      } catch (e) {
        _emit('raw', message);
      }
      return;
    }

    try {
      final packet = jsonDecode(message);
      _processMessage(packet);
    } catch (e) {
      _emit('raw', message);
    }
  }

  void _processMessage(dynamic packet) {
    if (packet is! Map<String, dynamic>) return;

    if (packet['event'] != null) {
      if (packet['cid'] != null) {
        _emitInvoke(packet['event'], packet);
      } else {
        _emitReceive(packet['event'], packet);
      }
      return;
    }

    if (packet['rid'] != null) {
      final rid = packet['rid'] as int;
      if (_expectedResponses.containsKey(rid)) {
        if (packet['error'] != null) {
          _expectedResponses[rid]!.completeError(
            SocketMessageError(
              packet['error'],
              name: packet['error']['name'],
              message: packet['error']['message'],
            ),
          );
        } else {
          _expectedResponses[rid]!.complete(packet['data']);
        }
        _expectedResponses.remove(rid);
      }
    }
  }

  void _onError(dynamic error) {
    // Handle connection errors
  }

  void _onClose() {
    _state = ConnectionState.closed;
    _emit(SCEvent.disconnect);
    _clearExpectedResponses();
    handleConnectionLost();
  }

  @override
  void onNetworkStateChanged(bool hasConnection) {
    // Network state changed, handled by connection manager
  }

  void _flushMessageQueue() {
    final messages = _messageQueue.dequeueAll();
    for (final message in messages) {
      _sendMessage(message);
    }
  }

  void _sendMessage(_QueuedMessage queuedMessage) {
    if (!isConnected) {
      _messageQueue.enqueue(queuedMessage);
      return;
    }

    final packet = queuedMessage.toPacket(++_cid);
    _websocket?.send(jsonEncode(packet));
  }

  @override
  Future<dynamic> invoke(
    String procedure, [
    dynamic data,
    Options options = const Options(expectResponse: true),
  ]) async {
    return await _processOutboundEvent(
      procedure,
      data,
      options.copyWith(expectResponse: true),
    );
  }

  @override
  void transmit(
    String event, [
    dynamic data,
    Options options = const Options(),
  ]) {
    _processOutboundEvent(event, data, options);
  }

  Future<dynamic> _processOutboundEvent(
    String? event,
    dynamic data, [
    Options options = const Options(),
    int? rid,
  ]) async {
    data ??= {};

    final queuedMessage = _QueuedMessage(
      event: event,
      data: data,
      options: options,
      rid: rid,
      priority: options.force,
    );

    Future? responseFuture;

    if (options.expectResponse) {
      final completer = Completer();
      _expectedResponses[++_cid] = completer;
      responseFuture = completer.future;
      
      if (!options.noTimeout) {
        responseFuture = responseFuture.timeout(
          Duration(milliseconds: options.ackTimeout ?? ackTimeout),
        );
      }
    }

    if (isConnected || options.force) {
      _sendMessage(queuedMessage);
    } else {
      _messageQueue.enqueue(queuedMessage);
    }

    return responseFuture;
  }

  @override
  void send(String data) {
    if (isConnected) {
      _websocket?.send(data);
    }
  }

  @override
  StreamSubscription on(SCEvent event, Function? callback) {
    return _eventsMultiplex!.subscribeToChannel(event.name, (event) async {
      try {
        callback?.call();
      } catch (e) {
        // Handle callback error
      }
    });
  }

  @override
  StreamSubscription onRaw(Function(String data)? callback) {
    return _eventsMultiplex!.subscribeToChannel('raw', (event) async {
      try {
        callback?.call(event.event.toString());
      } catch (e) {
        // Handle callback error
      }
    });
  }

  @override
  StreamSubscription onMessage(Function(String data)? callback) {
    return _eventsMultiplex!.subscribeToChannel('message', (event) async {
      try {
        callback?.call(event.event.toString());
      } catch (e) {
        // Handle callback error
      }
    });
  }

  @override
  StreamSubscription registerReceiver(
    String name,
    Function(dynamic data)? callback,
  ) {
    return _receiveMultiplex!.subscribeToChannel(name, (event) async {
      try {
        callback?.call(event.event);
      } catch (e) {
        // Handle callback error
      }
    });
  }

  @override
  StreamSubscription registerProcedure(
    String name,
    Function(dynamic data)? callback,
  ) {
    return _invokeMultiplex!.subscribeToChannel(name, (event) async {
      final rid = event.event['cid'] as int;
      try {
        final response = await callback?.call(event.event);
        await _processOutboundEvent(null, response, const Options(), rid);
      } catch (e) {
        // Handle procedure error
      }
    });
  }

  @override
  Future<void> close([int? code, String? reason]) async {
    if (_isDisposed) return;
    
    handleManualDisconnect();
    _state = ConnectionState.closed;
    _emit(SCEvent.close);
    _clearExpectedResponses();
    
    await _websocket?.close(code, reason);
    _cleanup();
  }

  @override
  void performDisconnect() {
    close();
  }

  void _emit(dynamic event, [dynamic data]) {
    if (event is SCEvent) {
      _eventsMultiplex?.publish(event.name, data);
    } else {
      _eventsMultiplex?.publish(event.toString(), data);
    }
  }

  void _emitReceive(String event, dynamic data) {
    _receiveMultiplex?.publish(event, data);
  }

  void _emitInvoke(String event, dynamic data) {
    _invokeMultiplex?.publish(event, data);
  }

  void _clearExpectedResponses() {
    for (final completer in _expectedResponses.values) {
      if (!completer.isCompleted) {
        completer.completeError(NetworkError('Connection closed'));
      }
    }
    _expectedResponses.clear();
  }

  void _cleanup() {
    _isDisposed = true;
    _messageQueue.clear();
    _websocket?.dispose();
    _eventsMultiplex?.close();
    _receiveMultiplex?.close();
    _invokeMultiplex?.close();
    disposeConnectionManager();
  }
}
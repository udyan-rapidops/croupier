part of enhanced_croupier;

class _NativeWebSocket with WidgetsBindingObserver {
  WebSocket? _socket;
  final String url;
  final List<String>? protocols;
  final Map<String, dynamic>? headers;
  final int connectTimeout;
  
  StreamController<String>? _messageController;
  StreamController<void>? _closeController;
  StreamController<dynamic>? _errorController;
  
  bool _isDisposed = false;
  Timer? _pingTimer;
  Timer? _pongTimer;
  DateTime? _lastPong;
  
  _NativeWebSocket({
    required this.url,
    this.protocols,
    this.headers,
    this.connectTimeout = 20000,
  }) {
    WidgetsBinding.instance.addObserver(this);
  }

  Stream<String> get onMessage => _messageController?.stream ?? const Stream.empty();
  Stream<void> get onClose => _closeController?.stream ?? const Stream.empty();
  Stream<dynamic> get onError => _errorController?.stream ?? const Stream.empty();

  bool get isConnected => _socket != null && _socket!.readyState == WebSocket.open;
  bool get isConnecting => _socket != null && _socket!.readyState == WebSocket.connecting;
  bool get isClosed => _socket == null || _socket!.readyState == WebSocket.closed;
  int? get closeCode => _socket?.closeCode;
  String? get closeReason => _socket?.closeReason;

  Future<void> connect() async {
    if (_isDisposed) return;
    
    await _cleanup();
    
    _messageController = StreamController<String>.broadcast();
    _closeController = StreamController<void>.broadcast();
    _errorController = StreamController<dynamic>.broadcast();

    try {
      _socket = await WebSocket.connect(
        url,
        protocols: protocols,
        headers: headers?.cast<String, dynamic>(),
      ).timeout(Duration(milliseconds: connectTimeout));

      _socket!.listen(
        _onMessage,
        onError: _onError,
        onDone: _onClose,
        cancelOnError: false,
      );

      _startHeartbeat();
    } catch (e) {
      _onError(e);
      rethrow;
    }
  }

  void send(String data) {
    if (isConnected) {
      try {
        _socket!.add(data);
      } catch (e) {
        _onError(e);
      }
    }
  }

  Future<void> close([int? code, String? reason]) async {
    _stopHeartbeat();
    
    if (_socket != null && !isClosed) {
      try {
        await _socket!.close(code ?? 1000, reason);
      } catch (e) {
        // Ignore close errors
      }
    }
    
    await _cleanup();
  }

  void _onMessage(dynamic data) {
    if (_isDisposed) return;
    
    _lastPong = DateTime.now();
    
    // Handle ping/pong
    if (data == '#2') {
      return; // Pong response
    }
    if (data == '#1') {
      send('#2'); // Send pong
      return;
    }
    
    _messageController?.add(data.toString());
  }

  void _onError(dynamic error) {
    if (_isDisposed) return;
    _errorController?.add(error);
  }

  void _onClose() {
    if (_isDisposed) return;
    _stopHeartbeat();
    _closeController?.add(null);
  }

  void _startHeartbeat() {
    _stopHeartbeat();
    _lastPong = DateTime.now();
    
    _pingTimer = Timer.periodic(Duration(seconds: 25), (_) {
      if (isConnected) {
        send('#1'); // Send ping
        
        _pongTimer?.cancel();
        _pongTimer = Timer(Duration(seconds: 10), () {
          if (_lastPong == null || 
              DateTime.now().difference(_lastPong!).inSeconds > 35) {
            // No pong received, connection might be dead
            close(4001, 'Ping timeout');
          }
        });
      }
    });
  }

  void _stopHeartbeat() {
    _pingTimer?.cancel();
    _pongTimer?.cancel();
    _pingTimer = null;
    _pongTimer = null;
  }

  Future<void> _cleanup() async {
    _socket = null;
    await _messageController?.close();
    await _closeController?.close();
    await _errorController?.close();
    _messageController = null;
    _closeController = null;
    _errorController = null;
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    switch (state) {
      case AppLifecycleState.paused:
      case AppLifecycleState.inactive:
        _stopHeartbeat();
        break;
      case AppLifecycleState.resumed:
        if (isConnected) {
          _startHeartbeat();
        }
        break;
      case AppLifecycleState.detached:
        dispose();
        break;
      case AppLifecycleState.hidden:
        break;
    }
  }

  void dispose() {
    if (_isDisposed) return;
    _isDisposed = true;
    
    WidgetsBinding.instance.removeObserver(this);
    _stopHeartbeat();
    close();
    _cleanup();
  }
}
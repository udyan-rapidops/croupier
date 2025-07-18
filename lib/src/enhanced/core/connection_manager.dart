part of enhanced_croupier;

mixin _ConnectionManager {
  late StreamSubscription<List<ConnectivityResult>> _connectivitySubscription;
  List<ConnectivityResult> _currentConnectivity = [];
  Timer? _reconnectTimer;
  int _reconnectAttempts = 0;
  bool _isManualDisconnect = false;
  bool _isInitialized = false;
  
  ReconnectPolicy get reconnectPolicy;
  bool get isConnected;
  bool get isConnecting;
  Future<void> performConnect();
  void performDisconnect();
  void onNetworkStateChanged(bool hasConnection);

  void initializeConnectionManager() {
    if (_isInitialized) return;
    _isInitialized = true;
    
    _connectivitySubscription = Connectivity()
        .onConnectivityChanged
        .distinct()
        .listen(_handleConnectivityChange);
  }

  void _handleConnectivityChange(List<ConnectivityResult> results) {
    final hadConnection = _hasNetworkConnection(_currentConnectivity);
    final hasConnection = _hasNetworkConnection(results);
    
    _currentConnectivity = results;
    
    if (!hadConnection && hasConnection && !_isManualDisconnect) {
      // Network restored, attempt reconnection
      _scheduleReconnect(immediate: true);
    } else if (hadConnection && !hasConnection) {
      // Network lost
      _cancelReconnect();
    }
    
    onNetworkStateChanged(hasConnection);
  }

  bool _hasNetworkConnection(List<ConnectivityResult> results) {
    return results.isNotEmpty && !results.contains(ConnectivityResult.none);
  }

  Duration _getAdaptiveTimeout() {
    if (_currentConnectivity.contains(ConnectivityResult.wifi)) {
      return Duration(milliseconds: 10000); // WiFi: 10s
    } else if (_currentConnectivity.contains(ConnectivityResult.mobile)) {
      return Duration(milliseconds: 30000); // Mobile: 30s
    }
    return Duration(milliseconds: 20000); // Default: 20s
  }

  void _scheduleReconnect({bool immediate = false}) {
    if (_isManualDisconnect || !reconnectPolicy.autoReconnect) return;
    
    _cancelReconnect();
    
    final delay = immediate ? Duration.zero : _calculateReconnectDelay();
    
    _reconnectTimer = Timer(delay, () async {
      if (!_isManualDisconnect && !isConnected && !isConnecting) {
        try {
          await performConnect();
          _reconnectAttempts = 0; // Reset on successful connection
        } catch (e) {
          _reconnectAttempts++;
          if (_reconnectAttempts < 10) { // Max attempts
            _scheduleReconnect();
          }
        }
      }
    });
  }

  Duration _calculateReconnectDelay() {
    final baseDelay = reconnectPolicy.initialDelay;
    final randomness = reconnectPolicy.randomness * Random().nextDouble();
    final exponential = pow(reconnectPolicy.multiplier, _reconnectAttempts);
    
    final totalDelay = ((baseDelay + randomness) * exponential).round();
    return Duration(milliseconds: min(totalDelay, reconnectPolicy.maxDelay));
  }

  void _cancelReconnect() {
    _reconnectTimer?.cancel();
    _reconnectTimer = null;
  }

  void handleConnectionLost() {
    if (!_isManualDisconnect && reconnectPolicy.autoReconnect) {
      _scheduleReconnect();
    }
  }

  void handleManualDisconnect() {
    _isManualDisconnect = true;
    _cancelReconnect();
    _reconnectAttempts = 0;
  }

  void handleManualConnect() {
    _isManualDisconnect = false;
    _reconnectAttempts = 0;
  }

  void disposeConnectionManager() {
    if (_isInitialized) {
      _connectivitySubscription.cancel();
      _cancelReconnect();
      _isInitialized = false;
    }
  }
}
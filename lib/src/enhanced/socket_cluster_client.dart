part of enhanced_croupier;

abstract class SocketClusterClient {
  String get hostname;
  bool get secure;
  int get port;
  String get path;
  Map<String, String>? get query;
  String get url;
  int get connectTimeout;
  int get ackTimeout;
  ReconnectPolicy get reconnectPolicy;
  List<String>? get protocols;
  Map<String, dynamic>? get headers;
  String? get id;
  String? get clientId;
  ConnectionState get state;
  AuthenticationState get authState;
  String? get authToken;
  int? get closeCode;
  String? get closeReason;

  // Connection methods
  Future connect();
  Future<void> close([int? code, String? reason]);
  
  // Messaging methods
  void send(String data);
  void transmit(String event, [dynamic data, Options options = const Options()]);
  Future<dynamic> invoke(String procedure, [dynamic data, Options options = const Options(expectResponse: true)]);
  
  // Event handling methods
  StreamSubscription on(SCEvent event, Function? callback);
  StreamSubscription onRaw(Function(String data)? callback);
  StreamSubscription onMessage(Function(String data)? callback);
  StreamSubscription registerReceiver(String name, Function(dynamic data)? callback);
  StreamSubscription registerProcedure(String name, Function(dynamic data)? callback);

  // Auth methods
  set authToken(String? token);

  factory SocketClusterClient({
    required String hostname,
    bool secure = false,
    int? port,
    String path = '/socketcluster/',
    Map<String, String>? query,
    int connectTimeout = 20000,
    int ackTimeout = 10000,
    ReconnectPolicy reconnectPolicy = const ReconnectPolicy(),
    List<String>? protocols,
    Map<String, dynamic>? headers,
    String? authToken,
    String? clientId,
  }) =>
      _SocketClusterClientImpl(
        hostname: hostname,
        secure: secure,
        port: port,
        path: path,
        query: query,
        connectTimeout: connectTimeout,
        ackTimeout: ackTimeout,
        reconnectPolicy: reconnectPolicy,
        protocols: protocols,
        headers: headers,
        authToken: authToken,
        clientId: clientId,
      );
}
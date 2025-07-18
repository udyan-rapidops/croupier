// Enhanced Croupier - Drop-in replacement for the original
export 'croupier.dart';
// For backward compatibility, re-export with original names
import 'croupier.dart' as enhanced;

// Factory function that matches the original interface exactly
enhanced.SocketClusterClient SocketClusterClient({
  required String hostname,
  bool secure = false,
  int? port,
  String path = '/socketcluster/',
  Map<String, String>? query,
  int connectTimeout = 20000,
  int ackTimeout = 10000,
  enhanced.ReconnectPolicy reconnectPolicy = const enhanced.ReconnectPolicy(),
  List<String>? protocols,
  Map<String, dynamic>? headers,
  String? authToken,
  String? clientId,
}) =>
    enhanced.createSocketClusterClient(
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
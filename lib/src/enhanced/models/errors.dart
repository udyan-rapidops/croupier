part of enhanced_croupier;

class SocketMessageError implements Exception {
  final Map<String, dynamic> error;
  final String? name;
  final String? message;

  SocketMessageError(this.error, {this.name, this.message});

  @override
  String toString() => 'SocketMessageError: $message';
}

class ConnectTimeoutError implements Exception {
  final String message;
  ConnectTimeoutError(this.message);

  @override
  String toString() => 'ConnectTimeoutError: $message';
}

class NetworkError implements Exception {
  final String message;
  NetworkError(this.message);

  @override
  String toString() => 'NetworkError: $message';
}

class HandshakeError implements Exception {
  final String message;
  HandshakeError(this.message);

  @override
  String toString() => 'HandshakeError: $message';
}
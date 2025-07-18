part of enhanced_croupier;

enum ConnectionState {
  closed,
  connecting,
  open,
}

enum AuthenticationState {
  unauthenticated,
  authenticated,
}

enum SCEvent {
  connecting('connecting'),
  ready('ready'),
  disconnect('disconnect'),
  close('close'),
  connectionStateChange('connectionStateChange'),
  authStateChange('authStateChange');

  final String name;

  const SCEvent(this.name);
}
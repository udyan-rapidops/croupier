part of enhanced_croupier;

class _MultiplexedStream {
  final Map<String, StreamController<_StreamEvent>> _channels = {};
  final bool allowGlobalChannel;
  bool _isClosed = false;

  _MultiplexedStream({this.allowGlobalChannel = true});

  StreamSubscription<_StreamEvent> subscribeToChannel(
    String channelName,
    Function(dynamic event) callback,
  ) {
    if (_isClosed) {
      throw StateError('MultiplexedStream has been closed');
    }

    _channels[channelName] ??= StreamController<_StreamEvent>.broadcast();
    
    return _channels[channelName]!.stream.listen((event) async {
      try {
        await callback(event);
      } catch (e) {
        // Handle callback errors gracefully
      }
    });
  }

  void publish(String channelName, dynamic data) {
    if (_isClosed) return;

    final event = _StreamEvent(channelName, data);
    
    // Publish to specific channel
    _channels[channelName]?.add(event);
    
    // Publish to global channel if allowed
    if (allowGlobalChannel && channelName != '*') {
      _channels['*']?.add(event);
    }
  }

  void addToChannel(String channelName, dynamic data) {
    publish(channelName, data);
  }

  Future<void> close() async {
    if (_isClosed) return;
    _isClosed = true;

    for (final controller in _channels.values) {
      await controller.close();
    }
    _channels.clear();
  }

  bool get isClosed => _isClosed;
}

class _StreamEvent {
  final String channel;
  final dynamic event;

  _StreamEvent(this.channel, this.event);
}
part of enhanced_croupier;

class _MessageQueue {
  final Queue<_QueuedMessage> _queue = Queue();
  final Map<String, _QueuedMessage> _messageMap = {}; // For deduplication
  final int maxQueueSize;
  
  _MessageQueue({this.maxQueueSize = 1000});

  void enqueue(_QueuedMessage message) {
    // Deduplication check
    final key = '${message.event}_${message.data.hashCode}';
    if (_messageMap.containsKey(key)) {
      return; // Duplicate message, ignore
    }

    // Queue size management
    if (_queue.length >= maxQueueSize) {
      final removed = _queue.removeFirst();
      _messageMap.remove('${removed.event}_${removed.data.hashCode}');
    }

    _queue.add(message);
    _messageMap[key] = message;
  }

  List<_QueuedMessage> dequeueAll() {
    final messages = _queue.toList();
    _queue.clear();
    _messageMap.clear();
    return messages;
  }

  List<_QueuedMessage> dequeuePriority() {
    final priorityMessages = _queue.where((msg) => msg.priority).toList();
    _queue.removeWhere((msg) => msg.priority);
    
    for (final msg in priorityMessages) {
      final key = '${msg.event}_${msg.data.hashCode}';
      _messageMap.remove(key);
    }
    
    return priorityMessages;
  }

  bool get isEmpty => _queue.isEmpty;
  int get length => _queue.length;

  void clear() {
    _queue.clear();
    _messageMap.clear();
  }
}

class _QueuedMessage {
  final String? event;
  final dynamic data;
  final Options options;
  final int? rid;
  final bool priority;
  final DateTime timestamp;
  int retryCount = 0;

  _QueuedMessage({
    this.event,
    required this.data,
    this.options = const Options(),
    this.rid,
    this.priority = false,
  }) : timestamp = DateTime.now();

  Map<String, dynamic> toPacket(int cid) {
    return {
      'cid': options.expectResponse ? cid : null,
      'rid': rid,
      'event': event,
      'data': (data is Map && options.cloneData) ? Map.from(data) : data,
    };
  }

  bool get shouldRetry => retryCount < 3 && 
      DateTime.now().difference(timestamp).inMinutes < 5;
}
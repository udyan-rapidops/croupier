part of enhanced_croupier;

class Options {
  final bool expectResponse;
  final bool force;
  final bool noTimeout;
  final bool cloneData;
  final int? ackTimeout;

  const Options({
    this.expectResponse = false,
    this.force = false,
    this.noTimeout = false,
    this.cloneData = false,
    this.ackTimeout,
  });

  Options copyWith({
    bool? expectResponse,
    bool? force,
    bool? noTimeout,
    bool? cloneData,
    int? ackTimeout,
  }) {
    return Options(
      expectResponse: expectResponse ?? this.expectResponse,
      force: force ?? this.force,
      noTimeout: noTimeout ?? this.noTimeout,
      cloneData: cloneData ?? this.cloneData,
      ackTimeout: ackTimeout ?? this.ackTimeout,
    );
  }
}
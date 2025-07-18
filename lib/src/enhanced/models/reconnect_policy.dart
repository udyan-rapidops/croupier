part of enhanced_croupier;

class ReconnectPolicy {
  final bool autoReconnect;
  final int initialDelay;
  final int maxDelay;
  final double multiplier;
  final double randomness;

  const ReconnectPolicy({
    this.autoReconnect = true,
    this.initialDelay = 1000,
    this.maxDelay = 30000,
    this.multiplier = 1.5,
    this.randomness = 0.5,
  });

  ReconnectPolicy copyWith({
    bool? autoReconnect,
    int? initialDelay,
    int? maxDelay,
    double? multiplier,
    double? randomness,
  }) {
    return ReconnectPolicy(
      autoReconnect: autoReconnect ?? this.autoReconnect,
      initialDelay: initialDelay ?? this.initialDelay,
      maxDelay: maxDelay ?? this.maxDelay,
      multiplier: multiplier ?? this.multiplier,
      randomness: randomness ?? this.randomness,
    );
  }
}
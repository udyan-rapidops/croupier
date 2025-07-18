/// Enhanced SocketCluster client implementation for Dart and Flutter.
library enhanced_croupier;

import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:flutter/material.dart';

part 'models/connection_state.dart';
part 'models/options.dart';
part 'models/errors.dart';
part 'models/reconnect_policy.dart';
part 'core/native_websocket.dart';
part 'core/connection_manager.dart';
part 'core/message_queue.dart';
part 'core/multiplexed_stream.dart';
part 'socket_cluster_client.dart';
part 'socket_cluster_client_impl.dart';

// Main factory function - same interface as before
SocketClusterClient createSocketClusterClient({
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
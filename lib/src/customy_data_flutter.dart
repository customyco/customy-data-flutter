import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

typedef JsonMap = Map<String, dynamic>;
typedef BeforeSend = JsonMap? Function(JsonMap event);
typedef DataTransport = Future<DataResponse> Function(
  Uri url,
  Map<String, String> headers,
  List<int> body,
  Duration timeout,
);

final class DataResponse {
  const DataResponse(this.statusCode, this.body);

  final int statusCode;
  final List<int> body;
}

final class CustomyDataException implements Exception {
  const CustomyDataException(this.message, {this.statusCode, this.response});

  final String message;
  final int? statusCode;
  final Object? response;

  @override
  String toString() => 'CustomyDataException: $message';
}

final class CustomyDataFlutter {
  CustomyDataFlutter({
    required String collectUrl,
    required this.writeKey,
    DataTransport? transport,
    this.maxRetries = 3,
    this.retryBase = const Duration(milliseconds: 250),
    this.timeout = const Duration(seconds: 10),
    int maxBatchSize = 100,
    int maxQueueSize = 10000,
    Set<String> redactFields = const {},
    this.beforeSend,
    DateTime Function()? now,
    String Function()? idFactory,
  })  : collectUrl = collectUrl.replaceFirst(RegExp(r'/+$'), ''),
        transport = transport ?? _httpTransport,
        maxBatchSize = maxBatchSize.clamp(1, 1000),
        maxQueueSize = max(1, maxQueueSize),
        redactFields = Set.unmodifiable(redactFields),
        now = now ?? DateTime.now,
        idFactory = idFactory ?? _randomId {
    if (this.collectUrl.isEmpty || writeKey.isEmpty) {
      throw ArgumentError('collectUrl and writeKey are required');
    }
  }

  static const version = '0.1.1';
  static const conformanceContract = 'customy.customer-data-sdk.conformance.v1';
  static const _eventTypes = {
    'track',
    'identify',
    'group',
    'page',
    'screen',
    'alias',
  };
  static const _forbiddenTenantFields = {
    'tenantId',
    'organizationId',
    'projectId',
    'environmentId',
  };
  static const _retryableStatuses = {429, 500, 502, 503, 504};

  final String collectUrl;
  final String writeKey;
  final DataTransport transport;
  final int maxRetries;
  final Duration retryBase;
  final Duration timeout;
  final int maxBatchSize;
  final int maxQueueSize;
  final Set<String> redactFields;
  final BeforeSend? beforeSend;
  final DateTime Function() now;
  final String Function() idFactory;
  final List<JsonMap> _queue = [];
  int _inFlightCount = 0;
  bool _flushing = false;

  int get queueSize => _queue.length + _inFlightCount;

  JsonMap event(Map<String, Object?> input) {
    var normalized = _deepCopyMap(input);
    _rejectTenantFields(normalized);
    _validate(normalized);
    normalized.putIfAbsent('messageId', idFactory);
    normalized.putIfAbsent('timestamp', () => now().toUtc().toIso8601String());
    normalized.putIfAbsent('schemaVersion', () => '1.0');
    normalized.putIfAbsent('properties', () => <String, dynamic>{});
    normalized.putIfAbsent('traits', () => <String, dynamic>{});
    normalized.putIfAbsent('consent', () => <String, dynamic>{});
    final context = _deepCopyMap(
      normalized['context'] is Map
          ? normalized['context'] as Map<Object?, Object?>
          : const {},
    );
    context['library'] = {'name': 'customy-data-flutter', 'version': version};
    normalized['context'] = context;
    normalized = _redact(normalized) as JsonMap;
    final hook = beforeSend;
    if (hook != null) {
      normalized = hook(_deepCopyMap(normalized)) ??
          (throw const CustomyDataException('event blocked by beforeSend'));
      normalized = _deepCopyMap(normalized);
      _rejectTenantFields(normalized);
      _validate(normalized);
      normalized = _redact(normalized) as JsonMap;
    }
    return normalized;
  }

  Future<JsonMap> sendEvent(Map<String, Object?> input) =>
      _request('event', event(input));

  Future<JsonMap> track(
    String name, {
    Map<String, Object?> properties = const {},
    required Map<String, Object?> identity,
  }) =>
      sendEvent({
        ...identity,
        'type': 'track',
        'event': name,
        'properties': properties,
      });

  Future<JsonMap> identify(
    Map<String, Object?> traits, {
    required Map<String, Object?> identity,
  }) =>
      sendEvent({...identity, 'type': 'identify', 'traits': traits});

  Future<JsonMap> group(
    Map<String, Object?> traits, {
    required Map<String, Object?> identity,
  }) =>
      sendEvent({...identity, 'type': 'group', 'traits': traits});

  Future<JsonMap> page(
    Map<String, Object?> properties, {
    required Map<String, Object?> identity,
  }) =>
      sendEvent({...identity, 'type': 'page', 'properties': properties});

  Future<JsonMap> screen(
    Map<String, Object?> properties, {
    required Map<String, Object?> identity,
  }) =>
      sendEvent({...identity, 'type': 'screen', 'properties': properties});

  Future<JsonMap> alias(
    String userId,
    String previousId, {
    Map<String, Object?> identity = const {},
  }) =>
      sendEvent({
        ...identity,
        'type': 'alias',
        'userId': userId,
        'anonymousId': previousId,
        'properties': {'previousId': previousId},
      });

  int enqueue(Map<String, Object?> input) {
    if (queueSize >= maxQueueSize) {
      throw const CustomyDataException('customer data queue is full');
    }
    _queue.add(event(input));
    return queueSize;
  }

  Future<JsonMap> flush() async {
    if (_flushing) {
      throw const CustomyDataException(
        'a customer data flush is already in progress',
      );
    }
    _flushing = true;
    final pending = List<JsonMap>.from(_queue);
    _queue.clear();
    _inFlightCount = pending.length;
    try {
      if (pending.isEmpty) return _emptyBatch();
      final aggregate = _emptyBatch();
      for (var offset = 0; offset < pending.length; offset += maxBatchSize) {
        final end = min(offset + maxBatchSize, pending.length);
        final response = await _request('batch', {
          'batch': pending.sublist(offset, end),
        });
        for (final key in ['accepted', 'deduplicated', 'quarantined']) {
          aggregate[key] = _number(aggregate[key]) + _number(response[key]);
        }
        (aggregate['results'] as List<Object?>).addAll(
          (response['results'] as List<Object?>?) ?? const [],
        );
      }
      _inFlightCount = 0;
      return aggregate;
    } catch (_) {
      _queue.insertAll(0, pending);
      _inFlightCount = 0;
      rethrow;
    } finally {
      _flushing = false;
    }
  }

  Future<JsonMap> _request(String path, Object payload) async {
    final body = utf8.encode(jsonEncode(payload));
    final headers = {
      HttpHeaders.contentTypeHeader: 'application/json',
      HttpHeaders.userAgentHeader: 'customy-data-flutter/$version',
      'x-write-key': writeKey,
    };
    Object? lastError;
    for (var attempt = 0; attempt <= max(0, maxRetries); attempt++) {
      try {
        final response = await transport(
          Uri.parse('$collectUrl/v1/collect/$path'),
          headers,
          body,
          timeout,
        );
        final parsed = _parse(response.body);
        if (response.statusCode >= 200 && response.statusCode < 300) {
          return parsed;
        }
        throw CustomyDataException(
          'Customy Data collection failed with HTTP ${response.statusCode}',
          statusCode: response.statusCode,
          response: parsed,
        );
      } catch (error) {
        lastError = error;
        if (attempt >= max(0, maxRetries) || !_retryable(error)) {
          if (error is CustomyDataException) rethrow;
          throw CustomyDataException('Customy Data collection failed: $error');
        }
        await Future<void>.delayed(retryBase * (1 << attempt));
      }
    }
    throw CustomyDataException('Customy Data collection failed: $lastError');
  }

  static Future<DataResponse> _httpTransport(
    Uri url,
    Map<String, String> headers,
    List<int> body,
    Duration timeout,
  ) async {
    final client = HttpClient()..connectionTimeout = timeout;
    try {
      final request = await client.postUrl(url).timeout(timeout);
      headers.forEach(request.headers.set);
      request.add(body);
      final response = await request.close().timeout(timeout);
      return DataResponse(
        response.statusCode,
        await response.fold<List<int>>([], (all, bytes) => all..addAll(bytes)),
      );
    } finally {
      client.close(force: true);
    }
  }

  void _validate(JsonMap value) {
    final type = value['type'];
    if (!_eventTypes.contains(type)) {
      throw ArgumentError(
        'type must be track, identify, group, page, screen or alias',
      );
    }
    if (![
      'userId',
      'anonymousId',
      'groupId',
    ].any((key) => _present(value[key]))) {
      throw ArgumentError(
        'at least one userId, anonymousId or groupId is required',
      );
    }
    if (type == 'track' && !_present(value['event'])) {
      throw ArgumentError('track calls require an event name');
    }
  }

  void _rejectTenantFields(JsonMap value) {
    final found = _forbiddenTenantFields.where(value.containsKey).toList()
      ..sort();
    if (found.isNotEmpty) {
      throw ArgumentError(
        'tenant scope is derived from the write key; forbidden fields: $found',
      );
    }
  }

  Object? _redact(Object? value) {
    if (value is Map) {
      return <String, dynamic>{
        for (final entry in value.entries)
          entry.key.toString(): redactFields.contains(entry.key.toString())
              ? '[REDACTED]'
              : _redact(entry.value),
      };
    }
    if (value is List) return value.map(_redact).toList();
    return value;
  }

  static JsonMap _deepCopyMap(Map<Object?, Object?> value) => {
        for (final entry in value.entries)
          entry.key.toString(): _deepCopy(entry.value),
      };

  static Object? _deepCopy(Object? value) {
    if (value is Map) return _deepCopyMap(value);
    if (value is List) return value.map(_deepCopy).toList();
    return value;
  }

  static JsonMap _parse(List<int> body) {
    if (body.isEmpty) return {};
    final decoded = jsonDecode(utf8.decode(body));
    if (decoded is! Map) throw const FormatException('expected a JSON object');
    return _deepCopyMap(decoded);
  }

  static bool _retryable(Object error) =>
      error is! CustomyDataException ||
      error.statusCode == null ||
      _retryableStatuses.contains(error.statusCode);

  static bool _present(Object? value) =>
      value != null && (value is! String || value.isNotEmpty);

  static num _number(Object? value) => value is num ? value : 0;

  static JsonMap _emptyBatch() => {
        'accepted': 0,
        'deduplicated': 0,
        'quarantined': 0,
        'results': <Object?>[],
      };

  static String _randomId() {
    final random = Random.secure();
    final bytes = List<int>.generate(16, (_) => random.nextInt(256));
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    final hex =
        bytes.map((byte) => byte.toRadixString(16).padLeft(2, '0')).join();
    return '${hex.substring(0, 8)}-${hex.substring(8, 12)}-'
        '${hex.substring(12, 16)}-${hex.substring(16, 20)}-${hex.substring(20)}';
  }
}

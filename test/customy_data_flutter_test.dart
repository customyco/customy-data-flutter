import 'dart:convert';
import 'dart:io';

import 'package:customy_data_flutter/customy_data_flutter.dart';
import 'package:test/test.dart';

final class Recorder {
  Recorder([Iterable<int> statuses = const []]) : statuses = [...statuses];

  final List<int> statuses;
  final List<JsonMap> bodies = [];

  Future<DataResponse> send(
    Uri url,
    Map<String, String> headers,
    List<int> body,
    Duration timeout,
  ) async {
    final payload = jsonDecode(utf8.decode(body)) as JsonMap;
    bodies.add(payload);
    final status = statuses.isEmpty ? 202 : statuses.removeAt(0);
    final count =
        payload['batch'] is List ? (payload['batch'] as List).length : 1;
    return DataResponse(
      status,
      utf8.encode(
        jsonEncode(
          status < 300
              ? {
                  'accepted': count,
                  'deduplicated': 0,
                  'quarantined': 0,
                  'results': [],
                }
              : {'error': 'temporary'},
        ),
      ),
    );
  }
}

CustomyDataFlutter client(
  Recorder recorder, {
  int maxRetries = 3,
  int maxBatchSize = 100,
  Set<String> redactFields = const {},
  BeforeSend? beforeSend,
}) {
  var id = 0;
  return CustomyDataFlutter(
    collectUrl: 'https://data.customy.ai',
    writeKey: 'cdw_test',
    transport: recorder.send,
    maxRetries: maxRetries,
    maxBatchSize: maxBatchSize,
    retryBase: Duration.zero,
    redactFields: redactFields,
    beforeSend: beforeSend,
    now: () => DateTime.utc(2026, 8, 16),
    idFactory: () => 'message_${++id}',
  );
}

void main() {
  test('portable six-call conformance', () async {
    final vectors = jsonDecode(
      File('conformance/customer-data-v1.json').readAsStringSync(),
    ) as JsonMap;
    expect(vectors['contract'], CustomyDataFlutter.conformanceContract);
    final recorder = Recorder();
    final sdk = client(recorder);
    for (final event in vectors['eventTypes'] as List) {
      await sdk.sendEvent(Map<String, Object?>.from(event as Map));
    }
    expect(recorder.bodies.map((event) => event['type']), [
      'track',
      'identify',
      'group',
      'page',
      'screen',
      'alias',
    ]);
    for (final event in recorder.bodies) {
      expect(event['schemaVersion'], '1.0');
      for (final key in vectors['forbiddenPayloadKeys'] as List) {
        expect(event.containsKey(key), isFalse);
      }
    }
  });

  test('retry keeps a stable messageId', () async {
    final recorder = Recorder([503, 429, 202]);
    await client(recorder).track(
      'Checkout Started',
      properties: {'value': 10},
      identity: {'anonymousId': 'anon_1'},
    );
    expect(recorder.bodies.map((event) => event['messageId']).toSet(), {
      'message_1',
    });
  });

  test('redacts after hook and rejects forged tenant scope', () async {
    final recorder = Recorder();
    final sdk = client(
      recorder,
      redactFields: {'password'},
      beforeSend: (event) {
        event['traits'] = {'password': 'reintroduced'};
        return event;
      },
    );
    await sdk.identify({'password': 'secret'}, identity: {'userId': 'user_1'});
    expect(
      (recorder.bodies.single['traits'] as JsonMap)['password'],
      '[REDACTED]',
    );
    expect(
      () => sdk.sendEvent({
        'type': 'identify',
        'userId': 'user_1',
        'organizationId': 'forged',
      }),
      throwsArgumentError,
    );
  });

  test('restores the queue after partial batch failure', () async {
    final sdk = client(Recorder([202, 503]), maxRetries: 0, maxBatchSize: 2);
    for (final name in ['A', 'B', 'C']) {
      sdk.enqueue({'type': 'track', 'event': name, 'anonymousId': 'anon_1'});
    }
    await expectLater(sdk.flush(), throwsA(isA<CustomyDataException>()));
    expect(
      sdk.enqueue({'type': 'track', 'event': 'D', 'anonymousId': 'anon_1'}),
      4,
    );
  });
}

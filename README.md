# Customy Data SDK for Flutter

Portable Dart SDK for governed `track`, `identify`, `group`, `page`, `screen`
and `alias` collection from Flutter mobile and desktop apps.

```dart
final data = CustomyDataFlutter(
  collectUrl: 'https://data.customy.ai',
  writeKey: 'cdw_your_source_write_key',
  redactFields: {'password', 'cardNumber'},
);

await data.track(
  'Product Viewed',
  properties: {'sku': 'A-1'},
  identity: {'anonymousId': 'anon_123'},
);
```

The source write key is the only tenant authority. The SDK rejects forged
tenant fields before and after `beforeSend`, redacts recursively after the
hook, retries with a stable `messageId`, bounds its queue, rejects concurrent
flushes and restores pending events after partial batch failures. It writes to
Customy Data only; Customy Analytics consumes governed read models.

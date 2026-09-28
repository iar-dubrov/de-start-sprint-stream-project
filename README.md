# Сервис уведомлений об акциях ресторанов для подписчиков

Стриминговое приложение на Spark Structured Streaming. Оно читает акции ресторанов из Kafka, находит подписчиков этих ресторанов в PostgreSQL и для каждого подписчика отправляет заготовку push-уведомления:

- в Kafka — для сервиса push-уведомлений;
- в PostgreSQL — для последующего сбора фидбэка.

## Как это работает

```
Kafka (<логин>_in)  ──►  Spark Structured Streaming  ──►  Kafka (<логин>_out)
  акции ресторанов        │  1. JSON из value → колонки      push-уведомления
                          │  2. только действующие акции
                          │  3. join с подписчиками    ──►  PostgreSQL (localhost)
Postgres (облако)  ───────┘     по restaurant_id              subscribers_feedback
  subscribers_restaurants  4. + trigger_datetime_created
```

1. Из входного топика читаются сообщения с акциями, `value` десериализуется из JSON.
2. Остаются только акции, которые действуют сейчас: `adv_campaign_datetime_start ≤ now ≤ adv_campaign_datetime_end`.
3. Акции соединяются (inner join по `restaurant_id`) с таблицей подписчиков `subscribers_restaurants`, добавляется `trigger_datetime_created` — время создания уведомления.
4. Каждый микробатч через `foreachBatch`:
   - кэшируется (`persist`);
   - дописывается в локальную таблицу `subscribers_feedback` с пустым полем `feedback`;
   - сериализуется в JSON без `feedback` и отправляется в выходной топик;
   - удаляется из памяти (`unpersist`).

## Файлы

| Файл | Назначение |
|---|---|
| `project.py` | стриминговое приложение |
| `create_tables.sql` | DDL выходной таблицы `public.subscribers_feedback` |



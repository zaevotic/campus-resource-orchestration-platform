"""
Real Apache Kafka adapter (aiokafka) exposing the SAME interface as the in-memory
PartitionedInMemoryBroker, so no service code has to change:

    await broker.start() / await broker.stop()
    await broker.produce(topic, key, value, headers)
    broker.subscribe(topic, callback)       # callback(MessageRecord), sync or async
    broker.get_metrics()                    # used by /health
    broker.get_partition(key) / broker.num_partitions

Enable with:  USE_REAL_KAFKA=true  KAFKA_BOOTSTRAP_SERVERS=localhost:9092

Delivery semantics
- Producer: idempotent, acks=all  (no duplicates on producer retries)
- Consumer: one consumer loop per (topic, consumer-group). Offsets are committed
  manually AFTER the handler finishes -> at-least-once delivery.
  Group id is taken from the subscribing object's CONSUMER_GROUP attribute, so the
  notification and analytics services each receive every event (fan-out).
- Partitioning: Kafka's own murmur2(key) % partitions, key = resource_id, so all events
  of one resource stay ordered within a single partition (STR-02).
"""
import asyncio
import hashlib
import logging
from collections import defaultdict
from typing import Callable, Dict, List, Tuple

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from src.common.config import settings
from src.common.message_broker import MessageRecord

logger = logging.getLogger(__name__)

ALL_TOPICS = [
    settings.TOPIC_BOOKING_EVENTS,
    settings.TOPIC_SERVICE_REQUESTS,
    settings.TOPIC_DLQ,
    settings.TOPIC_AUDIT,
]


class KafkaBroker:
    def __init__(self):
        self.bootstrap = settings.KAFKA_BOOTSTRAP_SERVERS
        self.num_partitions = settings.KAFKA_NUM_PARTITIONS
        self._producer: AIOKafkaProducer | None = None
        self._subs: List[Tuple[str, str, Callable]] = []          # (topic, group_id, callback)
        self._tasks: List[asyncio.Task] = []
        self._consumers: List[AIOKafkaConsumer] = []
        # topic -> partition -> latest offset + 1 seen by this process (for /health)
        self._end_offsets: Dict[str, Dict[int, int]] = defaultdict(dict)
        self._started = False

    # ---------------------------------------------------------------- lifecycle
    async def start(self):
        if self._started:
            return
        await self._ensure_topics()

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap,
            enable_idempotence=True,
            acks="all",
        )
        await self._producer.start()

        for topic, group_id, callback in self._subs:
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap,
                group_id=group_id,
                enable_auto_commit=False,        # commit manually after processing
                auto_offset_reset="earliest",
            )
            await consumer.start()
            self._consumers.append(consumer)
            self._tasks.append(asyncio.create_task(self._consume_loop(consumer, topic, group_id, callback)))

        self._started = True
        logger.info("KafkaBroker connected to %s (%d subscriptions)", self.bootstrap, len(self._subs))

    async def stop(self):
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for c in self._consumers:
            await c.stop()
        if self._producer:
            await self._producer.stop()
        self._tasks.clear()
        self._consumers.clear()
        self._started = False

    async def _ensure_topics(self):
        """Connect (with retries, Kafka may still be booting) and create topics with N partitions."""
        last_err = None
        for attempt in range(1, 11):
            admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap)
            try:
                await admin.start()
                try:
                    await admin.create_topics([
                        NewTopic(name=t, num_partitions=self.num_partitions,
                                 replication_factor=settings.KAFKA_REPLICATION_FACTOR)
                        for t in ALL_TOPICS
                    ])
                except TopicAlreadyExistsError:
                    pass
                await admin.close()
                return
            except Exception as e:                       # broker not reachable yet
                last_err = e
                logger.warning("Kafka not ready (%s) attempt %d/10", e, attempt)
                try:
                    await admin.close()
                except Exception:
                    pass
                await asyncio.sleep(2)
        raise RuntimeError(
            f"Cannot reach Kafka at {self.bootstrap}: {last_err}. "
            "Start it with `docker compose up -d` or unset USE_REAL_KAFKA to use the in-memory broker."
        )

    # ---------------------------------------------------------------- producing
    def get_partition(self, key: str) -> int:
        """Informational only: real routing is done by Kafka's murmur2 partitioner."""
        if not key:
            return 0
        return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16) % self.num_partitions

    async def produce(self, topic: str, key: str, value: bytes, headers: dict = None) -> MessageRecord:
        if not self._producer:
            raise RuntimeError("KafkaBroker.start() has not been called")
        kafka_headers = [(k, str(v).encode("utf-8")) for k, v in (headers or {}).items()]
        md = await self._producer.send_and_wait(
            topic, value=value, key=key.encode("utf-8") if key else None, headers=kafka_headers
        )
        cur = self._end_offsets[topic].get(md.partition, 0)
        self._end_offsets[topic][md.partition] = max(cur, md.offset + 1)
        return MessageRecord(topic=topic, key=key, value=value, headers=headers or {},
                             partition=md.partition, offset=md.offset)

    # ---------------------------------------------------------------- consuming
    def subscribe(self, topic: str, callback: Callable):
        group_id = getattr(getattr(callback, "__self__", None), "CONSUMER_GROUP", "default-group")
        self._subs.append((topic, group_id, callback))

    async def _consume_loop(self, consumer: AIOKafkaConsumer, topic: str, group_id: str, callback: Callable):
        try:
            async for msg in consumer:
                headers = {k: (v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else v)
                           for k, v in (msg.headers or [])}
                record = MessageRecord(
                    topic=msg.topic,
                    key=msg.key.decode("utf-8") if msg.key else "",
                    value=msg.value, headers=headers,
                    partition=msg.partition, offset=msg.offset,
                )
                try:
                    res = callback(record)
                    if asyncio.iscoroutine(res):
                        await res
                except Exception as e:
                    # Handlers already retry + route to the DLQ internally. Anything that still
                    # escapes (e.g. an undecodable payload) is logged and skipped so one poison
                    # message cannot block the partition forever.
                    logger.error("[%s] handler error on %s p%d@%d: %s", group_id, topic, msg.partition, msg.offset, e)
                await consumer.commit()
        except asyncio.CancelledError:
            pass

    # ---------------------------------------------------------------- observability
    def get_metrics(self) -> dict:
        summary = {}
        for topic in ALL_TOPICS:
            parts = {p: self._end_offsets[topic].get(p, 0) for p in range(self.num_partitions)}
            summary[topic] = {"total_messages": sum(parts.values()), "partitions": parts}
        return summary

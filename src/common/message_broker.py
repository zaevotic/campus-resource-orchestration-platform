import asyncio
import json
import logging
from typing import Callable, Dict, List, Optional
from collections import defaultdict
import hashlib

logger = logging.getLogger(__name__)

class MessageRecord:
    def __init__(self, topic: str, key: str, value: bytes, headers: dict = None, partition: int = 0, offset: int = 0):
        self.topic = topic
        self.key = key
        self.value = value
        self.headers = headers or {}
        self.partition = partition
        self.offset = offset

class PartitionedInMemoryBroker:
    """
    High-performance in-memory Kafka emulation satisfying STR-02:
    - Partitioned topics
    - Partitioning keyed deterministically by resource ID (hash(key) % partitions)
    - Consumer groups with committed offset tracking
    - Replay support from offset / start
    """
    def __init__(self, num_partitions: int = 4):
        self.num_partitions = num_partitions
        # topic -> partition_id -> list of MessageRecord
        self.topics: Dict[str, Dict[int, List[MessageRecord]]] = defaultdict(lambda: {p: [] for p in range(self.num_partitions)})
        # topic -> group_id -> partition_id -> committed_offset
        self.group_offsets: Dict[str, Dict[str, Dict[int, int]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        self._subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._lock = asyncio.Lock()

    def get_partition(self, key: str) -> int:
        if not key:
            return 0
        h = int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)
        return h % self.num_partitions

    async def produce(self, topic: str, key: str, value: bytes, headers: dict = None) -> MessageRecord:
        partition = self.get_partition(key)
        async with self._lock:
            records = self.topics[topic][partition]
            offset = len(records)
            record = MessageRecord(topic=topic, key=key, value=value, headers=headers or {}, partition=partition, offset=offset)
            records.append(record)
            
        # Dispatch to any registered consumers
        for callback in list(self._subscribers[topic]):
            try:
                res = callback(record)
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                logger.error(f"Error in subscriber callback for topic {topic}: {e}")
        return record

    def consume(self, topic: str, group_id: str, partition: int, max_records: int = 50) -> List[MessageRecord]:
        current_offset = self.group_offsets[topic][group_id][partition]
        partition_records = self.topics[topic][partition]
        records_to_return = partition_records[current_offset:current_offset + max_records]
        if records_to_return:
            self.group_offsets[topic][group_id][partition] = current_offset + len(records_to_return)
        return records_to_return

    def seek(self, topic: str, group_id: str, partition: int, offset: int):
        self.group_offsets[topic][group_id][partition] = max(0, offset)

    def subscribe(self, topic: str, callback: Callable):
        self._subscribers[topic].append(callback)

    # Lifecycle hooks (no-ops here; the real Kafka broker uses them)
    async def start(self):
        pass

    async def stop(self):
        pass

    def get_metrics(self) -> dict:
        summary = {}
        for topic, part_dict in self.topics.items():
            total = sum(len(records) for records in part_dict.values())
            summary[topic] = {
                "total_messages": total,
                "partitions": {p: len(records) for p, records in part_dict.items()}
            }
        return summary

# Global message broker instance.
# USE_REAL_KAFKA=true  -> real Kafka via aiokafka (src/common/kafka_broker.py)
# otherwise            -> in-memory Kafka emulation (default; used by the test-suite)
from src.common.config import settings as _settings  # noqa: E402

if _settings.USE_REAL_KAFKA:
    # Imported here (after MessageRecord is defined) to avoid a circular-import problem
    from src.common.kafka_broker import KafkaBroker  # noqa: E402
    broker = KafkaBroker()
else:
    broker = PartitionedInMemoryBroker(num_partitions=_settings.KAFKA_NUM_PARTITIONS)

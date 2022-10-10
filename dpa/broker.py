from logging import Logger
from enum import IntEnum
from collections import defaultdict
from typing import Protocol, Optional, Any

from dpa.datastore import DatastoreDescription
from util import ConsistentHash, obj_to_bytes
from dpa.query import (
    QueryEngine,
    Row,
    TableInfo,
    ShuffleReadQueryPlan,
    SimpleWriteQueryPlan,
    AnchoredReadQueryPlan,
    WriteQueryPlan,
    AnchoredReadQueryMessage,
    AnchoredReadQueryResponse,
    ShuffleReadQueryMessage,
    ShuffleReadQueryResponse,
)
from dpa import logger


class BrokerDCS(Protocol):
    def close(self):
        ...

    def get_datastore_description(self, datastore_id: int) -> DatastoreDescription:
        ...

    def set_transaction_status(self, tx_id: int, status: int) -> None:
        ...

    def get_master_location(self) -> Optional[tuple[str, int]]:
        ...

    def get_consistent_hash_function(self) -> ConsistentHash:
        ...

    def acquire_write_lock(self):
        ...

    def release_write_lock(self):
        ...

    def get_tx_id_and_increment(self) -> int:
        ...

    def get_last_committed_version(self) -> int:
        ...

    def set_last_committed_version(self, tx_id: int):
        ...


class Daemon(Protocol):
    """Could be implemented as a thread or a child process"""

    def start(self):
        ...

    def run(self):
        ...

    def join(self, timeout: Optional[float]):
        ...


class Channel(Protocol):
    """grpc channel?"""

    pass


class QueryStatus(IntEnum):
    SUCCESS = 0
    FAILURE = 1
    RETRY = 2


SHARDS_PER_TABLE = 1_000_000


class Broker:
    _query_engine: QueryEngine
    _coordination: BrokerDCS
    log: Logger = logger.null
    _consistent_hash: ConsistentHash
    _datastore_id_to_channel: dict[int, Channel]
    _broker_coordinator_stub: Any
    _table_info_map: dict[str, TableInfo]

    _shard_map_update_daemon: Daemon
    run_shard_map_update_daemon: bool = True
    shard_map_daemon_sleep_duration_ms = 1000

    _query_statistics_daemon: Daemon
    run_query_statistics_daemon: bool = True
    query_statistics_daemon_sleep_duration_ms = 10000

    remote_execution_times: list[int] = []
    aggregation_times: list[int] = []

    query_statistics: dict[set[int], int] = dict()

    read_query_worker_pool: Any  # TODO provide suitable interface/impl

    # TODO should be concurrency safe, move to BrokerCoordination?
    tx_ids: int = 0

    # TODO should be concurrency safe, move to BrokerCoordination?
    last_committed_version: int = 0

    # TODO complete
    def __init__(self, coordination: BrokerDCS, query_engine: QueryEngine):
        self._coordination = coordination
        self._query_engine = query_engine

    def shutdown(self):
        # set self.run_query_statistics_daemon to False
        # set self.run_query_statistics_daemon to False
        # stop both daemons
        # close connection to coordinator
        # close connection to all datastores
        # collect query stats
        # close BrokerCoordination
        # close read query worker pool
        raise NotImplementedError

    def create_table(self, table_name: str, num_shards: int) -> bool:
        # send create table message to coordinator
        raise NotImplementedError

    def _get_datastore_conn(self, datasore_id: int):
        ...

    def _get_target_shards(
        self, partition_keys: dict[str, list[int]]
    ) -> dict[str, list[int]]:
        target_shards: dict[str, list[int]] = {}
        for table_name, table_partition_keys in partition_keys.items():
            table_info = self._get_table_info(table_name)
            table_id = table_info.id_
            target_shards_single_table = None
            # -1 is a wildcard to include all shards
            if -1 in table_partition_keys:
                target_shards_single_table = [
                    s
                    for s in range(
                        table_id * SHARDS_PER_TABLE,
                        (table_id * SHARDS_PER_TABLE) + table_info.num_shards,
                    )
                ]
            else:
                target_shards_single_table = list(
                    {
                        self._key_to_shard(table_id, table_info.num_shards, k)
                        for k in table_partition_keys
                    }
                )
            target_shards[table_name] = target_shards_single_table

        return target_shards

    def shuffle_read_query(self, plan: ShuffleReadQueryPlan):
        tx_id = self._coordination.get_tx_id_and_increment()
        partition_keys = plan.keys_for_query()
        target_shards = self._get_target_shards(partition_keys)

        datastore_ids: list[int] = []  # TODO flesh out
        # TODO add latch
        msg = ShuffleReadQueryMessage(
            query=plan,
            repartition_num=0,
            num_repartitions=len(datastore_ids),
            tx_id=tx_id,
            target_shards=target_shards,
        )
        res_futures = []
        for datastore_id in datastore_ids:
            ds = None
            res_future = ds.shuffle_read_query(msg)
            # TODO fault tolerance
            res_futures.append(res_future)

        # gather results
        # TODO handle failure?
        intermediates: list[bytes] = []  # TODO flesh out
        for res_future in res_futures:
            res: ShuffleReadQueryResponse = res_future.result()
            intermediates.append(res.response)

        ret = plan.combine(intermediates)
        # TODO collect stats
        return ret

    def anchored_read_query(self, plan: AnchoredReadQueryPlan):
        tx_id = self._coordination.get_tx_id_and_increment()
        partition_keys = plan.keys_for_query()
        target_shards = self._get_target_shards(partition_keys)

        intermediate_shards: dict[str, dict[int, int]] = {}
        for subquery in plan.get_subqueries():
            subquery_shards = self.anchored_read_query(subquery)
            intermediate_shards.update(subquery_shards)
            for table_name, entries in subquery_shards:
                target_shards[table_name] = list(entries.values())

        anchor_table = plan.get_anchor_table()
        anchor_table_shards = target_shards[anchor_table]

        res_futures = []
        for anchor_shard_num in anchor_table_shards:
            ds_id = self._consistent_hash.get_random_bucket(anchor_shard_num)
            datastore = self._get_datastore_conn(ds_id)
            msg = AnchoredReadQueryMessage(
                target_shard=anchor_shard_num,
                serialized_query=plan,
                num_repartitions=len(anchor_table_shards),
                tx_id=tx_id,
                last_committed_version=self._coordination.get_last_committed_version(),
                target_shards=target_shards,
                intermediate_shards=intermediate_shards,
            )
            res_future = datastore.anchored_read_query(msg)
            res_futures.append(res_future)

        # gather results
        intermediates: list[bytes] = []
        for res_future in res_futures:
            res: AnchoredReadQueryResponse = res_future.result()
            intermediates.append(res.response)

        return_table = plan.get_return_table_name()
        if return_table is None:
            result = plan.combine(intermediates)
            return result
        else:
            shard_locations: list[dict[int, int]] = []
            for bs in intermediates:
                loc: dict[int, int] = None  # TODO parse from bs
                shard_locations.append(loc)
            combined_shard_locations: dict[int, int] = {}
            for e in shard_locations:
                combined_shard_locations.update(e)
            intermediate_shards[return_table] = combined_shard_locations
            return intermediate_shards

    def _group_rows_by_shard(
        self, rows: list[Row], table_info: TableInfo
    ) -> dict[int, list[Row]]:
        rows_grouped_by_shard: dict[int, list[Row]] = defaultdict(list)
        for row in rows:
            partition_key = row.get_partition_key()
            shard = self._key_to_shard(
                table_info.id_,
                table_info.num_shards,
                partition_key,
            )
            rows_grouped_by_shard[shard].append(row)

        return rows_grouped_by_shard

    def simple_write_query(self, plan: SimpleWriteQueryPlan, rows: list[Row]):
        # TODO maybe have rows as a generator or iterator or async iterator?
        table_info = self._get_table_info(plan.get_table())
        rows_grouped_by_shard = self._group_rows_by_shard(rows, table_info)

        tx_id = self._coordination.get_tx_id_and_increment()
        for shard, rows_for_shard in rows_grouped_by_shard.items():
            # TODO send as promises
            # send shard, plan, rows_for_shard, tx_id, query_status?
            pass

        self._coordination.set_last_committed_version(tx_id)

    def write_query(self, plan: WriteQueryPlan, rows: list[Row]):
        table_info = self._get_table_info(plan.get_table())
        rows_grouped_by_shard = self._group_rows_by_shard(rows, table_info)
        self._coordination.acquire_write_lock()
        tx_id = self._coordination.get_tx_id_and_increment()
        for shard, rows_for_shard in rows_grouped_by_shard.items():
            # TODO send as promises
            # send shard, plan, rows_for_shard, tx_id, query_status?, latch?
            pass
        self._coordination.set_last_committed_version(tx_id)
        self._coordination.release_write_lock()

    def _get_table_info(self, table_name: str) -> TableInfo:
        ...

    @staticmethod
    def _key_to_shard(table_id: int, num_shards: int, partition_key: int) -> int:
        return table_id * SHARDS_PER_TABLE + (partition_key % num_shards)

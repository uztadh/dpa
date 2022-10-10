from typing import Protocol, Optional, Any
from dataclasses import dataclass

from dpa.datastore import Shard


class QueryEngine(Protocol):
    """Generates query plan. Lives on broker"""

    ...


class Row(Protocol):
    """
    Row holds a row of data
    """

    def get_partition_key(self) -> int:
        """
        Returns the row's (non-negative) partition key.
        Objects with the same key are stored in the same shard.
        """
        ...


class ShuffleReadQueryPlan(Protocol):
    """
    Read query plan
    """

    def get_queried_tables(self) -> list[str]:
        """get tables being queried"""
        ...

    def keys_for_query(self) -> dict[str, list[int]]:
        """
        Keys on which the query executes. Query will execute on all shards
        containing any key from the list.
        Include -1 to execute on all shards.
        """
        ...

    def scatter(self, shard: Shard, num_repartition: int) -> dict[int, list[bytes]]:
        """Scatter"""
        ...

    def gather(
        self, ephemeral_data: dict[str, list[bytes]], ephemeral_shards: dict[str, Shard]
    ) -> dict[int, list[bytes]]:
        """Gather"""
        ...

    def combine(self, shard_query_results: list[bytes]) -> Any:
        """
        The query will return the result of this function executed on all
        results from gather
        """
        ...


class AnchoredReadQueryPlan(Protocol):
    def get_queried_tables(self) -> list[str]:
        """get tables being queried"""
        ...

    def keys_for_query(self) -> dict[str, list[int]]:
        """
        Keys on which the query executes. Query will execute on all shards
        containing any key from the list.
        Include -1 to execute on all shards.
        """
        ...

    def get_anchor_table(self) -> str:
        """which table does not need shuffling"""
        ...

    def get_subqueries(self) -> list["AnchoredReadQueryPlan"]:
        """for multi-stage queries"""
        ...

    def get_partition_keys(self, shard: Shard) -> list[int]:
        """get partition keys"""
        ...

    def scatter(
        self,
        shard: Shard,
        partition_keys: dict[int, list[int]],
    ) -> dict[int, list[bytes]]:
        """Scatter"""
        ...

    def gather(
        self,
        local_shard: Shard,
        ephemeral_data: dict[str, list[bytes]],
        ephemeral_shards: dict[str, Shard],
    ) -> bytes:
        """Gather"""
        ...

    def gather_to_shard(
        self,
        local_shard: Shard,
        ephemeral_data: dict[str, list[bytes]],
        ephemeral_shards: dict[str, Shard],
        return_shard: Shard,
    ):
        ...

    def get_return_table_name(self) -> Optional[str]:
        """return aggregate or shard? (Default aggregate)?"""
        ...

    def combine(self, shard_query_results: list[bytes]) -> Any:
        """
        The query will return the result of this function executed on all
        results from gather
        """
        ...


class SimpleWriteQueryPlan(Protocol):
    def get_table(self) -> str:
        """table being queried"""
        ...

    def write(self, shard: Shard, rows: list[Row]):
        ...


class WriteQueryPlan:
    def get_table(self) -> str:
        """table being queried"""
        ...

    def pre_commit(self, shard: Shard, rows: list[Row]) -> bool:
        """
        stage a write query on the rows, return true if ready commit, false
        if must abort
        """
        ...

    def commit(self, shard: Shard):
        ...

    def abort(self, shard: Shard):
        ...


@dataclass
class TableInfo:
    name: str
    id_: int
    num_shards: int


@dataclass
class WriteQueryMessage:
    shard: int
    serialized_query: bytes
    row_data: bytes
    tx_id: int
    write_state: int


@dataclass
class WriteQueryResponse:
    return_code: int


@dataclass
class AnchoredReadQueryMessage:
    target_shard: int
    query: AnchoredReadQueryPlan
    num_repartitions: int
    tx_id: int
    last_committed_version: int
    target_shards: dict[str, list[int]]
    intermediate_shards: dict[str, dict[int, int]]


@dataclass
class AnchoredReadQueryResponse:
    return_code: int
    response: bytes


@dataclass
class ShuffleReadQueryMessage:
    query: ShuffleReadQueryPlan
    repartition_num: int
    num_repartitions: int
    tx_id: int
    target_shards: dict[str, list[int]]  # table_name -> list of shards


@dataclass
class ShuffleReadQueryResponse:
    return_code: int
    response: bytes

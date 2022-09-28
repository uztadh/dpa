from types import LambdaType
from typing import Protocol, Optional, Any
from enum import IntEnum
from pathlib import Path
from dataclasses import dataclass


class QueryEngine(Protocol):
    """Generates query plan. Lives on broker"""

    ...


class Row(Protocol):  # extends java.io.Serializable
    """
    Row holds a row of data
    """

    def get_partition_key(self) -> int:
        """
        Returns the row's (non-negative) partition key.
        Objects with the same key are stored in the same shard.
        """
        ...


class Shard(Protocol):
    """
    Stateful data structure.
    Writes never run simultaneously.
    shardToData never runs at the same time as a write.
    Reads run at any time.
    """

    def get_memory_usage(self) -> int:
        """return the amount of memory this shard uses in kilobytes"""
        ...

    def destroy(self):
        """
        destroy shard data and processes. After destruction, shard is no
        longer usable
        """
        ...

    def shard_to_data(self) -> Optional[Path]:
        """
        returns a diretory containing a serialization of this shard
        """
        ...


class ShardFactory(Protocol):
    """
    Create a shard storing data in a directory
    """

    def create_new_shard(self, shard_path: Path, shard_num: int) -> Shard:
        """create a new shard at shard_path"""
        ...

    def create_shard_from_dir(self, shard_path: Path, shard_num: int) -> Shard:
        """
        Load a serialized shard (using shard.shard_to_data) at shard_path
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

    def table_name(self) -> Optional[str]:
        """return aggregate or shard? (Default aggregate)?"""
        ...

    def combine(self, shard_query_results: list[bytes]) -> Any:
        """
        The query will return the result of this function executed on all
        results from gather
        """
        ...


class SimpleWriteQueryPlan(Protocol):
    def get_queried_tables(self) -> str:
        """table being queried"""
        ...

    def write(self, shard: Shard, rows: list[Row]):
        ...


class WriteQueryPlan:
    def get_queried_tables(self) -> str:
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


class DatastoreStatus(IntEnum):
    ALIVE = 0
    DEAD = 1


class DatastoreDescription:
    def __init__(self, id_: int, status: DatastoreStatus, host: str, port: int):
        self.id_ = id_
        self.status = status
        self.host = host
        self.port = port

    @staticmethod
    def parse_from_summary_str(s: str) -> Optional["DatastoreDescription"]:

        try:
            ts = s.split("\n")
            if len(ts) != 2:
                raise ValueError(f"Invalid summary string: {s}")
            id_, status = int(ts[0]), DatastoreStatus(int(ts[1]))
            host, port = ts[2], int(ts[3])
            return DatastoreDescription(id_, status, host, port)
        except Exception:
            return None

    def __str__(self) -> str:
        return f"{self.id_}\n{self.status}\n{self.host}\n{self.port}"


@dataclass
class TableInfo:
    name: str
    id_: int
    num_shards: int


class ZKShardDescription:
    cloud_name: str
    version_number: int

    def __init__(self, cloud_name: str, version_number: int):
        self.cloud_name = cloud_name
        self.version_number = version_number

    def __str__(self) -> str:
        return f"{self.cloud_name}\n{self.version_number}"

    @staticmethod
    def parse_from_summary_str(s: str):
        try:
            ts = s.split("\n")
            if len(ts) != 2:
                raise ValueError(f"Invalid summary string: {s}")
            cloud_name, version_number = ts[0], int(ts[1])
            return ZKShardDescription(cloud_name, version_number)
        except Exception as e:
            return None

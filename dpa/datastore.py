from typing import Protocol, Optional
from pathlib import Path
from enum import IntEnum

from dpa.util import ConsistentHash


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


class ShardDescription:
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
            return ShardDescription(cloud_name, version_number)
        except Exception as e:
            return None


class DatastoreCoordination(Protocol):
    def close(self):
        ...

    def get_datastore_description(self, datastore_id: int) -> DatastoreDescription:
        ...

    def get_transaction_status(self, tx_id: int) -> int:
        ...

    def get_master_location(self) -> Optional[tuple[str, int]]:
        ...

    def get_shard_description(self, shard: int) -> ShardDescription:
        ...

    def set_shard_description(self, shard: int, cloud_name: str, version_number: int):
        ...

    def get_consistent_hash_function(self) -> ConsistentHash:
        ...

    def get_other_datastore_descriptions(
        self, skip_me: int
    ) -> list[DatastoreDescription]:
        ...


class Datastore:
    datastore_id: int
    address: str
    readwrite_atomicity: bool
    serving: bool = False
    use_reflink: bool = False
    shardmap: dict[int, Shard]

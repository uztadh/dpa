from dataclasses import dataclass

from dpa.interfaces import AnchoredReadQueryPlan, ShuffleReadQueryPlan


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

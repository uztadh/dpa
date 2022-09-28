from typing import Optional, Any, Protocol, Callable
import math
import threading
import random
import pickle


class ConsistentHash(Protocol):
    """
    A consistent hashing function that assigns shards to servers(buckets).
    Assignments can be overriden by the "reassignment map", modified by the load
    balancer
    """

    hash_fn: Callable[[int], int]

    def add_bucket(self, bucket_num: int) -> None:
        """Add a bucket(server) to the consistent hash"""
        ...

    def remove_bucket(self, bucket_num: int) -> None:
        """Remove a bucket(server) from the consistent hash"""
        ...

    def get_buckets(self, key: int) -> list[int]:
        """
        Get all buckets a key(shard) is assigned to. By default, a shard is only
        assigned to one server, but the load balancer replication may assign it
        to more
        """
        ...

    def get_random_bucket(self, key: int) -> int:
        """Get a random bucket(server) to which a key(shard) is assigned"""
        ...


def _get_default_hash_fn() -> Callable[[int], int]:
    _A: float = (math.sqrt(5) - 1) / 2
    _m: int = 2147483647  # 2 ^ 31 - 1

    def hash_fn(k: int) -> int:
        return int(_m * (k * _A - math.floor(k * _A)))

    return hash_fn


_default_hash_fn = _get_default_hash_fn()


class DefaultConsistentHash(ConsistentHash):
    def __init__(self, hash_fn: Optional[Callable[[int], int]] = None):
        if hash_fn is None:
            hash_fn = _default_hash_fn
        self.hash_fn = hash_fn
        self._hashring: list[int] = []
        self._hash_to_bucket: dict[int, int] = dict()
        self._num_virtual_nodes = 10
        self._virtual_offset = 1234567
        self._lock = threading.Lock()

        # a mapping of keys reassigned away from their consistent-hash buckets
        self.reassignment_map: dict[int, list[int]] = dict()

        self.buckets: set[int] = set()

    def add_bucket(self, bucket_num: int):
        with self._lock:
            for i in range(self._num_virtual_nodes):
                hash = self.hash_fn((bucket_num + self._virtual_offset) * i)
                self._hashring.append(hash)
                self._hash_to_bucket[hash] = bucket_num
            self._hashring.sort()
            self.buckets.add(bucket_num)

    def remove_bucket(self, bucket_num: int):
        with self._lock:
            assert bucket_num in self.buckets
            to_remove = []
            for k, replicas_list in self.reassignment_map.items():
                replicas_list.remove(bucket_num)
                if len(replicas_list) == 0:
                    to_remove.append(k)
            for k in to_remove:
                del self.reassignment_map[k]
            for i in range(self._num_virtual_nodes):
                hash = self.hash_fn((bucket_num + self._virtual_offset) * i)
                self._hashring.remove(hash)
                del self._hash_to_bucket[hash]
            self._hashring.sort()
            self.buckets.remove(bucket_num)

    def get_buckets(self, key: int) -> list[int]:
        buckets = self.reassignment_map.get(key)
        if buckets is not None:
            return buckets
        with self._lock:
            hash = self.hash_fn(key)
            for n in self._hashring:
                if hash < n:
                    ret = self._hash_to_bucket[n]
                    return [ret]
            ret = self._hash_to_bucket[self._hashring[0]]
            return [ret]

    def get_random_bucket(self, key: int) -> int:
        try:
            return random.choice(self.reassignment_map[key])
        except KeyError:
            pass
        with self._lock:
            hash = self.hash_fn(key)
            for n in self._hashring:
                if hash < n:
                    ret = self._hash_to_bucket[n]
                    return ret

            ret = self._hash_to_bucket[self._hashring[0]]
            return ret


def parse_connection_str(s: str) -> Optional[tuple[str, int]]:
    try:
        split = s.split(":")
        host, port = split[0], int(split[1])
        return host, port
    except Exception:
        return None


def obj_to_bytes(obj: Any) -> bytes:
    return pickle.dumps(obj)


def bytes_to_obj(b: bytes) -> Any:
    return pickle.loads(b)

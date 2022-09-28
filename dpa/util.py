from typing import Optional, Any
import pickle


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

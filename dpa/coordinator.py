from typing import Protocol
from enum import IntEnum


class ScaleOpt(IntEnum):
    NO_CHANGE = 0
    ADD = 1
    REMOVE = 2


class AutoScaler(Protocol):
    def autoscale(
        self,
        server_cpu_usage: dict[int, float],
    ) -> ScaleOpt:
        ...


class DefaultAutoScaler(AutoScaler):
    def __init__(self):
        self.quiscence = 0
        self.quiscence_period = 2
        self.add_server_threshold = 0.7
        self.remove_server_threshold = 0.3

    def autoscale(self, server_cpu_usage: dict[int, float]) -> ScaleOpt:
        if len(server_cpu_usage) == 0:
            return ScaleOpt.NO_CHANGE
        # after acting, wait quiescencePeriod cycles before acting again for
        # CPU to rebalance.
        if self.quiscence > 0:
            self.quiscence -= 1
            return ScaleOpt.NO_CHANGE

        self.quiscence = self.quiscence_period
        average_cpu_usage = sum(server_cpu_usage.values()) / len(server_cpu_usage)

        if average_cpu_usage > self.add_server_threshold:
            # add a server:
            return ScaleOpt.ADD
        elif average_cpu_usage < self.remove_server_threshold:
            # remove a server:
            return ScaleOpt.REMOVE
        else:
            return ScaleOpt.NO_CHANGE


class CoordinatorCloud(Protocol):
    def add_datastore(self) -> bool:
        ...

    def remove_datastore(self, cloud_ID: int):
        ...

    def shutdown(self):
        ...


def main():
    scale = DefaultAutoScaler()
    usage = {0: 0.1, 1: 0.5, 2: 0.8}
    res = scale.autoscale(usage)
    print(res)


if __name__ == "__main__":
    main()

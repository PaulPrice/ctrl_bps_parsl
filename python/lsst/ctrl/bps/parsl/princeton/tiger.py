from typing import TYPE_CHECKING, List

from lsst.ctrl.bps import BpsConfig
from parsl.executors import HighThroughputExecutor
from parsl.executors.base import ParslExecutor
from parsl.launchers import SrunLauncher
from parsl.monitoring.monitoring import MonitoringHub
from parsl.providers import SlurmProvider

if TYPE_CHECKING:
    from ..job import ParslJob


def get_executors(config: BpsConfig) -> List[ParslExecutor]:
    nodes = config.get(".tiger.nodes", 4)
    cores_per_node = config.get(".tiger.cores_per_node", 40)
    walltime = config.get(".tiger.walltime", "8:00:00")
    mem_per_node = config.get(".tiger.mem_per_node", 192)
    return [
        HighThroughputExecutor(
            "default",
            provider=SlurmProvider(
                nodes_per_block=nodes,
                cores_per_node=cores_per_node,
                mem_per_node=mem_per_node,
                walltime=walltime,
                launcher=SrunLauncher(),
            ),
        ),
    ]


def select_executor(job: ParslJob) -> str:
    return "default"


def get_monitor(config: BpsConfig) -> MonitoringHub:
    pass

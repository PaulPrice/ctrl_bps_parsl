from typing import TYPE_CHECKING, List

from lsst.ctrl.bps import BpsConfig
from parsl.executors import HighThroughputExecutor
from parsl.executors.base import ParslExecutor
from parsl.launchers import SrunLauncher
from parsl.monitoring import MonitoringHub
from parsl.providers import SlurmProvider
from parsl.addresses import address_by_hostname, address_by_interface

from ..configuration import get_bps_config_value
from ..environment import export_environment
from ..slurm import make_executor

if TYPE_CHECKING:
    from ..job import ParslJob



def get_executors(config: BpsConfig) -> List[ParslExecutor]:
    return [make_executor(config, label="tiger", site="princeton.tiger", nodes=4, cores_per_node=40, walltime="8:00:00", mem_per_node=192, singleton=True, provider_options=dict(init_blocks=1, min_blocks=1, max_blocks=2, parallelism=1, worker_init=export_environment()), executor_options=dict(address=get_address()))]


def select_executor(job: "ParslJob") -> str:
    return "tiger"


def get_address() -> str:
    return address_by_interface("ib0")  # Nodes can't connect via regular internet name

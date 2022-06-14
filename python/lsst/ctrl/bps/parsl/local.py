from typing import TYPE_CHECKING, List

from lsst.ctrl.bps import BpsConfig
from parsl.executors import ThreadPoolExecutor
from parsl.executors.base import ParslExecutor
from parsl.monitoring.monitoring import MonitoringHub

if TYPE_CHECKING:
    from .job import ParslJob


def get_executors(config: BpsConfig) -> List[ParslExecutor]:
    cores = config.get(".local.cores", 4)
    return [ThreadPoolExecutor("local", max_threads=cores)]


def select_executor(job: ParslJob) -> str:
    return "local"


def get_monitor(config: BpsConfig) -> MonitoringHub:
    pass

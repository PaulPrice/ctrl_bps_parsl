from typing import TYPE_CHECKING, List, Optional, Dict, Any

from lsst.ctrl.bps import BpsConfig
from parsl.executors import HighThroughputExecutor
from parsl.executors.base import ParslExecutor
from parsl.launchers import SrunLauncher
from parsl.monitoring import MonitoringHub
from parsl.providers import SlurmProvider
from parsl.addresses import address_by_hostname, address_by_interface

from .configuration import get_bps_config_value, require_bps_config_value, get_workflow_name
from .environment import export_environment

if TYPE_CHECKING:
    from .job import ParslJob


def make_executor(config: BpsConfig, *, site: str = "slurm", label: str = None, nodes: Optional[int] = None, cores_per_node : Optional[int] = None, walltime: Optional[str] = None, mem_per_node: Optional[int] = None, singleton: bool = False, scheduler_options: Optional[str] = None, provider_options: Dict[str, Any], executor_options: Dict[str, Any]) -> ParslExecutor:
    nodes = require_bps_config_value(config, f".site.{site}.nodes", int, nodes)
    cores_per_node = require_bps_config_value(config, f".site.{site}.cores_per_node", int, cores_per_node)
    walltime = require_bps_config_value(config, f".site.{site}.walltime", str, walltime)
    mem_per_node = get_bps_config_value(config, f".site.{site}.mem_per_node", mem_per_node)

    job_name = get_workflow_name(config)
    if scheduler_options is None:
        scheduler_options = ""
    scheduler_options += f"\n#SBATCH --job-name={job_name}"
    if singleton:
        # The following SBATCH directives allow only a single slurm job (parsl
        # block) with our job_name to run at once. This means we can have one job
        # running, and one already in the queue when the first exceeds the walltime
        # limit. More backups could be achieved with a larger value of max_blocks.
        # This only allows one job to be actively running at once, so that needs
        # to be sized appropriately by the user.
        scheduler_options += "\n#SBATCH --dependency=singleton"

    return HighThroughputExecutor(
            label,
            provider=SlurmProvider(
                nodes_per_block=nodes,
                cores_per_node=cores_per_node,
                mem_per_node=mem_per_node,
                walltime=walltime,
                scheduler_options=scheduler_options,
                **provider_options,
            ),
            **executor_options,
        )


def get_executors(config: BpsConfig) -> List[ParslExecutor]:
    return [make_executor("slurm", config)]


def select_executor(job: "ParslJob") -> str:
    return "slurm"


def get_address() -> str:
    return None  # parsl will try to determine it automagically

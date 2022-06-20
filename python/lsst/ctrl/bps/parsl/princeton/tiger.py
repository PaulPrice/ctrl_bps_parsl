from typing import TYPE_CHECKING, List

from lsst.ctrl.bps import BpsConfig
from parsl.executors import HighThroughputExecutor
from parsl.executors.base import ParslExecutor
from parsl.launchers import SrunLauncher
from parsl.monitoring.monitoring import MonitoringHub
from parsl.providers import SlurmProvider
from parsl.addresses import address_by_hostname, address_by_interface

from ..configuration import get_bps_config_value
from ..environment import export_environment

if TYPE_CHECKING:
    from ..job import ParslJob


def get_executors(config: BpsConfig) -> List[ParslExecutor]:
    nodes = get_bps_config_value(config, ".site.princeton.tiger.nodes", 4)
    cores_per_node = get_bps_config_value(config, ".site.princeton.tiger.cores_per_node", 40)
    walltime = get_bps_config_value(config, ".site.princeton.tiger.walltime", "8:00:00")
    if not isinstance(walltime, str) and not get_bps_config_value(config, ".site.princeton.tiger.allowWalltime", False):
        raise RuntimeError("site.princeton.tiger.walltime is not a string and site.princeton.tiger.allowWalltime=False")
    mem_per_node = get_bps_config_value(config, ".site.princeton.tiger.mem_per_node", 192)
    project = get_bps_config_value(config, "project", "bps")
    campaign = get_bps_config_value(config, "campaign", get_bps_config_value(config, "operator"))
    job_name = f"{project}.{campaign}"
    # The following SBATCH directives allow only a single slurm job (parsl
    # block) with our job_name to run at once. This means we can have one job
    # running, and one already in the queue when the first exceeds the walltime
    # limit. More backups could be achieved with a larger value of max_blocks.
    # This only allows one job to be actively running at once, so that needs
    # to be sized appropriately by the user.
    scheduler_options = "\n".join((
        f"#SBATCH --job-name={job_name}",
        "#SBATCH --dependency=singleton",
    ))
    if not isinstance(mem_per_node, int):
        breakpoint()
    return [
        HighThroughputExecutor(
            "default",
            provider=SlurmProvider(
                partition="tiger-test",
                nodes_per_block=nodes,
                cores_per_node=cores_per_node,
                mem_per_node=mem_per_node,
                walltime=walltime,
                init_blocks=1,
                min_blocks=1,
                max_blocks=2,
                parallelism=1,
                launcher=SrunLauncher(),
                worker_init=export_environment(),
                scheduler_options=scheduler_options,
            ),
            address=address_by_interface("ib0"),  # Nodes can't connect via regular internet name
        ),
    ]


def select_executor(job: "ParslJob") -> str:
    return "default"


def get_monitor(config: BpsConfig) -> MonitoringHub:
    pass

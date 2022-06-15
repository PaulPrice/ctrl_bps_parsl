import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, List

import parsl.config
from lsst.ctrl.bps import BpsConfig
from lsst.daf.butler import Config
from lsst.utils import doImport
from parsl.addresses import address_by_hostname
from parsl.executors.base import ParslExecutor
from parsl.monitoring import MonitoringHub

if TYPE_CHECKING:
    from .job import ParslJob


def get_monitor_filename(out_prefix: str) -> str:
    return os.path.join(out_prefix, "monitor.sqlite")


def get_workflow_filename(out_prefix: str) -> str:
    return os.path.join(out_prefix, "parsl_workflow.pickle")


def set_parsl_logging(config: BpsConfig) -> int:
    """Set parsl logging levels."""
    config = Config(config)  # Workaround BpsConfig returning an empty string for missing keys
    level = config.get(".parsl.log_level", "INFO")
    if level not in ("CRITICAL", "DEBUG", "ERROR", "FATAL", "INFO", "WARN"):
        raise RuntimeError(f"Unrecognised parsl.log_level: {level}")
    level = getattr(logging, level)
    for name in logging.root.manager.loggerDict:
        if name.startswith("parsl"):
            logging.getLogger(name).setLevel(level)
    return level


def get_parsl_config(bpsConfig: BpsConfig, path: str) -> parsl.config.Config:
    config = Config(bpsConfig)  # Workaround BpsConfig returning an empty string for missing keys
    name = config[".parsl.module"]
    if not isinstance(name, str) or not name:
        raise RuntimeError(f"parsl.module ({name}) is not set to a module name")
    site = SiteConfig.from_config(config)
    address = config.get(".parsl.address", None)
    if address is None:
        address = address_by_hostname()
    if False:
        monitor = MonitoringHub(
            hub_address=address,
            resource_monitoring_interval=60,
            logging_endpoint="sqlite://" + os.path.join(path, "monitor.sqlite"),
        )
    else:
        monitor = None
    retries = config.get(".parsl.retries", 0)
    return parsl.config.Config(executors=site.executors, monitoring=monitor, retries=retries)


@dataclass
class SiteConfig:
    executors: List[ParslExecutor]
    select_executor: Callable[["ParslJob"], str]

    @classmethod
    def from_config(cls, config: BpsConfig):
        name = config[".parsl.module"]
        if not isinstance(name, str) or not name:
            raise RuntimeError(f"parsl.module ({name}) is not set to a module name")
        executors = doImport(name + ".get_executors")(config)
        select_executor = doImport(name + ".select_executor")
        # monitor = doImport(name + ".get_monitor")(config)
        return cls(executors, select_executor)

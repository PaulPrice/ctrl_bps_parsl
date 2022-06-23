import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, List, Any, Optional

import parsl.config
from lsst.ctrl.bps import BpsConfig
from lsst.daf.butler import Config
from lsst.utils import doImport
from parsl.addresses import address_by_hostname
from parsl.executors.base import ParslExecutor
from parsl.monitoring import MonitoringHub

if TYPE_CHECKING:
    from .job import ParslJob


@dataclass
class SiteConfig:
    executors: List[ParslExecutor]
    select_executor: Callable[["ParslJob"], str]
    get_address: Callable[[], str]
    add_environment: bool = True

    @classmethod
    def from_config(cls, config: BpsConfig):
        computeSite = get_bps_config_value(config, "computeSite")
        module = f".site.{computeSite}.module"
        name = get_bps_config_value(config, module)
        if not isinstance(name, str) or not name:
            raise RuntimeError(f"{module}={name} is not set to a module name")
        executors = doImport(name + ".get_executors")(config)
        select_executor = doImport(name + ".select_executor")
        get_address = doImport(name + ".get_address")
        add_environment = get_bps_config_value(config, f".site.{computeSite}.environment", True)
        return cls(executors, select_executor, get_address, add_environment)


_NO_DEFAULT = object()


def get_bps_config_value(config: BpsConfig, key: str, default: Any = _NO_DEFAULT):
    """This is how BpsConfig.__getitem__ and BpsConfig.get should behave"""
    options = dict(expandEnvVars=True, replaceVars=True, required=True)
    if default is not _NO_DEFAULT:
        options["default"] = default
    found, value = config.search(key, options)
    if not found and default is _NO_DEFAULT:
        raise KeyError(f"No value found for {key} and no default provided")
    return value


def require_bps_config_value(config: BpsConfig, name: str, dataType: type, default: Optional[Any] = None):
    value = get_bps_config_value(config, name, default)
    if value is None:
        raise RuntimeError(f"Configuration value {name} must be set")
    if not isinstance(value, dataType):
        raise RuntimeError(f"Configuration value {name}={value} is not of type {dataType}")
    return value


def get_workflow_name(config: BpsConfig) -> str:
    project = get_bps_config_value(config, "project", "bps")
    campaign = get_bps_config_value(config, "campaign", get_bps_config_value(config, "operator"))
    return f"{project}.{campaign}"


def get_workflow_filename(out_prefix: str) -> str:
    return os.path.join(out_prefix, "parsl_workflow.pickle")


def set_parsl_logging(config: BpsConfig) -> int:
    """Set parsl logging levels."""
    level = get_bps_config_value(config, ".parsl.log_level", "INFO")
    if level not in ("CRITICAL", "DEBUG", "ERROR", "FATAL", "INFO", "WARN"):
        raise RuntimeError(f"Unrecognised parsl.log_level: {level}")
    level = getattr(logging, level)
    for name in logging.root.manager.loggerDict:
        if name.startswith("parsl"):
                logging.getLogger(name).setLevel(level)
    return level


def get_parsl_config(config: BpsConfig, path: str) -> parsl.config.Config:
    computeSite = get_bps_config_value(config, "computeSite")
    module = f".site.{computeSite}.module"
    name = get_bps_config_value(config, module)
    if not isinstance(name, str) or not name:
        raise RuntimeError(f"{module}={name} is not set to a module name")
    site = SiteConfig.from_config(config)
    retries = get_bps_config_value(config, ".parsl.retries", 1)
    monitor = get_parsl_monitor(config, site)
    return parsl.config.Config(executors=site.executors, monitoring=monitor, retries=retries, checkpoint_mode="task_exit")


def get_parsl_monitor(config: BpsConfig, site: Optional[SiteConfig] = None) -> Optional[MonitoringHub]:
    if not get_bps_config_value(config, ".parsl.monitor.enable", False):
        return None
    if site is None:
        site = SiteConfig.from_config(config)
    return MonitoringHub(
        workflow_name=get_workflow_name(config),
        hub_address=site.get_address(),
        resource_monitoring_interval=get_bps_config_value(config, ".parsl.monitor.interval", 30),
        logging_endpoint="sqlite:///" + get_bps_config_value(config, ".parsl.monitor.filename", "monitor.sqlite"),
    )

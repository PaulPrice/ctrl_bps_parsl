from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Optional

from lsst.ctrl.bps import BpsConfig
from lsst.utils import doImport
from parsl.addresses import address_by_hostname
from parsl.executors.base import ParslExecutor
from parsl.monitoring import MonitoringHub

from .configuration import get_workflow_name, get_bps_config_value
from .environment import export_environment

if TYPE_CHECKING:
    from .job import ParslJob

__all__ = ("SiteConfig",)


class SiteConfig(ABC):
    """Base class for site configuration

    Subclasses need to override at least the ``get_executors`` and
    ``select_executor`` methods.

    Parameters
    ----------
    config : `BpsConfig`
        BPS configuration.
    """

    def __init__(self, config: BpsConfig):
        self.config = config
        self.site = self.get_site_subconfig(config)

    @staticmethod
    def get_site_subconfig(config: BpsConfig) -> BpsConfig:
        """Get BPS configuration for the site of interest

        We return the BPS sub-configuration for the site indicated by the
        ``computeSite`` value, which is ``site.<computeSite>``.

        Parameters
        ----------
        config : `BpsConfig`
            BPS configuration.

        Returns
        -------
        site : `BpsConfig`
            Site sub-configuration.
        """
        computeSite = get_bps_config_value(config, "computeSite", str, required=True)
        return get_bps_config_value(config, f".site.{computeSite}", BpsConfig, required=True)

    @classmethod
    def from_config(cls, config: BpsConfig) -> "SiteConfig":
        """Get the site configuration nominated in the BPS config

        The ``computeSite`` (`str`) value in the BPS configuration is used to
        select a site configuration. The site configuration class to use is
        specified by the BPS configuration as ``site.<computeSite>.class``
        (`str`), which should be the fully-qualified name of a python class that
        inherits from `SiteConfig`.

        Parameters
        ----------
        config : `BpsConfig`
            BPS configuration.

        Returns
        -------
        site_config : subclass of `SiteConfig`
            Site configuration.
        """
        site = cls.get_site_subconfig(config)
        name = get_bps_config_value(site, "class", str, required=True)
        return doImport(name)(config)

    @abstractmethod
    def get_executors(self) -> List[ParslExecutor]:
        """Get a list of executors to be used in processing

        Each executor should have a unique ``label``.
        """
        raise NotImplementedError("Subclasses must define")

    @abstractmethod
    def select_executor(self, job: "ParslJob") -> str:
        """Get the ``label`` of the executor to use to execute a job

        Parameters
        ----------
        job : `ParslJob`
            Job to be executed.

        Returns
        -------
        label : `str`
            Label of executor to use to execute ``job``.
        """
        raise NotImplementedError("Subclasses must define")

    def get_address(self) -> str:
        """Return the IP address of the machine hosting the driver/submission

        This address should be accessible from the workers. This should
        generally by the return value of one of the functions in
        ``parsl.addresses``.

        This is used by the default implementation of ``get_monitor``, but will
        generally be used by ``get_executors`` too.

        This default implementation gets the address from the hostname, but that
        will not work if the workers don't access the driver/submission node by
        that address.
        """
        return address_by_hostname()

    def get_command_prefix(self) -> str:
        """Return command(s) to add before each job command

        These may be used to configure the environment for the job.

        This default implementation respects the BPS configuration elements:

        - ``site.<computeSite>.commandPrefix`` (`str`): command(s) to use as a
          prefix to executing a job command on a worker.
        - ``site.<computeSite>.environment`` (`bool`): add bash commands that
          replicate the environment on the driver/submit machine?
        """
        prefix = get_bps_config_value(self.site, "commandPrefix", str, "")
        if get_bps_config_value(self.site, "environment", bool, False):
            prefix += "\n" + export_environment()
        return prefix

    def get_monitor(self) -> Optional[MonitoringHub]:
        """Get parsl monitor

        The parsl monitor provides a database that tracks the progress of the
        workflow and the use of resources on the workers.

        This implementation respects the BPS configuration elements:

        - ``site.<computeSite>.monitorEnable`` (`bool`): enable monitor?
        - ``site.<computeSite>.monitorInterval`` (`float`): time interval (sec)
          between logging of resource usage.
        - ``site.<computeSite>.monitorFilename`` (`str`): name of file to use
          for the monitor sqlite database.

        Returns
        -------
        monitor : `MonitoringHub` or `None`
            Parsl monitor, or `None` for no monitor.
        """
        if not get_bps_config_value(self.site, "monitorEnable", bool, False):
            return None
        return MonitoringHub(
            workflow_name=get_workflow_name(self.config),
            hub_address=self.get_address(),
            resource_monitoring_interval=get_bps_config_value(self.site, "monitorInterval", float, 30),
            logging_endpoint="sqlite:///"
            + get_bps_config_value(self.site, "monitorFilename", str, "monitor.sqlite"),
        )

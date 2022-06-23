import os
import pickle
import logging
from typing import Dict, Mapping, Optional, Sequence, Iterable

import parsl
from lsst.ctrl.bps import BaseWmsWorkflow, BpsConfig, GenericWorkflow, GenericWorkflowJob
from parsl.app.app import bash_app
from parsl.app.futures import Future

from .configuration import SiteConfig, get_parsl_config, get_workflow_filename, set_parsl_logging, get_bps_config_value
from .job import ParslJob, get_file_paths
from .environment import export_environment

__all__ = ("ParslWorkflow",)

_env = export_environment()

_log = logging.getLogger("lsst.ctrl.bps.parsl")


class ParslWorkflow(BaseWmsWorkflow):
    """Parsl-based workflow object to manage execution of workflow.

    Parameters
    ----------
    name : `str`
        Unique name of workflow.
    config : `lsst.ctrl.bps.BpsConfig`
        Generic workflow config.
    """

    def __init__(self, name: str, config: BpsConfig, path: str, tasks: Dict[str, ParslJob], parents: Mapping[str, Iterable[str]], endpoints: Iterable[str], final: Optional[ParslJob] = None):
        super().__init__(name, config)
        self.path = path

        self.bps_config = config
        self.parsl_config = get_parsl_config(config, path)
        self.site_config = SiteConfig.from_config(config)
        self.dfk: Optional[parsl.DataFlowKernel] = None  # type: ignore

        self.command_prefix = export_environment() if self.site_config.add_environment else ""

        # these are function decorators
        self.apps = {
            ex.label: bash_app(executors=[ex.label], cache=True, ignore_for_cache=["stderr", "stdout"])
            for ex in self.site_config.executors
        }

        self.tasks = tasks
        self.parents = parents
        self.endpoints = endpoints
        self.final = final

    def __reduce__(self):
        return type(self), (self.name, self.bps_config, self.path, self.tasks, self.parents, self.endpoints, self.final)

    @classmethod
    def from_generic_workflow(
        cls, config: BpsConfig, generic_workflow: GenericWorkflow, out_prefix: str, service_class: str
    ) -> BaseWmsWorkflow:
        """
        Create a ParslWorkflow object from a generic_workflow.

        Parameters
        ----------
        config: `lss.ctrl.bps.BpsConfig`
            Configuration of the workflow.
        generic_workflow: `lsst.ctrl.bps.generic_workflow.GenericWorkflow`
            Generic representation of a single workflow.
        out_prefix: `str`
            Prefix for WMS output files.
        service_class: `str`
            Full module name of WMS service class that created this workflow.

        Returns
        -------
        ParslWorkflow
        """
        # Generate list of tasks
        tasks: Dict[str, ParslJob] = {}
        for job_name in generic_workflow:
            job = generic_workflow.get_job(job_name)
            assert job.name not in tasks
            tasks[job_name] = ParslJob(job, config, get_file_paths(generic_workflow, job_name))

        parents = {name: set(generic_workflow.predecessors(name)) for name in tasks}
        endpoints = [name for name in tasks if generic_workflow.out_degree(name) == 0]

        # Add final job: execution butler merge (if whenMerge == ALWAYS)
        job = generic_workflow.get_final()
        final: Optional[ParslJob] = None
        if job is not None:
            assert isinstance(job, GenericWorkflowJob)
            final = ParslJob(job, config, get_file_paths(generic_workflow, job.name))

        return cls(generic_workflow.name, config, out_prefix, tasks, parents, endpoints, final)

    def write(self, out_prefix: str):
        """Write WMS files for this particular workflow.

        Parameters
        ----------
        out_prefix : `str`
            Root directory to be used for WMS workflow inputs and outputs
            as well as internal WMS files.
        """
        filename = get_workflow_filename(out_prefix)
        _log.info("Writing workflow with ID=%s", out_prefix)
        with open(filename, "wb") as fd:
            pickle.dump(self, fd)

    @classmethod
    def read(cls, out_prefix: str) -> "ParslWorkflow":
        filename = get_workflow_filename(out_prefix)
        with open(filename, "rb") as fd:
            self = pickle.load(fd)
        assert isinstance(self, cls)
        return self

    def run(self, block: bool = True):
        futures = [self.execute(name) for name in self.endpoints]
        if block:
            # Calling .exception() for each future blocks returning
            # from this method until all the jobs have executed or
            # raised an error.  This is needed for running in a
            # non-interactive python process that would otherwise end
            # before the futures resolve.
            for ff in futures:
                if ff is not None:
                    ff.exception()
            self.finalize_jobs()
            self.shutdown()

    def execute(self, name: str) -> Optional[parsl.app.futures.Future]:  # type: ignore
        if name in ("pipetaskInit", "mergeExecutionButler"):
            # These get done outside of parsl
            return None
        job = self.tasks[name]
        inputs = [self.execute(parent) for parent in self.parents[name]]
        if len(self.site_config.executors) > 1:
            label = self.site_config.select_executor(job)
        else:
            label = self.site_config.executors[0].label
        return job.get_future(self.apps[label], [ff for ff in inputs if ff is not None], self.command_prefix)

    def load_dfk(self):
        if self.dfk is not None:
            raise RuntimeError("Workflow has already started.")
        set_parsl_logging(self.bps_config)
        self.dfk = parsl.load(self.parsl_config)

    def start(self):
        self.initialize_jobs()
        self.load_dfk()

    def restart(self):
        self.parsl_config.checkpoint_files = parsl.utils.get_last_checkpoint()
        self.load_dfk()

    def shutdown(self):
        """Shutdown and dispose of the Parsl DataFlowKernel.  This will stop
        the monitoring and enable a new workflow to be created in the
        same python session.  No further jobs can be run from this
        workflow object once the DFK has been shutdown.
        `ParslGraph.restore(...)` can be used to restart a workflow with
        a new DFK.
        """
        if self.dfk is None:
            raise RuntimeError("Workflow not started.")
        self.dfk.cleanup()
        self.dfk = None
        parsl.DataFlowKernelLoader.clear()

    def initialize_jobs(self):
        job = self.tasks.get("pipetaskInit", None)
        if job is not None:
            os.makedirs(os.path.join(get_bps_config_value(self.bps_config, "submitPath"), "logs"))
            job.run_local()

    def finalize_jobs(self):
        """Run final job to transfer datasets from the execution butler to
        the destination repo butler, and remove the temporaries"""
        if self.final is not None and not self.final.done:
            self.final.run_local()

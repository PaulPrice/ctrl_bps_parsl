import os
import pickle
from typing import Dict, Optional, Sequence

import parsl
from lsst.ctrl.bps import BaseWmsWorkflow, BpsConfig, GenericWorkflow
from parsl.app.app import bash_app
from parsl.app.futures import Future

from .configuration import SiteConfig, get_parsl_config, get_workflow_filename, set_parsl_logging
from .job import ParslJob

__all__ = ("ParslWorkflow",)


def exportEnv():
    """Generate bash script to regenerate the current environment"""
    output = ""
    for key, val in os.environ.items():
        if key in ("DISPLAY",):
            continue
        if val.startswith("() {"):
            # This is a function.
            # "Two parentheses, a single space, and a brace"
            # is exactly the same criterion as bash uses.

            # From 2014-09-25, the function name is prefixed by 'BASH_FUNC_'
            # and suffixed by '()', which we have to remove.
            if key.startswith("BASH_FUNC_") and key.endswith("()"):
                key = key[10:-2]

            output += "{key} {val}\nexport -f {key}\n".format(key=key, val=val)
        else:
            # This is a variable.
            output += "export {key}='{val}'\n".format(key=key, val=val.replace("'", "'\"'\"'"))
    return output


_env = exportEnv()


def run_command(command_line: str, inputs: Sequence[Future] = (), stdout: Optional[str] = None, stderr: Optional[str] = None) -> str:
    return _env + command_line


class ParslWorkflow(BaseWmsWorkflow):
    """Parsl-based workflow object to manage execution of workflow.

    Parameters
    ----------
    name : `str`
        Unique name of workflow.
    config : `lsst.ctrl.bps.BpsConfig`
        Generic workflow config.
    """

    def __init__(self, name: str, config: BpsConfig, path: str, tasks: Dict[str, ParslJob]):
        super().__init__(name, config)
        self.path = path

        self.bps_config = config
        self.parsl_config = get_parsl_config(config, path)
        self.site_config = SiteConfig.from_config(config)
        self.dfk: Optional[parsl.DataFlowKernel] = None  # type: ignore

        self.apps = {
            ex.label: bash_app(executors=[ex.label], cache=True, ignore_for_cache=["stderr", "stdout"])(run_command)
            for ex in self.site_config.executors
        }

        self.tasks = tasks

    def __reduce__(self):
        return type(self), (self.name, self.bps_config, self.path, self.tasks)

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
            file_paths = {ff.name: ff.src_uri for ff in generic_workflow.get_job_inputs(job_name)}
            tasks[job_name] = ParslJob(job, config, file_paths)

        # Add dependencies
        for job_name in tasks:
            parent = tasks[job_name]
            for child in generic_workflow.successors(job_name):
                parent.add_child(tasks[child])

        return cls(generic_workflow.name, config, out_prefix, {job.name: job for job in tasks.values()})

    def write(self, out_prefix: str):
        """Write WMS files for this particular workflow.

        Parameters
        ----------
        out_prefix : `str`
            Root directory to be used for WMS workflow inputs and outputs
            as well as internal WMS files.
        """
        return  # XXX current implementation fails with "maximum recursion depth exceeded"
        filename = get_workflow_filename(out_prefix)
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
        self.start()
        endpoints = []
        for job in self.tasks.values():
            if job.children or job.done:
                continue
            future = self.execute(job)
            if future is not None:
                endpoints.append(future)

        if block:
            # Calling .exception() for each future blocks returning
            # from this method until all the jobs have executed or
            # raised an error.  This is needed for running in a
            # non-interactive python process that would otherwise end
            # before the futures resolve.
            for future in endpoints:
                future.exception()
        self.shutdown()

    def execute(self, job: ParslJob) -> Optional[parsl.app.futures.Future]:  # type: ignore
        inputs = [self.execute(parent) for parent in job.parents]
        label = self.site_config.select_executor(job)
        return job.get_future(self.apps[label], inputs)

    def start(self):
        if self.dfk is not None:
            raise RuntimeError("Workflow has already started.")
        set_parsl_logging(self.bps_config)
        self.dfk = parsl.load(self.parsl_config)

    def shutdown(self):
        """Shutdown and dispose of the Parsl DataFlowKernel.  This will stop
        the monitoring and enable a new workflow to be created in the
        same python session.  No further jobs can be run from this
        `ParslGraph` object once the DFK has been shutdown.
        `ParslGraph.restore(...)` can be used to restart a workflow with
        a new DFK.
        """
        if self.dfk is None:
            raise RuntimeError("Workflow not started.")
        self.dfk.cleanup()
        self.dfk = None
        parsl.DataFlowKernelLoader.clear()

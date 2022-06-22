import re
import enum
import os
import subprocess
from typing import List, Optional, Callable, Sequence, Dict

from lsst.ctrl.bps import BpsConfig, GenericWorkflow, GenericWorkflowJob
from parsl.app.futures import Future

from .configuration import get_bps_config_value

__all__ = ("JobStatus", "get_file_paths", "ParslJob")

_env_regex = re.compile(r'<ENV:(\S+)>')
_file_regex = re.compile(r'<FILE:(\S+)>')


# Job status values
class JobStatus(enum.Enum):
    """Status of a job"""
    UNKNOWN = 0
    PENDING = 1
    SCHEDULED = 2
    RUNNING = 3
    SUCCEEDED = 4
    FAILED = 5


def get_file_paths(workflow: GenericWorkflow, name: str) -> Dict[str, str]:
    return {ff.name: ff.src_uri for ff in workflow.get_job_inputs(name)}


class ParslJob:
    def __init__(
        self,
        generic: GenericWorkflowJob,
        config: BpsConfig,
        file_paths: Dict[str, str],
    ):
        self.generic = generic
        self.name = generic.label
        self.config = config
        self.file_paths = file_paths
        self.done = False
        log_dir = os.path.join(get_bps_config_value(self.config, "submitPath"), "logs")
        self.stdout = os.path.join(log_dir, self.name + ".stdout")
        self.stderr = os.path.join(log_dir, self.name + ".stderr")

    def __reduce__(self):
        return type(self), (self.generic, self.config, self.file_paths)

    def get_command_line(self):
        """Get the Bash command-line to run

        We're not considering use of the execution butler here, so there's
        no staging to do.
        """
        command = [
            self.generic.executable.src_uri,
            self.generic.arguments,
            # f" && touch {self._succeeded} || (touch {self._failed}; false)",  # Leave a permanent record
        ]
        prefix = get_bps_config_value(self.config, "commandPrepend", "")
        if prefix:
            command.insert(0, prefix)

        return " ".join(command)

    def evaluate_command_line(self, command):
        """
        Evaluate command line, replacing bps variables, fixing env vars,
        and inserting job-specific file paths, all assuming that
        everything is running on a shared file system.
        """
        command = command.format(**self.generic.cmdvals)  # BPS variables
        command = re.sub(_env_regex, r'${\g<1>}', command)  # Environment variables
        command = re.sub(_file_regex, lambda match: self.file_paths[match.group(1)], command)  # Files
        return command

    # @property
    # def done(self):
    #     """
    #     Execution state of the job based on status in monintoring.db
    #     or whether the job has written the 'success' string to the end
    #     of its log file.
    #     """
    #     if self._done:
    #         return True
    #     if self.status == JobStatus.SUCCEEDED:
    #         self._done = True
    #     if self.future is None:
    #         return False
    #     return self._done

    # @property
    # def succeeded(self):
    #     return os.path.exists(self._succeeded)

    # @property
    # def failed(self):
    #     return os.path.exists(self._failed)

    # def check_status(self):
    #     if not self.future:
    #         if self.succeeded:
    #             return JobStatus.SUCCEEDED
    #         if self.failed:
    #             return JobStatus.FAILED
    #         return JobStatus.PENDING
    #     if self.future.done():
    #         return JobStatus.SUCCEEDED if self.future.exception() is None else JobStatus.FAILED
    #     return JobStatus.RUNNING if self.future.running() else JobStatus.SCHEDULED

    # @property
    # def status(self):
    #     """Return the job status"""
    #     if self._status in (JobStatus.SUCCEEDED, JobStatus.FAILED):  # Once set, these don't change
    #         return self._status
    #     self._status = self.check_status()
    #     return self._status

    def get_future(self, app: Callable[[str, Sequence[Future], Optional[str], Optional[str]], Future], inputs: List[Future]) -> Optional[Future]:
        """
        Get the parsl app future for the job to be run.
        """
        if self.done:
            return None  # Nothing to do
        command = self.get_command_line()
        command = self.evaluate_command_line(command)
        return app(command, inputs=inputs, stdout=self.stdout, stderr=self.stderr)

    def run_local(self):
        if self.done:  # Nothing to do
            return
        command = self.get_command_line()
        command = self.evaluate_command_line(command)
        with open(self.stdout, "w") as stdout, open(self.stderr, "w") as stderr:
            subprocess.check_call(command, shell=True, executable="/bin/bash", stdout=stdout, stderr=stderr)
        self.done = True

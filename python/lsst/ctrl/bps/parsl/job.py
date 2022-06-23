import re
import enum
import os
import subprocess
from typing import List, Optional, Callable, Sequence, Dict
from functools import partial

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


def run_command(command_line: str, inputs: Sequence[Future] = (), stdout: Optional[str] = None, stderr: Optional[str] = None) -> str:
    return command_line


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
        self.name = generic.name
        self.config = config
        self.file_paths = file_paths
        self.future = None
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

    def get_future(self, app: Callable[[str, Sequence[Future], Optional[str], Optional[str]], Future], inputs: List[Future], command_prefix: Optional[str] = None) -> Optional[Future]:
        """
        Get the parsl app future for the job to be run.
        """
        if self.done:
            return None  # Nothing to do
        if not self.future:
            command = self.get_command_line()
            command = self.evaluate_command_line(command)
            if command_prefix:
                command = command_prefix + "\n" + command

            # Add a layer of indirection to which we can add a useful name
            func = partial(run_command)
            func.__name__ = self.generic.label

            self.future = app(func)(command, inputs=inputs, stdout=self.stdout, stderr=self.stderr)
        return self.future

    def run_local(self):
        if self.done:  # Nothing to do
            return
        command = self.get_command_line()
        command = self.evaluate_command_line(command)
        with open(self.stdout, "w") as stdout, open(self.stderr, "w") as stderr:
            subprocess.check_call(command, shell=True, executable="/bin/bash", stdout=stdout, stderr=stderr)
        self.done = True

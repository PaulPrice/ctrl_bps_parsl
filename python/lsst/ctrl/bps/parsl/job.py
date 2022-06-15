import re
import enum
import os
from typing import List, Optional, Set, Callable, Sequence, Dict

from lsst.ctrl.bps import BpsConfig, GenericWorkflowJob
from parsl.app.bash import BashApp
from parsl.app.futures import Future

__all__ = ("JobStatus", "ParslJob")

_env_regex = re.compile(r'<ENV:(\S+)>')
_file_regex = re.compile(r'<FILE:(\S+)>')


def is_uuid(value):
    """Check if the passed value is formatted like a UUID."""
    sizes = tuple([len(_) for _ in value.split("-")])
    return sizes == (8, 4, 4, 4, 12)


def get_group_name(bps_job_name: str, config: Optional[BpsConfig] = None):
    """Extract a group name from the GenericWorkflowJob name."""
    # Get cluster names from any quantum clustering specification
    # in the bps config yaml.
    tokens = bps_job_name.split("_")
    if config is not None:
        cluster_names = list(config["cluster"].keys())
        if tokens[0] in cluster_names:
            # In case of quantum clustering, we use the cluster name as
            # the task name.
            return tokens[0]
    # If config is None or if the tokens[0] is not in
    # cluster_names, then check if it is formatted like a uuid,
    # in which case tokens[1] is the task name.
    if is_uuid(tokens[0]):
        return tokens[1]
    # Finally, for backwards compatibility with weeklies prior to
    # w_2022_01, check if tokens[0] can be cast as an int.  If not,
    # then it's the cluster name.
    try:
        _ = int(tokens[0])
    except ValueError:
        return tokens[0]
    return tokens[1]


# Job status values
class JobStatus(enum.Enum):
    """Status of a job"""
    UNKNOWN = 0
    PENDING = 1
    SCHEDULED = 2
    RUNNING = 3
    SUCCEEDED = 4
    FAILED = 5


class ParslJob:
    def __init__(
        self,
        generic: GenericWorkflowJob,
        config: BpsConfig,
        file_paths: Dict[str, str],
        parents: Optional[Set["ParslJob"]] = None,
        children: Optional[Set["ParslJob"]] = None,
    ):
        self.generic = generic
        self.name = generic.name
        self.group = get_group_name(self.name, config)
        self.config = config
        self.file_paths = file_paths
        self.parents: Set["ParslJob"] = set()
        self.children: Set["ParslJob"] = set()
        self._done: bool = False
        self._status = JobStatus.UNKNOWN
        self.future: Future = None
        log_dir = os.path.join(self.config["submitPath"], "logs")
        self.stdout = os.path.join(log_dir, self.name + ".stdout")
        self.stderr = os.path.join(log_dir, self.name + ".stderr")
        self._succeeded = os.path.join(log_dir, self.name + ".succeeded")
        self._failed = os.path.join(log_dir, self.name + ".failed")

    def __reduce__(self):
        return type(self), (self.generic, self.config, self.file_paths, self.parents, self.children)

    def get_command_line(self):
        """Get the Bash command-line to run

        We're not considering use of the execution butler here, so there's
        no staging to do.
        """
        assert not self.generic.executable.transfer_executable  # Execution butler disabled; no staging
        command = [
            self.generic.executable.src_uri,
            self.generic.arguments,
            f" && touch {self._succeeded} || (touch {self._failed}; false)",  # Leave a permanent record
        ]
        prefix = self.config.get("commandPrepend", "")
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

    def add_child(self, child: "ParslJob"):
        """
        Add a job child based on the workflow DAG.

        Parameters
        ----------
        parent: `ParslJob`
        """
        self.children.add(child)
        child.parents.add(self)

    @property
    def done(self):
        """
        Execution state of the job based on status in monintoring.db
        or whether the job has written the 'success' string to the end
        of its log file.
        """
        if self._done:
            return True
        if self.future is None:
            return False
        if self.status == JobStatus.SUCCEEDED:
            self._done = True
        return self._done

    @property
    def succeeded(self):
        return os.path.exists(self._succeeded)

    @property
    def failed(self):
        return os.path.exists(self._failed)

    def check_status(self):
        if not self.future:
            if self.succeeded:
                return JobStatus.SUCCEEDED
            if self.failed:
                return JobStatus.FAILED
            return JobStatus.PENDING
        if self.future.done():
            return JobStatus.SUCCEEDED if self.future.exception() is None else JobStatus.FAILED
        return JobStatus.RUNNING if self.future.running() else JobStatus.SCHEDULED

    @property
    def status(self):
        """Return the job status"""
        if self._status in (JobStatus.SUCCEEDED, JobStatus.FAILED):  # Once set, these don't change
            return self._status
        self._status = self.check_status()
        return self._status

    def get_future(self, app: Callable[[str, Sequence[Future], Optional[str], Optional[str]], Future], inputs: List[Future]) -> Optional[Future]:
        """
        Get the parsl app future for the job to be run.
        """
        if self.succeeded:  # From a previous attempt
            return None
        if self.future:
            return self.future
        if self.failed:  # From a previous attempt
            os.remove(self._failed)
        command = self.get_command_line()
        command = self.evaluate_command_line(command)
        self.future = app(command, inputs=inputs, stdout=self.stdout, stderr=self.stderr)
        return self.future

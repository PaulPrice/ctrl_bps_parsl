import os
import shutil

from lsst.ctrl.bps.drivers import transform_driver
from lsst.ctrl.bps.prepare import prepare

from .config import load_parsl_config
from .constants import PARSL_GRAPH_CONFIG


def start_pipeline(config_file: str, outfile: str = None, mode: str = 'symlink'):
    """
    Function to submit a pipeline job using ctrl_bps and the
    Parsl-based plugin.  The returned ParslGraph object provides
    access to the underlying GenericWorkflowJob objects and
    piplinetask quanta and a means of controlling and introspecting
    their execution.

    This is intended for use from the python command-line, and not with BPS
    proper.

    Parameters
    ----------
    config_file: str
        ctrl_bps yaml config file.
    outfile: str [None]
        Local link or copy of the file containing the as-run configuration
        for restarting pipelines.  The master copy is written to
        `{submitPath}/parsl_graph_config.pickle`.
    mode: str ['symlink']
        Mode for creating local copy of the file containing the
        as-run configuration.  If not 'symlink', then make a copy.

    Returns
    -------
    ParslGraph
    """
    if outfile is not None and os.path.isfile(outfile):
        raise FileExistsError(f"File exists: '{outfile}'")
    config, generic_workflow = transform_driver(config_file)
    submit_path = config['submitPath']
    workflow = prepare(config, generic_workflow, submit_path)
    as_run_config = os.path.join(submit_path, PARSL_GRAPH_CONFIG)
    workflow.parsl_graph.dfk = load_parsl_config(config)
    workflow.parsl_graph.save_config(as_run_config)
    if outfile is not None:
        if mode == 'symlink':
            os.symlink(as_run_config, outfile)
        else:
            shutil.copy(as_run_config, outfile)
    return workflow.parsl_graph

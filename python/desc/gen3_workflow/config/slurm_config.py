import parsl
from parsl.config import Config
from parsl.channels import LocalChannel
from parsl.providers import SlurmProvider
from parsl.providers import GridEngineProvider
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_query
from parsl.executors.threads import ThreadPoolExecutor
from parsl.monitoring.monitoring import MonitoringHub

from parsl.dataflow.rundirs import make_rundir

run_dir='/sps/lsst/users/leboulch/bps/runinfo/'
subrun_dir = make_rundir(run_dir)
logging_endpoint = "sqlite:///" + subrun_dir + "/monitoring.db"

#initenv='source /pbs/home/l/leboulch/LSST/parsl/env_v23.0.1.rc3.sh'

batch_3G = HighThroughputExecutor(
              label='batch-3G',
              address=address_by_query(),
              worker_debug=False,
              poll_period=1000,
              worker_port_range=(54000, 54050),
              interchange_port_range=(54051, 54100),
              max_workers=1,
              provider=SlurmProvider(
                partition='htc',
                channel=LocalChannel(),
                account='lsst',
                exclusive=False,
                nodes_per_block=1,
                init_blocks=0,
                max_blocks=2500,
                walltime="144:00:00",
                scheduler_options='#SBATCH --mem 3G -L sps',
#                worker_init=initenv,     # Input your worker_init if needed
                ),
              )

batch_9G = HighThroughputExecutor(
              label='batch-9G',
              address=address_by_query(),
              worker_debug=False,
              poll_period=1000,
              worker_port_range=(54000, 54050),
              interchange_port_range=(54051, 54100),
              max_workers=1,
              provider=SlurmProvider(
                partition='htc',
                channel=LocalChannel(),
                account='lsst',
                exclusive=False,
                nodes_per_block=1,
                init_blocks=0,
                max_blocks=1500,
                walltime="144:00:00",
                scheduler_options='#SBATCH --mem 9G -L sps',
#                worker_init=initenv,     # Input your worker_init if needed
                ),
              )

batch_18G = HighThroughputExecutor(
              label='batch-18G',
              address=address_by_query(),
              worker_debug=False,
              poll_period=1000,
              worker_port_range=(54000, 54050),
              interchange_port_range=(54051, 54100),
              max_workers=1,
              provider=SlurmProvider(
                partition='htc',
                channel=LocalChannel(),
                account='lsst',
                exclusive=False,
                nodes_per_block=1,
                init_blocks=0,
                max_blocks=1000,
                walltime="144:00:00",
                scheduler_options='#SBATCH --mem 18G -L sps',
#                worker_init=initenv,     # Input your worker_init if needed
                ),
              )

batch_48G = HighThroughputExecutor(
              label='batch-48G',
              address=address_by_query(),
              worker_debug=False,
              poll_period=1000,
              worker_port_range=(54000, 54050),
              interchange_port_range=(54051, 54100),
              max_workers=1,
              provider=SlurmProvider(
                partition='htc',
                channel=LocalChannel(),
                account='lsst',
                exclusive=False,
                nodes_per_block=1,
                init_blocks=0,
                max_blocks=500,
                walltime="144:00:00",
                scheduler_options='#SBATCH --mem 54G -L sps',
#                worker_init=initenv,     # Input your worker_init if needed
                ),
              )

#batch_120G = HighThroughputExecutor(
#              label='batch-120G',
#              address=address_by_query(),
#              worker_debug=False,
#              poll_period=1000,
#              worker_port_range=(54000, 54050),
#              interchange_port_range=(54051, 54100),
#              max_workers=1,
#              provider=SlurmProvider(
#                partition='htc',
#                channel=LocalChannel(),
#                account='lsst',
#                exclusive=False,
#                nodes_per_block=1,
#                init_blocks=0,
#                max_blocks=100,
#                walltime="144:00:00",
##                scheduler_options='#SBATCH --mem 120G -L sps',
#                scheduler_options='#SBATCH --mem 170G -L sps',
#                worker_init=initenv,     # Input your worker_init if needed
#                ),
#              )


batch_120G = HighThroughputExecutor(
              label='batch-120G',
              address=address_by_query(),
              worker_debug=False,
              poll_period=1000,
              worker_port_range=(54000, 54050),
              interchange_port_range=(54051, 54100),
              max_workers=1,
              provider=GridEngineProvider(
                channel=LocalChannel(),
                nodes_per_block=1,
                init_blocks=0,
                max_blocks=50,
                walltime="144:00:00",
                scheduler_options='#$ -P P_lsst -l cvmfs=1,sps=1 -pe multicores 1 -q mc_highmem_huge',
#                worker_init=initenv,     # Input your worker_init if needed
                ),
              )



local_executor = ThreadPoolExecutor(max_threads=16, label="submit-node")

monitor = MonitoringHub(
       hub_address=address_by_query(),
       hub_port=54501,
       logging_endpoint=logging_endpoint,
       monitoring_debug=False,
#       resource_monitoring_enabled=True,
#       resource_monitoring_interval=30,
   )


config = parsl.config.Config(executors=[batch_3G, batch_9G, batch_18G, batch_48G, batch_120G, local_executor],
                             app_cache=True,
                             retries=2,
                             strategy='htex_auto_scale',
                             run_dir=subrun_dir,
                             monitoring=monitor)

DFK = parsl.load(config)


from parsl.config import Config
from parsl.monitoring.monitoring import MonitoringHub
from parsl.providers import CondorProvider
from parsl.providers import LocalProvider
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_hostname
import yaml


with open("config.yml") as _file:
    phz_config = yaml.load(_file, Loader=yaml.FullLoader)

phz_root_dir = phz_config.get("phz_root_dir")
test_env = phz_config.get("test_environment", {})

executors = {
    "htcondor": HighThroughputExecutor(
        label='htcondor',
        address=address_by_hostname(),
        max_workers=3,
        provider=CondorProvider(
            init_blocks=2,
            min_blocks=2,
            max_blocks=5,
            parallelism=0.5,
            scheduler_options='+RequiresWholeMachine = True',
            worker_init=f"source {phz_root_dir}/env.sh",
            cmd_timeout=120,
        ),
    ),
    "local": HighThroughputExecutor(
        label='local',
        provider=LocalProvider(
            min_blocks=1,
            init_blocks=1,
            max_blocks=2,
            nodes_per_block=1,
            parallelism=0.5
        )
    )
}

executor_key = phz_config.get("executor", "local")
executor = executors[executor_key]

config = Config(
    executors=[executor],
    run_dir='/lustre/t0/tmp/scratch/',
    monitoring=MonitoringHub(
       hub_address=address_by_hostname(),
       hub_port=55055,
       monitoring_debug=False,
       logging_endpoint=f"sqlite:///{phz_root_dir}/phz.db",
       resource_monitoring_interval=10,
   ),
   strategy=None
)

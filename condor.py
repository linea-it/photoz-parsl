from parsl.config import Config
from parsl.monitoring.monitoring import MonitoringHub
from parsl.providers import CondorProvider
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_hostname
import yaml


with open("config.yml") as _file:
    phz_config = yaml.load(_file, Loader=yaml.FullLoader)
    phz_root_dir = phz_config.get("phz_root_dir")

config = Config(
    executors=[
        HighThroughputExecutor(
            label='cluster',
            address=address_by_hostname(),
            max_workers=100,
            cores_per_worker=1.2,
            provider=CondorProvider(
                init_blocks=2,
                min_blocks=2,
                max_blocks=3,
                parallelism=1,
                scheduler_options='+RequiresWholeMachine = True',
                worker_init=f"source {phz_root_dir}/env.sh",
                cmd_timeout=120,
            ),
        )
    ],
    monitoring=MonitoringHub(
       hub_address=address_by_hostname(),
       hub_port=55055,
       monitoring_debug=True,
       logging_endpoint=f"sqlite:///{phz_root_dir}/phz.db",
       resource_monitoring_interval=10,
   ),
   strategy=None
)

from parsl import ThreadPoolExecutor
from parsl.config import Config
from parsl.monitoring.monitoring import MonitoringHub
from parsl.providers import CondorProvider
from parsl.providers import LocalProvider
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_hostname


def get_config(phz_config):
    """
    Creates an instance of the Parsl configuration 

    Args:
        phz_config (dict): Photo-z pipeline configuration - available in the config.yml
    """

    phz_root_dir = phz_config.get("phz_root_dir")

    executors = {
        "htcondor": HighThroughputExecutor(
            label='htcondor',
            address=address_by_hostname(),
            max_workers=54,
            worker_debug=True,
            provider=CondorProvider(
                init_blocks=15,
                min_blocks=15,
                max_blocks=16,
                parallelism=0.5,
                scheduler_options='+RequiresWholeMachine = True',
                worker_init=f"source {phz_root_dir}/env.sh",
                cmd_timeout=120,
            ),
        ),
        "local": HighThroughputExecutor(
            label='local',
            worker_debug=True,
            provider=LocalProvider(
                min_blocks=1,
                init_blocks=1,
                max_blocks=2,
                nodes_per_block=1,
                parallelism=0.5
            )
        ),
        "local_threads": ThreadPoolExecutor(
            label='local_threads',
            max_threads=2
        )
    }

    executor_key = phz_config.get("executor", "local")
    executor = executors[executor_key]

    return Config(
        executors=[executor],
        monitoring=MonitoringHub(
            hub_address=address_by_hostname(),
            hub_port=55055,
            monitoring_debug=False,
            logging_endpoint=f"sqlite:///{phz_root_dir}/phz.db",
            resource_monitoring_interval=10,
        ),
        strategy=None
    )

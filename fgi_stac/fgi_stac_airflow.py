"""
# fgi_stac
This DAG updates the following datasets:

- [100095stac](https://data.bs.ch/explore/dataset/100095stac)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from helpers.failure_tracking_operator import FailureTrackingDockerOperator
from docker.types import Mount

from common_variables import COMMON_ENV_VARS, PATH_TO_CODE

DAG_ID = "fgi_stac"
FAILURE_THRESHOLD = 1
EXECUTION_TIMEOUT = timedelta(minutes=90)
SCHEDULE = "0 * * * *"

default_args = {
    "owner": "opendata.bs",
    "depend_on_past": False,
    "start_date": datetime(2024, 9, 25),
    "email": Variable.get("EMAIL_RECEIVERS"),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}


with DAG(
    dag_id=DAG_ID,
    description="Run the fgi_stac docker pipeline",
    default_args=default_args,
    schedule=SCHEDULE,
    catchup=False,
) as dag:
    dag.doc_md = __doc__

    cleanup_containers = BashOperator(
        task_id="cleanup_old_containers",
        bash_command="""
            docker rm -f fgi_stac--extract_collections 2>/dev/null || true
            docker rm -f fgi_stac--extract_metadata 2>/dev/null || true
            docker rm -f fgi_stac--etl 2>/dev/null || true
            docker rm -f fgi_stac--migrate_publish_catalog 2>/dev/null || true
            docker rm -f fgi_stac--publish_dataset 2>/dev/null || true
        """,
    )

    common_docker_kwargs = {
        "failure_threshold": FAILURE_THRESHOLD,
        "execution_timeout": EXECUTION_TIMEOUT,
        "image": "ghcr.io/opendatabs/data-processing/fgi_stac:latest",
        "force_pull": True,
        "api_version": "auto",
        "auto_remove": "force",
        "mount_tmp_dir": False,
        "private_environment": {
            **COMMON_ENV_VARS,
            "FTP_USER_01": Variable.get("FTP_USER_01"),
            "FTP_PASS_01": Variable.get("FTP_PASS_01"),
        },
        "docker_url": "unix://var/run/docker.sock",
        "network_mode": "bridge",
        "tty": True,
        "mounts": [
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/{DAG_ID}/data_orig",
                target="/code/data_orig",
                type="bind",
            ),
            Mount(
                source=f"{PATH_TO_CODE}/data-processing/{DAG_ID}/data",
                target="/code/data",
                type="bind",
            ),
        ],
    }

    extract_collections = FailureTrackingDockerOperator(
        task_id="extract_collections",
        command="uv run python extract_collections.py",
        container_name="fgi_stac--extract_collections",
        **common_docker_kwargs,
    )

    extract_metadata = FailureTrackingDockerOperator(
        task_id="extract_metadata",
        command="uv run python extract_metadata.py",
        container_name="fgi_stac--extract_metadata",
        **common_docker_kwargs,
    )

    etl = FailureTrackingDockerOperator(
        task_id="etl",
        command="uv run python etl.py",
        container_name="fgi_stac--etl",
        **common_docker_kwargs,
    )

    migrate_publish_catalog = FailureTrackingDockerOperator(
        task_id="migrate_publish_catalog",
        command="uv run python migrate_publish_catalog.py",
        container_name="fgi_stac--migrate_publish_catalog",
        **common_docker_kwargs,
    )

    publish_dataset = FailureTrackingDockerOperator(
        task_id="publish_dataset",
        command="uv run python publish_dataset.py",
        container_name="fgi_stac--publish_dataset",
        **common_docker_kwargs,
    )

    cleanup_containers >> extract_collections >> extract_metadata >> etl >> migrate_publish_catalog >> publish_dataset

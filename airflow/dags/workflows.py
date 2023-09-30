from pathlib import Path

from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)

from airflow.decorators import dag
from airflow.operators.bash_operator import BashOperator
from airflow.providers.airbyte.operators.airbyte import (
    AirbyteTriggerSyncOperator,
)
# from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

from airflow.utils.dates import days_ago

# Default DAG args
default_args = {
    "owner": "airflow",
    "catch_up": False,
}
BASE_DIR = Path(__file__).parent.parent.parent.absolute()
GE_ROOT_DIR = Path(BASE_DIR, "great_expectations")
DBT_ROOT_DIR = Path(BASE_DIR, "dbf_transforms")

@dag(
    dag_id="dataops",
    description="DataOps workflows.",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["dataops"],
)
def dataops():
    """Production DataOps workflows."""
    # Extract + Load
    extract_and_load_projects = AirbyteTriggerSyncOperator(
        task_id="extract_and_load_projects",
        airbyte_conn_id="airbyte",
        connection_id="33665cd3-d408-4d33-b651-115a1fbaad2b",  # REPLACE
        asynchronous=False,
        timeout=3600,
        wait_seconds=3,
    )
    extract_and_load_tags = AirbyteTriggerSyncOperator(
        task_id="extract_and_load_tags",
        airbyte_conn_id="airbyte",
        connection_id="4db4b72a-a995-4238-991f-fe20195e31d2",  # REPLACE
        asynchronous=False,
        timeout=3600,
        wait_seconds=3,
    )
    validate_projects = GreatExpectationsOperator(
        task_id="validate_projects",
        checkpoint_name="projects",
        data_context_root_dir=GE_ROOT_DIR,
        fail_task_on_validation_failure=True,
    )
    validate_tags = GreatExpectationsOperator(
        task_id="validate_tags",
        checkpoint_name="tags",
        data_context_root_dir=GE_ROOT_DIR,
        fail_task_on_validation_failure=True,
    )

    # Transform (need to)
    transform = BashOperator(task_id="transform", bash_command=f'cd "{DBT_ROOT_DIR}" && dbt run && dbt test')
    # transform = DbtCloudRunJobOperator(
    # task_id="transform",
    # job_id=280770,  # Go to dbt UI > click left menu > Jobs > Transform > job_id in URL
    # wait_for_termination=True,
    # check_interval=10,
    # timeout=300,
    # )  # only paid coustomers can use dbt cloud API

    validate_transforms = GreatExpectationsOperator(
    task_id="validate_transforms",
    checkpoint_name="labeled_projects",
    data_context_root_dir=GE_ROOT_DIR,
    fail_task_on_validation_failure=True,
    )

    # Define DAG
    extract_and_load_projects >> validate_projects
    extract_and_load_tags >> validate_tags
    [validate_projects, validate_tags] >> transform >> validate_transforms

# Run DAG
do = dataops()

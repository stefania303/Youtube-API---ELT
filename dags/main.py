from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import (
    get_mainPlaylist_ID,
    get_video_IDs,
    get_video_data,
    save_to_json,
)
from datawarehouse.data_warehouse import staging_table, core_table
from dataquality.soda import yt_data_quality
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


local_tz = pendulum.timezone("Europe/Bucharest")

# default args
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "stefania.secheli99@gmail.com",
    # "retries":1,
    # "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    # "end_date":datetime(2025,1,1, tzinfo=local_tz),
}


with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce json file with the raw data",
    schedule="0 14 * * *",
    catchup=False,
) as dag_produce:

    # define tasks
    playlist_id = get_mainPlaylist_ID()
    video_id = get_video_IDs(playlist_id)
    extract_data = get_video_data(video_id)
    save_to_json_task = save_to_json(extract_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db",
    )

    # define dependecies

    playlist_id >> video_id >> extract_data >> save_to_json_task >> trigger_update_db


with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to update the db",
    schedule=None,
    catchup=False,
) as dag_update:

    staging_table_task = staging_table()
    core_table_task = core_table()

    trigger_check_quality = TriggerDagRunOperator(
        task_id="trigger_check_quality",
        trigger_dag_id="dataquality_checks",
    )

    staging_table_task >> core_table_task >> trigger_check_quality


with DAG(
    dag_id="dataquality_checks",
    default_args=default_args,
    description="DAG to run the data quality checks",
    schedule=None,
    catchup=False,
) as dag_quality:

    staging_data_quality = yt_data_quality("staging")
    core_data_quality = yt_data_quality("core")

    staging_data_quality >> core_data_quality

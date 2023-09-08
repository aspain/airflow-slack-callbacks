"""
### Callbacks with Slack

Example DAG to showcase the various callbacks in Airflow.

Follow Option #2 outlined here https://medium.com/datareply/integrating-slack-alerts-in-airflow-c9dcd155105
in order to set up Slack HTTP webhook
"""

import datetime
from datetime import timedelta
from functools import partial

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from include.new_version import slack_callback_functions_with_partial_updated

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
    # Callbacks set in default_args will apply to all tasks unless overridden at the task-level
    # Using the partial module allows you to specify different channel_conn_ids for different alerts
    "on_success_callback": partial(
        slack_callback_functions_with_partial_updated.success_callback,
        channel_conn_id="slack_callbacks_partial_one",
    ),
    "on_failure_callback": partial(
        slack_callback_functions_with_partial_updated.failure_callback,
        channel_conn_id="slack_callbacks_partial_one",
    ),
    "on_retry_callback": partial(
        slack_callback_functions_with_partial_updated.retry_callback,
        channel_conn_id="slack_callbacks_partial_one",
    ),
    "sla": timedelta(seconds=10),
}
with DAG(
    dag_id="slack_callbacks_with_partial_updated",
    default_args=default_args,
    start_date=datetime.datetime(2021, 1, 1),
    schedule_interval=timedelta(minutes=2),
    # schedule_interval=None,
    # sla_miss only applies to scheduled DAG runs, it does not work for manually triggered runs
    # If a DAG is running for the first time and sla is missed, sla_miss will not fire on that first run
    sla_miss_callback=partial(
        slack_callback_functions_with_partial_updated.sla_miss_callback,
        channel_conn_id="slack_callbacks_partial_two",
    ),
    catchup=False,
    doc_md=__doc__,
) as dag:
    # This task uses on_execute_callback to send a notification when the task begins
    empty_trigger = EmptyOperator(
        task_id="empty_trigger",
        on_execute_callback=partial(
            slack_callback_functions_with_partial_updated.dag_triggered_callback,
            channel_conn_id="slack_callbacks_partial_two",
        ),
        on_success_callback=None,
    )

    # This task uses the default_args on_success_callback
    empty_success_test = EmptyOperator(task_id="empty_success_test")

    # This task sends a Slack message via a python_callable
    slack_test_func = PythonOperator(
        task_id="slack_test_func",
        python_callable=partial(
            slack_callback_functions_with_partial_updated.slack_test,
            channel_conn_id="slack_callbacks_partial_two",
        ),
        on_success_callback=None,
    )

    # Task will sleep to showcase sla_miss callback
    bash_sleep = BashOperator(
        task_id="bash_sleep",
        bash_command="sleep 30",
    )

    # Task will retry before failing to showcase on_retry_callback
    bash_fail = BashOperator(
        task_id="bash_fail",
        retries=1,
        bash_command="exit 123",
    )

    # Task will still succeed despite previous task failing
    empty_dag_success = EmptyOperator(
        task_id="empty_dag_success",
        on_success_callback=partial(
            slack_callback_functions_with_partial_updated.dag_success_callback,
            channel_conn_id="slack_callbacks_partial_two",
        ),
        trigger_rule="all_done",
    )

    (
        empty_trigger
        >> empty_success_test
        >> slack_test_func
        >> bash_sleep
        >> bash_fail
        >> empty_dag_success
    )

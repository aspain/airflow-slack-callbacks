import traceback

from airflow.operators.python import get_current_context
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

"""
Follow Option #2 outlined here https://medium.com/datareply/integrating-slack-alerts-in-airflow-c9dcd155105
in order to set up Slack HTTP webhook
"""


def dag_triggered_callback(context):
    log_url = context.get("task_instance").log_url
    slack_msg = f"""
            :airflow: DAG has been triggered.
            *Task*: {context.get('task_instance').task_id}
            *DAG*: {context.get('task_instance').dag_id}
            *Execution Time*: {context.get('logical_date')}
            <{log_url}| *Log URL*>
            """
    slack_hook = SlackWebhookHook(slack_webhook_conn_id="slack_callbacks")
    slack_hook.send(text=slack_msg)


def dag_success_callback(context):
    log_url = context.get("task_instance").log_url
    slack_msg = f"""
            :airflow: DAG has succeeded.
            *Task*: {context.get('task_instance').task_id}
            *DAG*: {context.get('task_instance').dag_id}
            *Execution Time*: {context.get('logical_date')}
            <{log_url}| *Log URL*>
            """
    slack_hook = SlackWebhookHook(slack_webhook_conn_id="slack_callbacks")
    slack_hook.send(text=slack_msg)


def success_callback(context):
    log_url = context.get("task_instance").log_url
    slack_msg = f"""
            :white_check_mark: Task has succeeded.
            *Task*: {context.get('task_instance').task_id}
            *DAG*: {context.get('task_instance').dag_id}
            *Execution Time*: {context.get('logical_date')}
            <{log_url}| *Log URL*>
            """
    slack_hook = SlackWebhookHook(slack_webhook_conn_id="slack_callbacks")
    slack_hook.send(text=slack_msg)


def failure_callback(context):
    log_url = context.get("task_instance").log_url
    exception = context.get("exception")
    formatted_exception = "".join(
        traceback.format_exception(
            type(exception), value=exception, tb=exception.__traceback__
        )
    ).strip()
    slack_msg = f"""
            :x: Task has failed.
            *Task*: {context.get('task_instance').task_id}
            *DAG*: {context.get('task_instance').dag_id}
            *Execution Time*: {context.get('logical_date')}
            *Exception*: {formatted_exception}
            <{log_url}| *Log URL*>
            """
    slack_hook = SlackWebhookHook(slack_webhook_conn_id="slack_callbacks")
    slack_hook.send(text=slack_msg)


def retry_callback(context):
    log_url = context.get("task_instance").log_url
    exception = context.get("exception")
    formatted_exception = "".join(
        traceback.format_exception(
            type(exception), value=exception, tb=exception.__traceback__
        )
    ).strip()
    slack_msg = f"""
            :sos: Task is retrying.
            *Task*: {context.get('task_instance').task_id}
            *Try number:* {context.get('task_instance').try_number - 1} out of {context.get('task_instance').max_tries + 1}.
            *DAG*: {context.get('task_instance').dag_id}
            *Execution Time*: {context.get('logical_date')}
            *Exception*: {formatted_exception}
            <{log_url}| *Log URL*>
            """
    slack_hook = SlackWebhookHook(slack_webhook_conn_id="slack_callbacks")
    slack_hook.send(text=slack_msg)


def slack_test(**kwargs):
    context = get_current_context()
    log_url = context.get("task_instance").log_url
    slack_msg = f"""
            :airflow: This is a test for sending a slack message via a PythonOperator.
            *Task*: {context.get('task_instance').task_id}
            *DAG*: {context.get('task_instance').dag_id}
            *Execution Time*: {context.get('logical_date')}
            <{log_url}| *Log URL*>
            """
    slack_hook = SlackWebhookHook(slack_webhook_conn_id="slack_callbacks")
    slack_hook.send(text=slack_msg)


# sla_miss callback requires the following arguments: dag, task_list, blocking_task_list, slas, blocking_tis
def sla_miss_callback(
    dag, task_list, blocking_task_list, slas, blocking_tis, *args, **kwargs
):
    dag_id = slas[0].dag_id
    task_id = slas[0].task_id
    logical_date = slas[0].logical_date.isoformat()
    slack_msg = f"""
            :sos: *SLA has been missed*
            *Task:* {task_id}
            *DAG:* {dag_id}
            *Execution Date:* {logical_date}
            """
    slack_hook = SlackWebhookHook(slack_webhook_conn_id="slack_callbacks")
    slack_hook.send(text=slack_msg)

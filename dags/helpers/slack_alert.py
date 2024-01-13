from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

# Send slack notification from our failure callback
def slack_alert(context):
    ti = context['ti'] # to get the Task Instance
    task_state = ti.state # To get the Task state
    
    # Task state = Success 
    if task_state == 'success':
        exec_date = context.get('execution_date').strftime('%Y-%m-%d %H:%M')
        slack_msg = f"""
        :white_check_mark: Success Run for [{context.get('task_instance').task_id}] at [{exec_date}].
        """
    # Task state = Failed 
    
    elif task_state =='failed':
        exec_date = context.get('execution_date').strftime('%Y-%m-%d %H:%M')
        slack_msg = f"""
:alert: Airflow Task Failed <!here>
*Task*: {context.get('task_instance').task_id}
*Dag*: {context.get('task_instance').dag_id}
*Execution Date*: {exec_date}
*Error Log*: <{context.get('task_instance').log_url}|Link to Airflow Log>
        """

    slack_hook = SlackWebhookHook(slack_webhook_conn_id='slack_app')
    slack_hook.send(text=slack_msg)
from airflow.utils.email import send_email
from airflow.models import Variable

def email_notify(context, **kwargs):
    print(context)
    # Helper data
    current_env = Variable.get('ENVIRONMENT')
    failed_task = context.get('task_instance').task_id
    failed_dag = context.get('task_instance').dag_id
    exec_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url

    # Email subject sine
    subject = f"[{current_env}] Airflow alert: {failed_task} Failed"
    if current_env not in ["demo4", "qae"]:
        subject = "🚨- " + subject

    # Relfect correct log url
    if "localhost" in log_url:
        log_url = log_url.replace("http://localhost:8080", "https://aiq-airflow.{current_env}.agentiq.co")

    # Email message
    msg = (
        f"🚨 Task Failed 🚨"
        f"<b>Task</b>: {failed_task}"
        f"<b>Dag</b>: {failed_dag}"
        f"<b>Execution Time</b>: {exec_date}"
        f"<b>Log Url</b>: {log_url}"
        f"<br>"
        f"There's been an error in the {failed_task} job.<br>"
        f"<br>"
        f"params:"
        f"{context.get('params')}"
        f"conf:"
        f"{context.get('conf')}"
        f"Sincerely,<br>"
        f"AIQ Airflow Bot 🤖<br>"
    )

    send_email('swe@agentiq.com', subject, msg)

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
        subject = "🚨 - " + subject

    # Relfect correct log url
    if "localhost" in log_url:
        env_url = f"https://aiq-airflow.{current_env}.agentiq.co"
        log_url = log_url.replace("http://localhost:8080", env_url)

    # Email message
    msg = (
        f"🚨 Task Failed 🚨"
        "<br>"
        "<br>"
        f"There's been an error in the {failed_task} job."
        "<br>"
        "<br>"
        f"<b>Task</b>: {failed_task}"
        "<br>"
        f"<b>Dag</b>: {failed_dag}"
        "<br>"
        f"<b>Execution Time</b>: {exec_date}"
        "<br>"
        f"<b>Log Url</b>: {log_url}"
        "<br>"
        "<br>"
        f"Sincerely,<br>"
        f"AIQ Airflow Bot 🤖"
    )

    subject.encode('unicode-escape').decode('utf-8')
    msg.encode('unicode-escape').decode('utf-8')

    send_email('dawson@agentiq.com', subject, msg, mime_charset='utf-8')

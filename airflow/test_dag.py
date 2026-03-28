from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models.baseoperator import BaseOperator

try:
    from airflow.operators.dummy import DummyOperator
except ImportError:
    from airflow.operators.empty import EmptyOperator as DummyOperator


# (1) Default arguments del DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(1900, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}


# (4) Operador custom TimeDiff
class TimeDiff(BaseOperator):
    def __init__(self, diff_date: datetime, **kwargs):
        super().__init__(**kwargs)
        self.diff_date = diff_date

    def execute(self, context):
        now = datetime.now(timezone.utc)

        if self.diff_date.tzinfo is None:
            diff_date = self.diff_date.replace(tzinfo=timezone.utc)
        else:
            diff_date = self.diff_date

        diff = now - diff_date

        self.log.info("Current date: %s", now)
        self.log.info("Input date: %s", diff_date)
        self.log.info("Difference: %s", diff)


# (1) Definición del DAG test con ejecución diaria a las 03:00 UTC
with DAG(
    dag_id="test",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    catchup=False,
) as dag:

    # (2) Tareas start y end como DummyOperator
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    # (3) Definición de tareas dummy task_n
    task_1 = DummyOperator(task_id="task_1")
    task_2 = DummyOperator(task_id="task_2")
    task_3 = DummyOperator(task_id="task_3")
    task_4 = DummyOperator(task_id="task_4")
    task_5 = DummyOperator(task_id="task_5")

    # (4) Tarea usando el operador TimeDiff
    time_diff_task = TimeDiff(
        task_id="time_diff_task",
        diff_date=datetime(2024, 1, 1),
    )

    # (2) Dependencia básica start >> end
    start >> end

    # (3) Separación de tareas impares y pares
    odd_tasks = [task_1, task_3, task_5]
    even_tasks = [task_2, task_4]

    # (3) Cada tarea par depende de todas las impares
    for even_task in even_tasks:
        odd_tasks >> even_task

    # (4) Integración del operador custom en el flujo
    start >> time_diff_task >> end


# (5) Diferencia entre Hook y Connection
# Hook vs Connection
# Una Connection guarda la información de acceso a un sistema externo.
# Un Hook usa esa información para conectarse y ejecutar operaciones sobre ese sistema.

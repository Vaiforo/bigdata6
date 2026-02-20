import os
import json
import asyncio
from datetime import datetime

import aiohttp

from airflow.decorators import dag, task
from airflow.sensors.python import PythonSensor
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from config import FLAG_PATH, OUT_DIR, TEACHERS_ID
from scheduler import get_json_async, get_json_sync


@dag(
    dag_id="omgtu_teacher_scheduler",
    start_date=datetime(1900, 1, 1),
    schedule=None,
    catchup=False,
    tags=["omgtu", "schedule"]
)
def dag_teacher_shedule():
    start = EmptyOperator(task_id="start")

    @task
    def get_teachers() -> list[int]:
        return TEACHERS_ID
    
    @task
    def process_teacher_schedule(teacher_id: int) -> list:
        path = os.path.join(OUT_DIR, f"schedule_{teacher_id}_{'{{ ds_nodash }}'}.json")

        if not os.path.isfile(path):
            raise FileNotFoundError(f"Файл JSON не найден по пути: {path}")
        
        ctx = get_current_context()
        logical_date = ctx["logical_date"]

        year = logical_date.year
        mounth = logical_date.mounth
        day = logical_date.day

        hdfs_dir = (f"/schedule/year={year}/mounth={mounth}/day={day}/teacher_id={teacher_id}")

        hdfs_file = f"{hdfs_dir}/schedule.json"

        hook = WebHDFSHook(webhdfs_conn_id="webhdfs_default")
        client = hook.get_conn()

        client.makedirs(hdfs_dir)

        hook.load_file(source=path, destination=hdfs_file, owerwrite=True)

        return hdfs_file
    
    teachers = get_teachers()
    
    trigger_scheduler = TriggerDagRunOperator.partial(
        task_id="run_scheduler",
        trigger_dag_id="omgtu_scheduler",
        wait_for_completion=True,
        poke_interval=10,
        trigger_run_id="teacher={{ dag_run.conf.get('person_id', 'x') }}__idx={{ ti.map_index }}__{{ ds_nodash }}",
        logical_date="{{ logical_date }}",
    ).expand(
        conf=teachers.map(lambda x: {"person_id": x}),
    )

    teacher_schedule = process_teacher_schedule.expand(teacher_id=teachers)

    start >> trigger_scheduler >> teacher_schedule


dag_teacher_shedule()
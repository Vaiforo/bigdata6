import os
import json
import asyncio
from datetime import datetime

import aiohttp

from airflow.decorators import dag, task
from airflow.sensors.python import PythonSensor
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from config import *


@dag(
    dag_id="omgtu_teacher_scheduler",
    start_date=datetime(1900, 1, 1),
    schedule=None,
    catchup=False,
    tags=["omgtu", "schedule"]
)
def dag_teacher_shedule():
    start = EmptyOperator(task_id="start")\
    


dag_teacher_shedule()
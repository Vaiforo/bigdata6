from __future__ import annotations

import os
import json
import asyncio
from datetime import datetime

import aiohttp
from airflow.decorators import dag, task
from airflow.sensors.python import PythonSensor
from airflow.operators.empty import EmptyOperator

PERSON_ID = 1003026
START_DATE = "2026.02.02"
FINISH_DATE = "2026.02.08"
LNG = 1

SCHEDULE_URL = (
    f"https://rasp.omgtu.ru/api/schedule/person/{PERSON_ID}"
    f"?start={START_DATE}&finish={FINISH_DATE}&lng={LNG}"
)

FLAG_PATH = "/opt/airflow/dags/configs/start_process.conf"
OUT_DIR = "/opt/airflow/dags/data"


async def _fetch_schedule_async(url: str) -> list:
    headers = {"Accept": "application/json, text/plain, */*",
               "User-Agent": "Mozilla/5.0 (Airflow DAG)"}
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
    if not isinstance(data, list):
        raise ValueError(f"Ожидали JSON-массив, получили: {type(data)}")
    return data


def fetch_schedule_sync(url: str) -> list:
    return asyncio.run(_fetch_schedule_async(url))


@dag(
    dag_id="omgtu_gunenkov_schedule_2026_02_02_2026_02_08",
    start_date=datetime(2026, 2, 2),
    schedule=None,
    catchup=False,
)
def dag_impl():
    wait_for_flag = PythonSensor(
        task_id="wait_for_start_flag",
        python_callable=lambda: os.path.exists(FLAG_PATH),
        poke_interval=10,
        timeout=60 * 60 * 6,
        mode="poke",
    )

    @task
    def fetch_schedule() -> list:
        return fetch_schedule_sync(SCHEDULE_URL)

    @task.branch
    def branch_by_schedule(schedule: list) -> str:
        return "save_json" if schedule else "no_classes"

    @task
    def save_json(schedule: list, run_ds_nodash: str) -> str:
        os.makedirs(OUT_DIR, exist_ok=True)
        path = os.path.join(OUT_DIR, f"gunenkov_schedule_{run_ds_nodash}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(schedule, f, ensure_ascii=False, indent=2)
        return path

    @task
    def no_classes():
        print("На этой неделе пар у коллеги нет, можно отдыхать")

    done = EmptyOperator(task_id="done")

    schedule = fetch_schedule()
    decision = branch_by_schedule(schedule)

    saved = save_json(schedule=schedule, run_ds_nodash="{{ ds_nodash }}")

    wait_for_flag >> schedule >> decision
    decision >> saved >> done
    decision >> no_classes() >> done


dag_impl()

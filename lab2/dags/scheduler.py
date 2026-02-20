import os
import json
import asyncio
from datetime import datetime

import pendulum

import aiohttp

from airflow.decorators import dag, task
from airflow.sensors.python import PythonSensor
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context

from config import *


async def get_json_async(url: str) -> list:
    timeout = aiohttp.ClientTimeout(total=30)
    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    }

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)

    return data


def get_json_sync(url: str) -> list:
    return asyncio.run(get_json_async(url))


@dag(
    dag_id="omgtu_scheduler",
    start_date=datetime(1900, 1, 1),
    schedule=None,
    catchup=False,
    tags=["omgtu", "respect", "schedule"],
)
def dag_schedule():
    # wait_for_flag = PythonSensor(
    #     task_id="wait_for_start_flag",
    #     python_callable=lambda: os.path.exists(FLAG_PATH),
    #     poke_interval=10,
    #     timeout=60 * 60 * 6,
    #     mode="poke",
    # )
    start = EmptyOperator(task_id="start")

    @task
    def use_person_id() -> int:
        context = get_current_context()

        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run and dag_run.conf else {}

        person_id = conf.get("person_id", DEFAULT_PERSON_ID)

        print(f"Получили person_id = {person_id}")

        return int(person_id)

    @task
    def fetch_schedule(person_id: int) -> list:
        ctx = get_current_context()
        conf = (ctx.get("dag_run").conf or {}) if ctx.get("dag_run") else {}

        base_ds = conf.get("ds")
        logical_date: pendulum.DateTime = (
            pendulum.parse(base_ds) if base_ds else ctx["logical_date"]
        )

        week_start = logical_date.start_of("week")

        week_end = week_start.add(days=6)

        default_start = week_start.format("YYYY.MM.DD")
        default_finish = week_end.format("YYYY.MM.DD")

        start = conf.get("start", default_start)
        finish = conf.get("finish", default_finish)

        url = (
            f"https://rasp.omgtu.ru/api/schedule/person/{person_id}"
            f"?start={start}&finish={finish}&lng=1"
        )

        print(f"logical_date={logical_date.to_iso8601_string()}")
        print(f"week_start={default_start} week_end={default_finish}")
        print(f"Запрос расписания: {url}")

        return get_json_sync(url)

    @task.branch
    def choose_branch(schedule: list) -> str:
        return "save_json" if schedule else "stay_home"

    @task
    def save_json(schedule: list, person_id: int, run_ds_nodash: str) -> str:
        os.makedirs(OUT_DIR, exist_ok=True)
        path = os.path.join(OUT_DIR, f"schedule_{person_id}_{run_ds_nodash}.json")

        with open(path, "w", encoding="utf-8") as f:
            json.dump(schedule, f, ensure_ascii=False, indent=2)

        print(f"Сохранено в файл: {path}")
        return path

    @task
    def stay_home(person_id: int) -> None:
        ctx = get_current_context()
        conf = (ctx.get("dag_run").conf or {}) if ctx.get("dag_run") else {}
        ds_nodash = conf.get("ds_nodash") or ctx["ds_nodash"]

        os.makedirs(OUT_DIR, exist_ok=True)
        path = os.path.join(OUT_DIR, f"schedule_{person_id}_{ds_nodash}.EMPTY")

        with open(path, "w", encoding="utf-8") as f:
            f.write("no data")
        print("На этой неделе пар у коллеги нет, можно отдыхать")

    done = EmptyOperator(task_id="done")

    person_id = use_person_id()
    schedule = fetch_schedule(person_id)
    branch = choose_branch(schedule)

    saved = save_json(schedule=schedule, person_id=person_id, run_ds_nodash="{{ ds_nodash }}")

    start >> person_id >> schedule >> branch
    branch >> saved >> done
    branch >> stay_home(person_id) >> done


dag_schedule()

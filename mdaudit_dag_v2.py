import sqlalchemy as sa
from urllib.parse import quote
import json
import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.contrib.operators.vertica_operator import VerticaOperator

from mdaudit_etl_v2.scripts.collable import get_data


api_con = BaseHook.get_connection('mdaudit')
headers = json.loads(api_con.extra)

dwh_con = BaseHook.get_connection('vertica')

dwh_ps = quote(dwh_con.password)
engine = sa.create_engine(
    f'vertica+vertica_python://{dwh_con.login}:{dwh_ps}@{dwh_con.host}:{dwh_con.port}/sttgaz'
    )


#-------------- DAG -----------------

default_args = {
    'owner': 'Швейников Андрей',
    'email': ['shveynikovab@st.tech'],
    'retries': 4,
    'retry_delay': dt.timedelta(minutes=30),
}
with DAG(
        'MD_Audit_v2',
        default_args=default_args,
        description='Получение данных из MD Audit.',
        start_date=dt.datetime(2021, 1, 1),
        schedule_interval='@daily',
        catchup=True,
        max_active_runs=1
) as dag:

    start = DummyOperator(task_id='Начало')

    with TaskGroup('Загрузка_данных_в_stage_слой') as data_to_stage:

        get_checks_and_answers = PythonOperator(
            task_id='get_checks_and_answers',
            python_callable=get_data,
            op_kwargs={
            'data_type': 'checks_and_answers',
            'host': api_con.host,
            'headers': headers,
            'engine': engine,
            }
        )

        get_shops = PythonOperator(
            task_id='get_shops',
            python_callable=get_data,
            op_kwargs={
                'data_type': 'shops',
                'host': api_con.host,
                'headers': headers,
                'engine': engine,
            }
        )

        [get_checks_and_answers, get_shops]

    with TaskGroup('Формирование_слоя_DDS') as data_to_dds:

        aux_mdaudit_regions = VerticaOperator(
            task_id='update_dds_mdaudit_regions',
            vertica_conn_id='vertica',
            sql='scripts/dds_mdaudit_region.sql',
        )

        tables = (
            'dds_mdaudit_shops',
            'dds_mdaudit_divisions',
            'dds_mdaudit_templates',
            'dds_mdaudit_resolvers',
        )

        parallel_tasks = []

        for table in tables:
            parallel_tasks.append(
                VerticaOperator(
                    task_id=f'update_{table}',
                    vertica_conn_id='vertica',
                    sql=f'scripts/{table}.sql',
                )
            )

        aux_mdaudit_checks = VerticaOperator(
            task_id='update_dds_mdaudit_checks',
            vertica_conn_id='vertica',
            sql='scripts/dds_mdaudit_checks.sql',
        )

        aux_mdaudit_answers = VerticaOperator(
            task_id='update_dds_mdaudit_answers',
            vertica_conn_id='vertica',
            sql='scripts/dds_mdaudit_answers.sql',
        )

        aux_mdaudit_regions >> parallel_tasks >> aux_mdaudit_checks >> aux_mdaudit_answers

    with TaskGroup('Формирование_слоя_dm') as data_to_dm:

        dm_mdaudit_detailed = VerticaOperator(
            task_id='update_dm_mdaudit_detailed',
            vertica_conn_id='vertica',
            sql='scripts/dm_mdaudit_detailed.sql',
        )

        dm_mdaudit_answers = VerticaOperator(
            task_id='update_dm_mdaudit_answers',
            vertica_conn_id='vertica',
            sql='scripts/dm_mdaudit_answers.sql',
        )

        [dm_mdaudit_detailed, dm_mdaudit_answers]

    with TaskGroup('Проверка_данных') as data_check:

        check_1 = VerticaOperator(
            task_id='checking_for_duplicates',
            vertica_conn_id='vertica',
            sql='scripts/checking_for_duplicates.sql'
        )

        check_2 = VerticaOperator(
            task_id='checking_for_accuracy_of_execution',
            vertica_conn_id='vertica',
            sql='scripts/checking_for_accuracy_of_execution.sql'
        )

        [check_1, check_2]

    end = DummyOperator(task_id='Конец')

    start >> data_to_stage >> data_to_dds >> data_to_dm >> data_check >> end
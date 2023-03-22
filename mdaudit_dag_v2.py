import requests
import pandas as pd
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


api_con = BaseHook.get_connection('mdaudit')
headers = json.loads(api_con.extra)

dwh_con = BaseHook.get_connection('vertica')

dwh_ps = quote(dwh_con.password)
engine = sa.create_engine(
    f'vertica+vertica_python://{dwh_con.login}:{dwh_ps}@{dwh_con.host}:{dwh_con.port}/sttgaz'
    )


def result_check(initial_data_volume, table_name):
    """
    Проверяем, что данных в DWH не стало меньше
    после окончания процесса загрузки.
    """

    final_data_volume_in_dwh = pd.read_sql_query(
        f"""
        SELECT COUNT(*) FROM sttgaz.stage_mdaudit_{table_name}
        """,
        engine
    ).values[0][0]

    if final_data_volume_in_dwh < initial_data_volume:
        raise Exception(
            'Количество данных в dwh уменьшилось!'
        )
    print(
        f'Было данных: {initial_data_volume}, стало данных: {final_data_volume_in_dwh}'
    )


#-------------- HANDLERS ---------------

def checks_and_answers(data, start_date):
    """
    Обрабатываем и записываем данные по проверкам и ответам.
    """

    checks = pd.json_normalize(data).drop('answers', axis=1)
    print(checks)

    print('Обеспечение идемпотентности')

    ids_for_del = tuple(checks['id'].values)

    if len(ids_for_del) == 1:
        query_part = f'= {ids_for_del[0]};'
    else:
        query_part = f'IN {ids_for_del};'

    checks_initial_data_volume_in_dwh = pd.read_sql_query(
        """
        SELECT COUNT(*) FROM sttgaz.stage_mdaudit_checks
        """,
        engine
    ).values[0][0]

    answers_initial_data_volume_in_dwh = pd.read_sql_query(
        """
        SELECT COUNT(*) FROM sttgaz.stage_mdaudit_answers
        """,
        engine
    ).values[0][0]

    engine.execute(
        f"""
        DELETE FROM sttgaz.stage_mdaudit_checks
        WHERE id {query_part}
            OR (last_modified_at > '{start_date}' AND id NOT {query_part}
        """
    )

    pd.read_sql_query(
        """
        DELETE FROM sttgaz.stage_mdaudit_answers
        WHERE check_id
        """ + query_part,
        engine
    )

    checks.to_sql(
        'stage_mdaudit_checks',
        engine,
        schema='sttgaz',
        if_exists='append',
        index=False,
    )

    result_check(checks_initial_data_volume_in_dwh, 'checks')

    answers = pd.json_normalize(
        data,
        'answers',
        ['id', 'shop_id'],
        meta_prefix = "check_"
    ).rename({'check_shop_id': 'shop_id'}, axis=1)

    print(answers)

    answers.to_sql(
        'stage_mdaudit_answers',
        engine,
        schema='sttgaz',
        if_exists='append',
        index=False,
    )

    result_check(answers_initial_data_volume_in_dwh, 'answers')


# def deleted_objects_searching(data_type, **context):
    """
    Поиск и удаление объектов, которые были удалены в источнике.
    """
    start_searching_date = context['execution_date'].date() - dt.timedelta(days=90)

    params = {
        'last_modified_at': f'gt.{start_searching_date}'
    }

    response = requests.get(
        data_types[data_type]['url'],
        params=params,
        headers=headers,
        verify=False
    )

    response.raise_for_status()

    data = pd.json_normalize(response.json())

    if data.empty:
        print('Нет данных.')
    else:

        ids = tuple(data['id'].drop_duplicates().values)

        engine.execute(
            f"""
            DELETE FROM sttgaz.stage_mdaudit_checks
            WHERE last_modified_at > '{start_searching_date}'
                AND id NOT IN {ids};
            """
        )


def shops(data, start_date):
    """
    Обрабатываем и записываем данные по СЦ/ДЦ и их руководству.
    """

    dirs_list = []

    for item in data:
        try:
            dir = item['dir']
            df = pd.json_normalize(dir)
            df['shop_id'] = item['id']
            dirs_list.append(df)
        except KeyError:
            continue

    df = pd.concat(dirs_list, axis=0, ignore_index=True)

    updatedict = {
        'id': sa.sql.sqltypes.BIGINT,
        'active': sa.sql.sqltypes.BOOLEAN,
        'login': sa.sql.sqltypes.VARCHAR(100),
        'firstName': sa.sql.sqltypes.VARCHAR(100),
        'lastName': sa.sql.sqltypes.VARCHAR(100),
        'position': sa.sql.sqltypes.VARCHAR(2000),
        'email': sa.sql.sqltypes.VARCHAR(250),
        'level': sa.sql.sqltypes.VARCHAR(250),
        'businessDirId': sa.sql.sqltypes.BIGINT,
        'lang': sa.sql.sqltypes.VARCHAR,
        'invited': sa.sql.sqltypes.BOOLEAN,
    }

    print(df)

    dirs_initial_data_volume_in_dwh = pd.read_sql_query(
        """
        SELECT COUNT(*) FROM sttgaz.stage_mdaudit_dirs
        """,
        engine
    ).values[0][0]

    df.to_sql(
        'stage_mdaudit_dirs',
        engine,
        schema='sttgaz',
        if_exists='replace',
        index=False,
        dtype=updatedict
    )

    result_check(dirs_initial_data_volume_in_dwh, 'dirs')

    df = pd.json_normalize(data, max_level=0)
    df = df.drop('dir', axis=1)

    updatedict = {
        'id': sa.sql.sqltypes.BIGINT,
        'active': sa.sql.sqltypes.BOOLEAN,
        'sap': sa.sql.sqltypes.VARCHAR(100),
        'locality': sa.sql.sqltypes.VARCHAR(6000),
        'address': sa.sql.sqltypes.VARCHAR(6000),
        'city': sa.sql.sqltypes.VARCHAR(255),
        'latitude': sa.sql.sqltypes.Numeric(20, 10),
        'longitude': sa.sql.sqltypes.Numeric(20, 10),
        'regionId': sa.sql.sqltypes.BIGINT,
        'clusterId': sa.sql.sqltypes.BIGINT,
        'timeZoneId': sa.sql.sqltypes.VARCHAR,
    }

    print(df)

    shops_initial_data_volume_in_dwh = pd.read_sql_query(
        """
        SELECT COUNT(*) FROM sttgaz.stage_mdaudit_shops
        """,
        engine
    ).values[0][0]

    df.to_sql(
        'stage_mdaudit_shops',
        engine,
        schema='sttgaz',
        if_exists='replace',
        index=False,
        dtype=updatedict
    )

    result_check(shops_initial_data_volume_in_dwh, 'shops')



#------------- LAUNCHER --------------

data_types = {
    'checks_and_answers': {
        'url': f'{api_con.host}/connector/rpc/stt_checklists',
        'handler': checks_and_answers,
    },
    'shops': {
        'url': f'{api_con.host}/orgstruct/shops',
        'handler': shops,
    }
}


def get_data(data_type, **context):
    """
    Получение данных из API MDAudit,
    сохранение их в DWH.
    """

    params = None

    if data_type == 'checks_and_answers':

        start_date = context['execution_date'].date() - dt.timedelta(days=90)
        end_date = context['execution_date'].date().replace(month=12, day=31)

        params = {
            'and': f'(last_modified_at.gt.{start_date}, last_modified_at.lt.{end_date})'
        }

    response = requests.get(
        data_types[data_type]['url'],
        params=params,
        headers=headers,
        verify=False
    )

    response.raise_for_status()

    data = response.json()

    if not data:
        print('Нет данных.')
    else:

        data_types[data_type]['handler'](data, start_date)


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
        start_date=days_ago(1),
        schedule_interval='@daily',
        catchup=True,
        max_active_runs=1
) as dag:

    start = DummyOperator(task_id='Начало')

    with TaskGroup('Загрузка_данных_в_stage_слой') as data_to_stage:

        get_checks_and_answers = PythonOperator(
            task_id='get_checks_and_answers',
            python_callable=get_data,
            op_kwargs={'data_type': 'checks_and_answers'}
        )

        get_shops = PythonOperator(
            task_id='get_shops',
            python_callable=get_data,
            op_kwargs={'data_type': 'shops'}
        )

        delete_objects = PythonOperator(
            task_id='deleted_objects_searching',
            python_callable=deleted_objects_searching,
            op_kwargs={'data_type': 'checks_and_answers'}
        )        

        [get_checks_and_answers, get_shops] >> delete_objects

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
import requests
import pandas as pd
import sqlalchemy as sa
import datetime as dt


def result_check(initial_data_volume, table_name, engine):
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

def checks_and_answers(data, start_date, engine):
    """
    Обрабатываем и записываем данные по проверкам и ответам.
    """

    checks = pd.json_normalize(data).drop('answers', axis=1)
    print(checks)

    print('Обеспечение идемпотентности')

    ids_for_del = tuple(checks['id'].values)

    if len(ids_for_del) == 1:
        query_part = f'= {ids_for_del[0]}'
    else:
        query_part = f'IN {ids_for_del}'

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
            OR (last_modified_at > '{start_date}' AND id NOT {query_part})
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

    result_check(checks_initial_data_volume_in_dwh, 'checks', engine)

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

    result_check(answers_initial_data_volume_in_dwh, 'answers', engine)

   
def shops(data, start_date, engine):
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

    result_check(dirs_initial_data_volume_in_dwh, 'dirs', engine)

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

    result_check(shops_initial_data_volume_in_dwh, 'shops', engine)



#------------- LAUNCHER --------------


def get_data(data_type, host, headers, engine, **context):
    """
    Получение данных из API MDAudit,
    сохранение их в DWH.
    """

    data_types = {
        'checks_and_answers': {
            'url': f'{host}/connector/rpc/stt_checklists',
            'handler': checks_and_answers,
        },
        'shops': {
            'url': f'{host}/orgstruct/shops',
            'handler': shops,
        }
    }

    params = None

    start_date = context['execution_date'].date() - dt.timedelta(days=30)
    end_date = context['next_execution_date'].date()

    print('Запрашиваем данные за период', start_date, end_date)

    if data_type == 'checks_and_answers':

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

        data_types[data_type]['handler'](data, start_date, engine)

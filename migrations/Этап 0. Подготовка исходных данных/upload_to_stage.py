import pandas as pd
import numpy as np
import psycopg2
import os

# Глобальные переменные
import sys
sys.path.insert(0, '/lessons/migrations/')
import global_variables as gv

conn = psycopg2.connect(**gv.conn_parameters)
cur = conn.cursor()

# Создание таблиц БД
truncate_schema =\
    '''
    DROP SCHEMA IF EXISTS staging CASCADE;
    CREATE SCHEMA staging;

    CREATE TABLE staging.customer_research (
        date_id timestamp,
        category_id smallint,
        geo_id smallint,
        sales_qty bigint,
        sales_amt bigint);

    CREATE TABLE staging.user_activity_log (
        id serial,
        date_time timestamp,
        action_id smallint,
        customer_id smallint,
        quantity bigint);

    CREATE TABLE staging.user_orders_log (
        id serial,
        date_time timestamp,
        city_id smallint,
        city_name varchar(50),
        customer_id smallint,
        first_name varchar(15),
        last_name varchar(15),
        item_id integer,
        item_name varchar(50),
        quantity bigint,
        payment_amount numeric(10,2));
    '''
cur.execute(truncate_schema)
conn.commit()

# Получение схемы БД
get_db_info =\
    '''
    SELECT table_name, ordinal_position, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'staging'
    ORDER BY table_name, ordinal_position
    '''
cur.execute(get_db_info)
db_info = pd.DataFrame(columns=['table_name', 'ordinal_position', 'column_name', 'data_type'], data=cur.fetchall())
#print(db_info)

# зарузка исходных данных
file_location = '/lessons/migrations/Этап 0. Подготовка исходных данных/data/'
file_names = os.listdir(file_location)

for i in file_names:
    table_name = i.replace('.csv', '')
    table_columns = db_info[db_info['table_name'] == table_name]['column_name']

    if table_name in db_info['table_name'].unique():
        df = pd.read_csv(file_location + i)
        df = df.reindex(columns=table_columns) # проверка порядка столбцов

        input = [tuple(x) for x in df.to_numpy()]
        input = ','.join(str(x) for x in input)

        insert_request =\
            f'''
            INSERT INTO staging.{table_name} ({','.join(table_columns)})
            VALUES %s;
            '''
        #print(insert_request)
        cur.execute(insert_request % input)
        conn.commit()

        print('Таблица загружена в БД:', table_name)
    else:
        print('Таблица не найдена:', table_name)

conn.close()












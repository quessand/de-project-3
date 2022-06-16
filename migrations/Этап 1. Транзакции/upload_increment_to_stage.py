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


file_location = '/lessons/migrations/Этап 1. Транзакции/data/'
file_names = os.listdir(file_location)

for i in file_names:
    table_name = i.replace('_inc.csv', '')
    table_columns = db_info[db_info['table_name'] == table_name]['column_name']

    if table_name in db_info['table_name'].unique():
        df = pd.read_csv(file_location + i)

        date_column = db_info[ (db_info['table_name'] == table_name) & (db_info['data_type'] == 'timestamp without time zone') ]['column_name'].values[0]
        increment_dates = ','.join(str(x) for x in df[date_column].unique())

        # проверка наличия новой колонки
        if table_name == 'user_orders_log' and 'status' not in df.columns:
            df['status'] = 'shipped'
        
        # установка отрицательного знака для возвратов
        if table_name == 'user_orders_log':
            df['quantity'] = np.where(df['status'] == 'refunded', df['quantity']*-1, df['quantity'])
            df['payment_amount'] = np.where(df['status'] == 'refunded', df['payment_amount']*-1, df['payment_amount'])
            #print(df[ df['status'] == 'refunded'])
        
        # проверка порядка столбцов
        df = df.reindex(columns=table_columns)

        input = [tuple(x) for x in df.to_numpy()]
        input = ','.join(str(x) for x in input)

        insert_request =\
            f'''
            -- удаление записей с датой инкремента, чтобы избежать дубликатов
            DELETE FROM staging.{table_name}
            WHERE {date_column} = '{increment_dates}';

            INSERT INTO staging.{table_name} ({','.join(table_columns)})
            VALUES %s;
            '''
        #print(insert_request)
        cur.execute(insert_request % input)
        conn.commit()

        print('Обновление загружено в БД:', table_name)

conn.close()

        




            
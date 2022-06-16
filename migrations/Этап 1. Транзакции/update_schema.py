import psycopg2

# Глобальные переменные
import sys
sys.path.insert(0, '/lessons/migrations/')
import global_variables as gv

conn = psycopg2.connect(**gv.conn_parameters)
cur = conn.cursor()

add_status = \
    '''
    ALTER TABLE staging.user_orders_log ADD COLUMN IF NOT EXISTS status varchar(20);

    UPDATE staging.user_orders_log
    SET status = 'shipped'
    WHERE status IS NULL;

    ALTER TABLE mart.f_sales ADD COLUMN IF NOT EXISTS status varchar(20);

    UPDATE mart.f_sales
    SET status = 'shipped'
    WHERE status IS NULL;
    '''
cur.execute(add_status)
conn.commit()

conn.close()


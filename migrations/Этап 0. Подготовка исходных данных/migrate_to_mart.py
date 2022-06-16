import psycopg2

# Глобальные переменные
import sys
sys.path.insert(0, '/lessons/migrations/')
import global_variables as gv

conn = psycopg2.connect(**gv.conn_parameters)
cur = conn.cursor()

upload_city = \
    '''
    TRUNCATE TABLE mart.d_city CASCADE;

    INSERT INTO mart.d_city (city_id, city_name)
    SELECT DISTINCT city_id, city_name
    FROM staging.user_orders_log
    ORDER BY city_id;
    '''
cur.execute(upload_city)
conn.commit()

upload_customer = \
    '''
    TRUNCATE TABLE mart.d_customer CASCADE;

    INSERT INTO mart.d_customer (customer_id, first_name, last_name, city_id)
    SELECT customer_id, first_name, last_name, MAX(city_id) AS city_id
    FROM staging.user_orders_log
    GROUP BY customer_id, first_name, last_name
    ORDER BY customer_id;
    '''
cur.execute(upload_customer)
conn.commit()

upload_item = \
    '''
    TRUNCATE TABLE mart.d_item CASCADE;

    INSERT INTO mart.d_item (item_id, item_name)
    SELECT DISTINCT item_id, item_name
    FROM staging.user_orders_log
    ORDER BY item_id;
    '''
cur.execute(upload_item)
conn.commit()

upload_sales = \
    '''
    TRUNCATE TABLE mart.f_sales CASCADE;

    INSERT INTO mart.f_sales (date_id,item_id, customer_id, city_id, quantity, payment_amount)
    SELECT mart.d_calendar.date_id,item_id, customer_id, city_id, quantity, payment_amount
    FROM staging.user_orders_log
    LEFT JOIN mart.d_calendar ON staging.user_orders_log.date_time=mart.d_calendar.date_actual
    ORDER BY mart.d_calendar.date_id;
    '''
cur.execute(upload_sales)
conn.commit()

conn.close()
import psycopg2

# Глобальные переменные
import sys
sys.path.insert(0, '/lessons/migrations/')
import global_variables as gv

conn = psycopg2.connect(**gv.conn_parameters)
cur = conn.cursor()

update_city = \
    '''
    INSERT INTO mart.d_city (city_id, city_name)
    SELECT DISTINCT staging.user_orders_log.city_id, staging.user_orders_log.city_name
    FROM staging.user_orders_log
    LEFT JOIN mart.d_city ON staging.user_orders_log.city_id=mart.d_city.city_id
    WHERE mart.d_city.city_id IS NULL
    ORDER BY staging.user_orders_log.city_id;
    '''
cur.execute(update_city)
conn.commit()

update_customer = \
    '''
    INSERT INTO mart.d_customer (customer_id, first_name, last_name, city_id)
    SELECT staging.user_orders_log.customer_id, staging.user_orders_log.first_name, staging.user_orders_log.last_name, MAX(staging.user_orders_log.city_id) AS city_id
    FROM staging.user_orders_log
    LEFT JOIN mart.d_customer ON staging.user_orders_log.customer_id=mart.d_customer.customer_id
    WHERE mart.d_customer.customer_id IS NULL
    GROUP BY staging.user_orders_log.customer_id, staging.user_orders_log.first_name, staging.user_orders_log.last_name
    ORDER BY staging.user_orders_log.customer_id;
    '''
cur.execute(update_customer)
conn.commit()

update_item = \
    '''
    INSERT INTO mart.d_item (item_id, item_name)
    SELECT DISTINCT staging.user_orders_log.item_id, staging.user_orders_log.item_name
    FROM staging.user_orders_log
    LEFT JOIN mart.d_item ON staging.user_orders_log.item_id=mart.d_item.item_id
    WHERE d_item.item_id IS NULL
    ORDER BY staging.user_orders_log.item_id
    '''
cur.execute(update_item)
conn.commit()

delete_increment_dates = \
    f'''
    -- удаление записей с датой инкремента
    DELETE FROM mart.f_sales
    WHERE date_id = (
        SELECT date_id
        FROM mart.d_calendar
        WHERE date_actual = '{gv.increment_date}');
    '''
cur.execute(delete_increment_dates)
conn.commit()

update_sales = \
    f'''
    INSERT INTO mart.f_sales (date_id,item_id, customer_id, city_id, quantity, payment_amount, status)
    SELECT mart.d_calendar.date_id,staging.user_orders_log.item_id, staging.user_orders_log.customer_id, staging.user_orders_log.city_id, staging.user_orders_log.quantity, staging.user_orders_log.payment_amount, staging.user_orders_log.status
    FROM staging.user_orders_log
    LEFT JOIN mart.d_calendar ON staging.user_orders_log.date_time=mart.d_calendar.date_actual
    WHERE mart.d_calendar.date_actual = '{gv.increment_date}'
    ORDER BY mart.d_calendar.date_id
    '''

'''
INSERT INTO mart.f_sales (date_id,item_id, customer_id, city_id, quantity, payment_amount, status)
SELECT mart.d_calendar.date_id,staging.user_orders_log.item_id, staging.user_orders_log.customer_id, staging.user_orders_log.city_id, staging.user_orders_log.quantity, staging.user_orders_log.payment_amount, staging.user_orders_log.status
FROM staging.user_orders_log
LEFT JOIN mart.d_calendar ON staging.user_orders_log.date_time=mart.d_calendar.date_actual
LEFT JOIN mart.f_sales ON mart.d_calendar.date_id=mart.f_sales.date_id 
WHERE mart.f_sales.date_id IS NULL
ORDER BY mart.d_calendar.date_id
'''
cur.execute(update_sales)
conn.commit()

conn.close()
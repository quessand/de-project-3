import psycopg2

# Глобальные переменные
import sys
sys.path.insert(0, '/lessons/migrations/')
import global_variables as gv

conn = psycopg2.connect(**gv.conn_parameters)
cur = conn.cursor()

create_view = \
    '''
    CREATE OR REPLACE VIEW mart.f_customer_retention AS

        WITH gen AS (
            SELECT
                subq.week_of_year,
                SUM(CASE WHEN subq."count"=1 THEN 1 ELSE 0 END) AS new_customers_count,
                SUM(CASE WHEN subq."count">1 THEN 1 ELSE 0 END) AS returning_customers_count,
                'weekly' AS period_name,
                subq.week_of_year AS period_id,
                SUM(CASE WHEN subq."count"=1 THEN subq."sum" ELSE 0 END) AS new_customers_revenue,
                SUM(CASE WHEN subq."count">1 THEN subq."sum" ELSE 0 END) AS returning_customers_revenue
            FROM (
                SELECT dc.week_of_year, ms.customer_id, COUNT(ms.customer_id), SUM(ms.payment_amount)
                FROM mart.f_sales ms
                LEFT JOIN mart.d_calendar dc ON ms.date_id=dc.date_id
                GROUP BY dc.week_of_year, ms.customer_id
                ORDER BY dc.week_of_year
                ) AS subq
            GROUP BY subq.week_of_year)

        SELECT
            gen.new_customers_count,
            gen.returning_customers_count,
            subq2.refunded_customer_count,
            gen.period_name,
            gen.period_id,
            gen.new_customers_revenue,
            gen.returning_customers_revenue,
            subq2.customers_refunded
        FROM gen
        LEFT JOIN (
            SELECT 
                rfd.week_of_year,
                COUNT(rfd.count) AS refunded_customer_count,
                SUM(rfd.count) AS customers_refunded
            FROM (
                SELECT dc.week_of_year, ms.customer_id,COUNT(ms.payment_amount)
                FROM mart.f_sales ms
                LEFT JOIN mart.d_calendar dc ON ms.date_id=dc.date_id
                WHERE ms.status = 'refunded'
                GROUP BY dc.week_of_year, ms.customer_id
                ORDER BY dc.week_of_year) AS rfd
            GROUP BY rfd.week_of_year) AS subq2
        ON gen.week_of_year=subq2.week_of_year;
    '''
cur.execute(create_view)
conn.commit()

conn.close()
import requests
import pandas as pd
import time

# Глобальные переменные
import sys
sys.path.insert(0, '/lessons/migrations/')
import global_variables as gv

# Получение task_id

generate_task = requests.post(gv.api_link + '/generate_report',headers=gv.headers).json()
print(generate_task)
task_id = generate_task['task_id'] 

# Получение report_id
status = ''
print('Ожидание ответа сервера...')

while status != 'SUCCESS':
    generate_report = requests.get(gv.api_link + '/get_report' + '?task_id=' + task_id, headers=gv.headers).json()
    status = generate_report['status']
    report_id = generate_report['data']['report_id']
    time.sleep(5)


# Получение increment_id
status = ''
print('Ожидание ответа сервера...')

while status != 'SUCCESS':
    #'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/get_increment?report_id={{ report_id }}&date={{ date }}'
    generate_increment = requests.get(gv.api_link + '/get_increment' + '?report_id=' + report_id + '&date=' + gv.increment_date, headers=gv.headers).json()
    status = generate_increment['status']
    time.sleep(5)

file_links = dict(generate_increment ['data']['s3_path'])

save_to = '/lessons/migrations/Этап 1. Транзакции/data/'
for key, value in file_links.items():
    file_name = key + '.csv'
    file_link = value

    df = pd.read_csv(file_link)
    df.to_csv(save_to + file_name, index=False)
    print('Файл скачан:', file_name)
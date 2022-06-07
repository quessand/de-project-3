import requests
import pandas as pd
import time

# Получение task_id
api_link = 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net'
headers = {
    'X-Nickname': 'quessand',
    'X-Cohort': '2',
    'X-Project': 'True',
    'X-API-KEY': '5f55e6c0-e9e5-4a9c-b313-63c01fc31460'}

generate_task = requests.post(api_link + '/generate_report',headers=headers).json()
print(generate_task)
task_id = generate_task['task_id'] 

# Получение report_id
status = ''
print('Ожидание ответа сервера...')

while status != 'SUCCESS':
    generate_report = requests.get(api_link + '/get_report' + '?task_id=' + task_id, headers=headers).json()
    status = generate_report['status']
    report_id = generate_report['data']['report_id']
    time.sleep(5)

print(generate_report['data'])

# Получение файлов
file_names = ['customer_research', 'user_order_log', 'user_activity_log']
storage_link = 'https://storage.yandexcloud.net/s3-sprint3'

for i in file_names:
    url = storage_link + f'/cohort_{headers["X-Cohort"]}' + f'/{headers["X-Nickname"]}' + '/project' + f'/{report_id}' + f'/{i}' + '.csv'
    df = pd.read_csv(url)
    df.to_csv('/lessons/migrations/Этап 0. Получение исходных данных/data/' + i + '.csv')



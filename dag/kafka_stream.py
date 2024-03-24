# where we run our DAG
# run in the terminal: python dags/kafka_stream.py

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2024, 3, 24, 9, 15)
}

def get_data():
    import requests
    
    res = requests.get(  # fetch data from the api
        "https://randomuser.me/api"
    )
    
    # convert data fetched to JSON
    res = res.json()
    
    # get the first part of data
    res = res['results'][0]  # results is data returned from api
    
    return res

def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f'{str(location['street']['number'])} {location['street']['name']} {location['city'] + ', ' + location['state']} '\
                        f'{location['country']}'
    data['postcode'] = res['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']
    
    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging
    
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ns=5000)
    curr_time = time.time()
    
    while True:
        if time.time() > curr_time + 60: # 1 minute
            break
        try:
            res = get_data()
            res = format_data(res)
            
            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            continue
    
    # print(json.dumps(res, indent=3))

with DAG('user_automation',
            default_args = default_args,
            schedule_interval = '@daily',
            catchup = False
        ) as dag:
    streaming_task = PythonOperator(
        task_id = 'stream_data_from_api',
        python_callable = stream_data
    )

stream_data()


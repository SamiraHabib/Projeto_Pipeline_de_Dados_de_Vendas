from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import logging

CLIENT_ID = ''
CLIENT_SECRET = ''
REFRESH_TOKEN = ''

def get_access_token():
    token_url = "https://api.mercadolibre.com/oauth/token"
    payload = {
        'grant_type': 'refresh_token',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'refresh_token': REFRESH_TOKEN
    }

    try:

        response = requests.post(token_url, data=payload)
        response.raise_for_status() 
        token_data = response.json()

        if 'access_token' in token_data:
            access_token = token_data['access_token']
            logging.info("Novo token de acesso obtido: %s", access_token[:5] + "*****")

            existing_token = Variable.get("ACCESS_TOKEN", default_var=None)
            if existing_token:
                logging.info("Variável ACCESS_TOKEN já existe. Atualizando o valor...")
                Variable.update("ACCESS_TOKEN", access_token)
            else:
                logging.info("Variável ACCESS_TOKEN não existe. Criando nova entrada...")
                Variable.set("ACCESS_TOKEN", access_token)

        else:
            raise Exception("Erro ao obter o token: " + str(token_data))

    except requests.exceptions.RequestException as e:
        logging.error(f"Erro na requisição para obter o token: {e}")
        raise Exception(f"Erro na requisição para obter o token: {e}")
    except Exception as e:
        logging.error(f"Erro inesperado: {e}")
        raise Exception(f"Erro inesperado: {e}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'get_access_token_dag',
    default_args=default_args,
    description='DAG para obter e atualizar o token de acesso do Mercado Livre',
    schedule_interval='@daily',
)

task_get_token = PythonOperator(
    task_id='get_access_token',
    python_callable=get_access_token,
    dag=dag,
)

task_get_token
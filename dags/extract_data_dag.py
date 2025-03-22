import random
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import pandas as pd
import logging

TERMOS_DE_BUSCA = [
    'notebook', 'smartphone', 'tv', 'fone de ouvido', 'câmera', 
    'geladeira', 'microondas', 'videogame', 'tablet', 'impressora'
]

def extract_data():
    logging.info("Iniciando extração de dados da API do Mercado Livre.")

    ACCESS_TOKEN = Variable.get("ACCESS_TOKEN", default_var=None)
    if not ACCESS_TOKEN:
        raise Exception("ACCESS_TOKEN não encontrado. Execute `get_access_token_dag` antes.")

    termo_busca = random.choice(TERMOS_DE_BUSCA)
    logging.info(f"Termo de busca selecionado: {termo_busca}")

    url = "https://api.mercadolibre.com/sites/MLB/search"
    params = {
        'q': termo_busca,
        'access_token': ACCESS_TOKEN
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if 'results' not in data or not isinstance(data['results'], list):
            raise Exception("Resposta da API inválida: 'results' não encontrado ou não é uma lista.")

        df = pd.DataFrame(data['results'])

        colunas_desejadas = ['id', 'title', 'price', 'condition', 'thumbnail']
        df = df[df.columns.intersection(colunas_desejadas)]

        if df.empty:
            raise Exception("Nenhuma coluna desejada encontrada na resposta da API.")

        hook = PostgresHook(postgres_conn_id='postgres_default', timeout=10)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT id FROM Produtos;")
        ids_existentes = {row[0] for row in cursor.fetchall()}

        df_novos_produtos = df[~df['id'].isin(ids_existentes)]

        if not df_novos_produtos.empty:
            for _, row in df_novos_produtos.iterrows():
                cursor.execute(
                    """
                    INSERT INTO Produtos (id, title, price, condition, thumbnail)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                    """,
                    (row['id'], row['title'], row['price'], row['condition'], row['thumbnail'])
                )
            conn.commit()
            logging.info(f"{len(df_novos_produtos)} novos produtos inseridos no banco de dados.")
        else:
            logging.info("Nenhum novo produto encontrado para inserção.")

    except requests.exceptions.RequestException as e:
        logging.error(f"Erro na requisição à API: {e}")
        raise
    except Exception as e:
        logging.error(f"Erro inesperado: {e}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        logging.info("Conexão com o banco de dados fechada.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'extract_data_dag',
    default_args=default_args,
    description='Pipeline para extrair dados do Mercado Livre e carregar no PostgreSQL',
    schedule_interval='@daily',
)

task_extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

task_extract_data
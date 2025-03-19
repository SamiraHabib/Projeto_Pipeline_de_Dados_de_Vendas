from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import pandas as pd
import logging
import sqlite3
import os

def extract_data():
    logging.info("Iniciando extração de dados da API do Mercado Livre...")

    ACCESS_TOKEN = Variable.get("ACCESS_TOKEN", default_var=None)
    if not ACCESS_TOKEN:
        logging.error("ACCESS_TOKEN não encontrado. Execute `get_access_token_dag` antes.")
        raise Exception("ACCESS_TOKEN não encontrado. Execute `get_access_token_dag` antes.")

    url = "https://api.mercadolibre.com/sites/MLB/search"
    params = {
        'q': 'notebook',
        'access_token': ACCESS_TOKEN
    }

    logging.info(f"URL da requisição: {url}")
    logging.info(f"Parâmetros da requisição: {params}")

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        logging.info("Resposta da API recebida com sucesso.")
        logging.info(f"Resposta da API: {data}")

        if 'results' not in data:
            logging.error(f"Resposta inesperada da API: {data}")
            raise Exception("Erro na API: 'results' não encontrado no JSON.")

        if not isinstance(data['results'], list):
            logging.error(f"Resposta inesperada da API: {data}")
            raise Exception("A chave 'results' não contém uma lista válida.")

        df = pd.DataFrame(data['results'])
        logging.info(f"DataFrame criado com {len(df)} registros.")
        logging.info(f"Colunas disponíveis no DataFrame: {df.columns.tolist()}")  # Log das colunas

        colunas_desejadas = ['id', 'title', 'price', 'sold_quantity', 'condition', 'thumbnail']
        colunas_disponiveis = [col for col in colunas_desejadas if col in df.columns]

        if not colunas_disponiveis:
            logging.error("Nenhuma das colunas desejadas está disponível na resposta da API.")
            raise Exception("Nenhuma das colunas desejadas está disponível na resposta da API.")

        logging.info(f"Colunas disponíveis: {colunas_disponiveis}")

        caminho_csv = "/usr/local/airflow/dags/mercado_livre_data.csv"
        df[colunas_disponiveis].to_csv(caminho_csv, index=False)
        logging.info(f"Arquivo CSV salvo em {caminho_csv} com {len(df)} registros.")
        print("Dados extraídos e salvos em CSV.")

    except requests.exceptions.RequestException as e:
        logging.error(f"Erro na requisição à API: {e}")
        raise Exception(f"Erro na requisição à API: {e}")
    except Exception as e:
        logging.error(f"Erro inesperado: {e}")
        raise Exception(f"Erro inesperado: {e}")

def load_data():
    logging.info("Iniciando carregamento de dados no banco de dados...")

    # Caminho do arquivo CSV
    caminho_csv = "/usr/local/airflow/dags/mercado_livre_data.csv"

    # Verifica se o arquivo CSV existe
    if not os.path.exists(caminho_csv):
        logging.error(f"Arquivo CSV não encontrado: {caminho_csv}")
        raise FileNotFoundError(f"Arquivo CSV não encontrado: {caminho_csv}")

    # Caminho do banco de dados SQLite
    data_folder = 'data'
    db_path = os.path.join(data_folder, 'mercado_livre.db')

    # Cria o diretório 'data' se não existir
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
        logging.info(f"Diretório '{data_folder}' criado com sucesso.")

    try:
        # Conecta ao banco de dados SQLite (ou cria se não existir)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        logging.info(f"Conectado ao banco de dados: {db_path}")

        # Cria a tabela se não existir
        create_table_query = """
        CREATE TABLE IF NOT EXISTS Produtos (
            id TEXT PRIMARY KEY,
            title TEXT,
            price REAL,
            condition TEXT,
            thumbnail TEXT
        )
        """
        cursor.execute(create_table_query)
        conn.commit()
        logging.info("Tabela 'Produtos' criada ou verificada com sucesso.")
        logging.info("Commit realizado com sucesso.")

        df = pd.read_csv(caminho_csv)
        logging.info(f"Arquivo CSV lido com {len(df)} registros.")

        # Insere os dados no banco de dados
        for index, row in df.iterrows():
            insert_query = """
            INSERT OR IGNORE INTO Produtos (id, title, price, condition, thumbnail)
            VALUES (?, ?, ?, ?, ?)
            """
            try:
                cursor.execute(insert_query, (
                    row['id'], row['title'], row['price'], row['condition'], row['thumbnail']
                ))
            except sqlite3.IntegrityError:
                logging.warning(f"Registro duplicado ignorado: ID {row['id']}")
            except Exception as e:
                logging.error(f"Erro ao inserir registro {row['id']}: {e}")

        conn.commit()
        logging.info(f"Dados carregados no banco de dados com sucesso. Total de registros: {len(df)}")

    except sqlite3.Error as e:
        logging.error(f"Erro ao conectar ou executar consulta no SQLite: {e}")
        raise Exception(f"Erro no banco de dados: {e}")
    except Exception as e:
        logging.error(f"Erro inesperado: {e}")
        raise Exception(f"Erro inesperado: {e}")
    finally:
        if conn:
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
    description='Pipeline para extrair dados do Mercado Livre e carregar no SQLite',
    schedule_interval='@daily',
)

task_extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

task_load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

task_extract_data >> task_load_data
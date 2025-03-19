from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import requests
import sqlite3
import pandas as pd
import os

CLIENT_ID = ''
CLIENT_SECRET = ''
REDIRECT_URI = ''
REFRESH_TOKEN = ''

def get_access_token():
    global ACCESS_TOKEN

    token_url = "https://api.mercadolibre.com/oauth/token"
    payload = {
        'grant_type': 'refresh_token',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'refresh_token': REFRESH_TOKEN
    }

    response = requests.post(token_url, data=payload)
    token_data = response.json()

    if 'access_token' in token_data:
        ACCESS_TOKEN = token_data['access_token']
        print("Novo token de acesso obtido:", ACCESS_TOKEN)
    else:
        raise Exception("Erro ao obter o token: " + str(token_data))

def extract_data():
    logging.info("Iniciando extração de dados da API do Mercado Livre...")

    # Obtém o ACCESS_TOKEN das variáveis do Airflow
    ACCESS_TOKEN = Variable.get("ACCESS_TOKEN", default_var=None)
    if not ACCESS_TOKEN:
        logging.error("ACCESS_TOKEN não encontrado. Execute `get_access_token()` antes.")
        raise Exception("ACCESS_TOKEN não encontrado. Execute `get_access_token()` antes.")

    url = "https://api.mercadolibre.com/sites/MLB/search"
    params = {
        'q': 'notebook',
        'access_token': ACCESS_TOKEN
    }

    logging.info(f"URL da requisição: {url}")
    logging.info(f"Parâmetros da requisição: {params}")

    try:
        # Faz a requisição à API
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        logging.info("Resposta da API recebida com sucesso.")

        # Verifica se a chave 'results' está presente no JSON
        if 'results' not in data:
            logging.error(f"Resposta inesperada da API: {data}")
            raise Exception("Erro na API: 'results' não encontrado no JSON.")

        # Verifica se 'results' é uma lista
        if not isinstance(data['results'], list):
            logging.error(f"Resposta inesperada da API: {data}")
            raise Exception("A chave 'results' não contém uma lista válida.")

        # Cria o DataFrame
        df = pd.DataFrame(data['results'])
        logging.info(f"DataFrame criado com {len(df)} registros.")

        # Seleciona as colunas desejadas
        colunas_desejadas = ['id', 'title', 'price', 'sold_quantity', 'condition', 'thumbnail']
        colunas_disponiveis = [col for col in colunas_desejadas if col in df.columns]

        if not colunas_disponiveis:
            logging.error("Nenhuma das colunas desejadas está disponível na resposta da API.")
            raise Exception("Nenhuma das colunas desejadas está disponível na resposta da API.")

        logging.info(f"Colunas disponíveis: {colunas_disponiveis}")

        # Salva o DataFrame em um arquivo CSV
        df[colunas_disponiveis].to_csv('/tmp/mercado_livre_data.csv', index=False)
        logging.info(f"Arquivo CSV salvo em /tmp/mercado_livre_data.csv com {len(df)} registros.")
        print("Dados extraídos e salvos em CSV.")

    except requests.exceptions.RequestException as e:
        logging.error(f"Erro na requisição à API: {e}")
        raise Exception(f"Erro na requisição à API: {e}")
    except Exception as e:
        logging.error(f"Erro inesperado: {e}")
        raise Exception(f"Erro inesperado: {e}")
            
def load_data():
    data_folder = 'data'
    db_path = os.path.join(data_folder, 'mercado_livre.db')
    
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS Produtos (
            id TEXT PRIMARY KEY,
            title TEXT,
            price REAL,
            sold_quantity INTEGER,
            condition TEXT,
            thumbnail TEXT
        )
        """
        cursor.execute(create_table_query)
        conn.commit()
        
        caminho_csv = "/tmp/mercado_livre_data.csv"
        if not os.path.exists(caminho_csv):
            raise FileNotFoundError(f"Arquivo CSV não encontrado: {caminho_csv}")

        df = pd.read_csv(caminho_csv)

        # Verificar se todas as colunas necessárias estão presentes
        required_columns = ['id', 'title', 'price', 'sold_quantity', 'condition', 'thumbnail']
        if not all(column in df.columns for column in required_columns):
            raise ValueError("O arquivo CSV não contém todas as colunas necessárias.")

        # Inserir os dados no banco de dados
        for index, row in df.iterrows():
            insert_query = """
            INSERT OR IGNORE INTO Produtos (id, title, price, sold_quantity, condition, thumbnail)
            VALUES (?, ?, ?, ?, ?, ?)
            """
            try:
                cursor.execute(insert_query, (
                    row['id'], row['title'], row['price'], row['sold_quantity'], row['condition'], row['thumbnail']
                ))
            except sqlite3.IntegrityError:
                logging.warning(f"Registro duplicado ignorado: ID {row['id']}")
            except Exception as e:
                logging.error(f"Erro ao inserir registro {row['id']}: {e}")

        conn.commit()
        print("Dados carregados no SQLite com sucesso.")

    except FileNotFoundError:
        logging.error(f"Arquivo CSV não encontrado: {caminho_csv}")
    except sqlite3.Error as e:
        logging.error(f"Erro ao conectar ou executar consulta no SQLite: {e}")
    except Exception as e:
        logging.error(f"Erro inesperado: {e}")
    finally:
        if conn:
            conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'mercado_livre_pipeline',
    default_args=default_args,
    description='Pipeline para extrair dados do Mercado Livre e carregar no SQLite',
    schedule_interval="@daily",
)

task_get_token = PythonOperator(
    task_id='get_access_token',
    python_callable=get_access_token,
    dag=dag,
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

task_get_token >> task_extract_data >> task_load_data

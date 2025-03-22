from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import logging
import os

def load_csv_to_postgres():
    csv_file_path = '/usr/local/airflow/dags/mercado_livre_data.csv'
    
    if not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"Arquivo CSV não encontrado: {csv_file_path}")
    
    df = pd.read_csv(csv_file_path)
    logging.info(f"Lendo arquivo CSV: {csv_file_path}")
    logging.info(f"Total de linhas no CSV: {len(df)}")
    
    conn = None
    cursor = None
    
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default', timeout=10)
        conn = hook.get_conn()
        cursor = conn.cursor()
        logging.info("Conectado ao PostgreSQL.")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Produtos (
                id VARCHAR PRIMARY KEY,
                title VARCHAR,
                price NUMERIC,
                condition VARCHAR,
                thumbnail VARCHAR
            );
        """)
        conn.commit()
        logging.info("Tabela 'Produtos' criada ou já existente.")
        
        for index, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO Produtos (id, title, price, condition, thumbnail)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    title = EXCLUDED.title,
                    price = EXCLUDED.price,
                    condition = EXCLUDED.condition,
                    thumbnail = EXCLUDED.thumbnail;
                """,
                (row['id'], row['title'], row['price'], row['condition'], row['thumbnail'])
            )
        conn.commit()
        logging.info("Dados inseridos ou atualizados com sucesso.")
    
    except Exception as e:
        logging.error(f"Erro ao carregar dados: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logging.info("Conexão fechada.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'load_csv_to_postgres',
    default_args=default_args,
    description='Uma DAG para carregar dados de um CSV para o PostgreSQL',
    schedule_interval='@daily',
)

load_data_task = PythonOperator(
    task_id='load_csv_to_postgres',
    python_callable=load_csv_to_postgres,
    dag=dag,
)

load_data_task
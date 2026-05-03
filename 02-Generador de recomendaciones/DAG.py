from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook

from google.cloud import storage
import psycopg2

import io
import datetime
import pandas as pd

def get_bucket(**context):  # agregamos context para tener información del DAG
    key_path = "/opt/airflow/airflow/dags/pgvd-493422-056785555b2a.json" 
    storage_client = storage.Client.from_service_account_json(key_path)
    return storage_client.bucket("tp_final_pgvd_udesa") 

def leer_csv(bucket, file_name):
    try:
        blob = bucket.blob(file_name)
        content = blob.download_as_text()
        return pd.read_csv(io.StringIO(content))
    
    except Exception as e:
        print(f"Error leyendo {file_name}: {e}")
        raise

def obtener_archivos(**context):
    return {
        'ads_views_path': "datos_crudos/ads_views",
        'product_views_path': "datos_crudos/product_views",
        'advertiser_ids_path': "datos_crudos/advertiser_ids"
    }

def subir_csv(bucket, df, file_name):
    blob = bucket.blob(file_name)
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    blob.upload_from_string(buffer.getvalue(), content_type='text/csv')

def filtrar_datos(**context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids='filter_process.obtener_archivos')
    # hacemos pull de los archivos de la anterior task del grupo (no se comunican entre ellos directamente)

    bucket = get_bucket()

    ads_views = leer_csv(bucket, data['ads_views_path'])
    product_views = leer_csv(bucket, data['product_views_path'])
    advertiser_ids = leer_csv(bucket, data['advertiser_ids_path'])

    today = context['ds'] 

    # normalizar fechas
    ads_views['date'] = pd.to_datetime(ads_views['date']).dt.strftime('%Y-%m-%d')
    product_views['date'] = pd.to_datetime(product_views['date']).dt.strftime('%Y-%m-%d')

    # filtrar por fecha
    ads_today = ads_views.loc[ads_views['date'] == today]
    prod_today = product_views.loc[product_views['date'] == today]

    # filtrar por advertisers activos
    active_ids = advertiser_ids['advertiser_id']

    ads_today = ads_today.loc[ads_today['advertiser_id'].isin(active_ids)]
    prod_today = prod_today.loc[prod_today['advertiser_id'].isin(active_ids)]

    # guardar
    ads_path = f"datos_filtrados/ads_views_filtered_{today}.csv"
    prod_path = f"datos_filtrados/product_views_filtered_{today}.csv"

    subir_csv(bucket, ads_today, ads_path)
    subir_csv(bucket, prod_today, prod_path)

    return {
        'ads_filtered_path': ads_path,
        'prod_filtered_path': prod_path
    }
    
def top_ctr(**context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids='filter_process.filtrar_datos')

    bucket = get_bucket()

    ads = leer_csv(bucket, data['ads_filtered_path'])
    prod = leer_csv(bucket, data['prod_filtered_path'])

    impressions = ads.groupby(['advertiser_id', 'product_id']).size().reset_index(name='impressions')
    clicks = ads[ads['type'] == 'click'].groupby(['advertiser_id', 'product_id']).size().reset_index(name='clicks')

    df = impressions.merge(clicks, on=['advertiser_id', 'product_id'], how='left').fillna(0)

    # evitar division por 0 (filtramos los que tienen al menos una impresion)
    df = df[df['impressions'] > 0]
    df['ctr'] = df['clicks'] / df['impressions']

    df = df.sort_values(['advertiser_id', 'ctr'], ascending=[True, False])
    df = df.groupby('advertiser_id').head(20)

    today = context['ds']
    output_path = f"resultados/top_ctr_{today}.csv"

    subir_csv(bucket, df, output_path)

def top_product(**context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids='filter_process.filtrar_datos')

    bucket = get_bucket()

    prod = leer_csv(bucket, data['prod_filtered_path'])

    df = prod.groupby(['advertiser_id', 'product_id']).size().reset_index(name='views')

    df = df.sort_values(['advertiser_id', 'views'], ascending=[True, False])
    df = df.groupby('advertiser_id').head(20)

    today = context['ds']
    output_path = f"resultados/top_product_{today}.csv"

    subir_csv(bucket, df, output_path)

def write_to_db(**context):

    bucket = get_bucket()
    today = context['ds']

    top_ctr_path = f"resultados/top_ctr_{today}.csv"
    top_product_path = f"resultados/top_product_{today}.csv"

    df_ctr = leer_csv(bucket, top_ctr_path)
    df_prod = leer_csv(bucket, top_product_path)

    # agregar fecha
    df_ctr['date'] = today
    df_prod['date'] = today

    # conexion de forma segura
    # con conexión guardada mediante comando bash en el AirFlow de la VM
    hook = PostgresHook(postgres_conn_id="my_postgres")
    conn = hook.get_conn()

    cur = conn.cursor()

    # crear tablas si no existen (primera vez que se ejecuta)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS top_ctr (
            advertiser_id TEXT,
            product_id TEXT,
            impressions INT,
            clicks INT,
            ctr FLOAT,
            date DATE,
            UNIQUE(advertiser_id, product_id, date)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS top_product (
            advertiser_id TEXT,
            product_id TEXT,
            views INT,
            date DATE,
            UNIQUE(advertiser_id, product_id, date)
        )
    """)

    # insertar
    for _, row in df_ctr.iterrows():
        cur.execute("""
            INSERT INTO top_ctr VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            row['advertiser_id'],
            row['product_id'],
            int(row['impressions']),
            int(row['clicks']),
            float(row['ctr']),
            today
        ))

    for _, row in df_prod.iterrows():
        cur.execute("""
            INSERT INTO top_product VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            row['advertiser_id'],
            row['product_id'],
            int(row['views']),
            today
        ))

    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id='adtech_dag',
    schedule='@daily',
    start_date=datetime.datetime(2026, 4, 18),
    catchup=True
) as dag:

    with TaskGroup(group_id="filter_process") as filter_group:

        t_obtener = PythonOperator(
            task_id='obtener_archivos',
            python_callable=obtener_archivos
        )

        t_filtrar = PythonOperator(
            task_id='filtrar_datos',
            python_callable=filtrar_datos
        )

        t_obtener >> t_filtrar

    t_top_ctr = PythonOperator(
        task_id='top_ctr',
        python_callable=top_ctr
    )

    t_top_product = PythonOperator(
        task_id='top_product',
        python_callable=top_product
    )

    db_writing = PythonOperator(
        task_id='db_writing',
        python_callable=write_to_db
    )

    filter_group >> [t_top_ctr, t_top_product] >> db_writing

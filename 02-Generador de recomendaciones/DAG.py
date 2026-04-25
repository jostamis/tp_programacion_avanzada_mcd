from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup
from google.cloud import storage

import io
import datetime
import pandas as pd

def obtener_archivos(**context):  # agregamos context para tener información del DAG
    key_path = "pgvd-493422-056785555b2a.json" 
    storage_client = storage.Client.from_service_account_json(key_path)
    bucket = storage_client.bucket("tp_final_pgvd_udesa") 

    def leer_csv(file_name):
        blob = bucket.blob(file_name)
        content = blob.download_as_text()
        return pd.read_csv(io.StringIO(content))

    ads_views = leer_csv("datos_crudos/ads_views.csv")
    product_views = leer_csv("datos_crudos/product_views.csv")
    advertiser_ids = leer_csv("datos_crudos/advertiser_ids.csv")

    # hacemos push de los archivos para la siguiente task del grupo (no se comunican entre ellos)

    context['ti'].xcom_push(key='ads_views', value=ads_views.to_json())
    context['ti'].xcom_push(key='product_views', value=product_views.to_json())
    context['ti'].xcom_push(key='advertiser_ids', value=advertiser_ids.to_json())

def filtrar_datos(**context):
    today = context['ds']

    # hacemos pull de los datos que obtivimos en la task anterior 

    ads_views = pd.read_json(context['ti'].xcom_pull(key='ads_views'))
    product_views = pd.read_json(context['ti'].xcom_pull(key='product_views'))
    advertiser_ids = pd.read_json(context['ti'].xcom_pull(key='advertiser_ids'))

    ads_views_today = ads_views.loc[ads_views['date'] == today]
    product_views_today = product_views.loc[product_views['date'] == today]

    active_ids = advertiser_ids['advertiser_id']
    ads_views_today_act = ads_views_today.loc[ads_views_today['advertiser_id'].isin(active_ids)]
    product_views_today_act = product_views_today.loc[product_views_today['advertiser_id'].isin(active_ids)] 

    key_path = "pgvd-493422-056785555b2a.json"
    storage_client = storage.Client.from_service_account_json(key_path)
    bucket = storage_client.bucket("tp_final_pgvd_udesa")

    def subir_csv(df, file_name):
        blob = bucket.blob(file_name)
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        blob.upload_from_string(buffer.getvalue(), content_type='text/csv')

    subir_csv(ads_views_today_act, f"datos_filtrados/ads_views_filtered_{today}.csv")
    subir_csv(product_views_today_act, f"datos_filtrados/product_views_filtered_{today}.csv")  # ✅ corregido

with DAG(
    dag_id='ad-tech-dag',
    schedule=None,
    start_date=datetime.datetime(2026, 4, 18),
    catchup=False
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

    top_ctr = BashOperator(task_id='top_ctr', bash_command='sleep 3 && echo top_ctr')
    top_product = BashOperator(task_id='top_product', bash_command='sleep 3 && echo top_product')
    db_writing = BashOperator(task_id='db_writing', bash_command='sleep 3 && echo db_writing')

    filter_group >> [top_ctr, top_product] >> db_writing
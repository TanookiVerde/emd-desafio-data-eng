import datetime
import requests

import prefect
from prefect import task

import pandas as pd
import json

@task(max_retries=3, retry_delay=datetime.timedelta(minutes=0.5))
def extract_brt_gps_data_json():
    logger = prefect.context.get("logger")

    logger.info("Fazendo Requisição de Dados de GPS do BRT")
    response = requests.get(
        "https://dados.mobilidade.rio/gps/brt"
    )

    return response.json()


@task(max_retries=3, retry_delay=datetime.timedelta(minutes=0.5))
def transform_json_data_to_df(gps_data_json):
    logger = prefect.context.get("logger")

    logger.info("Fazendo Transformação de JSON para Dataframe")
    gps_data = pd.DataFrame(gps_data_json['veiculos'])

    return gps_data


@task(max_retries=3, retry_delay=datetime.timedelta(minutes=0.5))
def load_brt_gps_data():
    return


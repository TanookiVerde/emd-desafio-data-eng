import datetime
import requests

import prefect
from prefect import task
from prefect.tasks.postgres.postgres import PostgresExecute

import pandas as pd


@task(max_retries=3, retry_delay=datetime.timedelta(minutes=10))
def extract_brt_gps_data_json():
    logger = prefect.context.get("logger")

    logger.info("Fazendo Requisição de Dados de GPS do BRT")
    response = requests.get(
        "https://dados.mobilidade.rio/gps/brt"
    )

    return response.json()


@task(max_retries=3, retry_delay=datetime.timedelta(minutes=10))
def transform_json_data_to_df(gps_data_json):
    logger = prefect.context.get("logger")

    logger.info("Fazendo Transformação de JSON para Dataframe")
    gps_data = pd.DataFrame(gps_data_json['veiculos'])

    return gps_data


@task(max_retries=3, retry_delay=datetime.timedelta(minutes=10))
def transform_identify_empty_values(gps_data_df):
    gps_data_df.sort_values(by='codigo',inplace=True)
    gps_data_df.replace("", pd.NA, inplace=True)
    gps_data_df.replace(" ", pd.NA, inplace=True)

    return gps_data_df

@task(max_retries=3, retry_delay=datetime.timedelta(minutes=10))
def transform_epoch_to_datetime(gps_data_df):
    convert_epoch_to_datetime = lambda dt_epoch : datetime.datetime.fromtimestamp( dt_epoch / 1000.0 )

    gps_data_df['dataHora'] = gps_data_df['dataHora'].apply(convert_epoch_to_datetime)

    return gps_data_df


@task(max_retries=3, retry_delay=datetime.timedelta(minutes=10))
def load_data_to_csv(gps_data_df):
    logger = prefect.context.get("logger")

    logger.info("Carregando Dados já Registrados CSV")
    try:
        current_data = pd.read_csv("registros.csv")
    except FileNotFoundError:
        current_data = pd.DataFrame()

    logger.info("Acumulando Dados no CSV")
    accumulated_data = pd.concat([current_data, gps_data_df])
    accumulated_data.to_csv("registros.csv", index=False, header=True)

    pass 

@task(max_retries=3, retry_delay=datetime.timedelta(minutes=10))
def load_data_to_db(gps_data_df):
    logger = prefect.context.get("logger")

    logger.info("Iniciando Conexão com Banco de Dados")
    executor = PostgresExecute(
        db_name = "brt_gps",
        user    = "admin",
        host    = "password",
        port    = 5432
    )

    logger.info("Envio de Dados")
    # TODO
    executor.run(
        query   = "INSERT INTO registros_brt ()",
        data    = tuple(),
        commit  = True
    )

    pass


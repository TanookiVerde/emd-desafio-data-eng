import datetime
import requests
import pandas as pd

from sqlalchemy import create_engine

import prefect
from prefect import task


@task(max_retries=3, retry_delay=datetime.timedelta(minutes=10))
def extract_brt_gps_data_json():
    """
    Extrai da API de GPS do BRT registros em Json
    """
    logger = prefect.context.get("logger")

    logger.info("Fazendo Requisição de Dados de GPS do BRT")
    response = requests.get(
        "https://dados.mobilidade.rio/gps/brt"
    )

    return response.json()


@task(max_retries=3, retry_delay=datetime.timedelta(minutes=10))
def transform_json_data_to_df(gps_data_json):
    """
    Recebe um Json com registros de GPS BRT e converte para Dataframe
    """
    logger = prefect.context.get("logger")

    logger.info("Fazendo Transformação de JSON para Dataframe")
    gps_data = pd.DataFrame(gps_data_json['veiculos'])

    return gps_data


@task(max_retries=3, retry_delay=datetime.timedelta(minutes=10))
def transform_identify_empty_values(gps_data_df):
    """
    Identifica valores vazios no Dataframe e substitui para Nulo
    """
    gps_data_df.sort_values(by='codigo',inplace=True)
    gps_data_df.replace("", pd.NA, inplace=True)
    gps_data_df.replace(" ", pd.NA, inplace=True)

    return gps_data_df


@task(max_retries=3, retry_delay=datetime.timedelta(minutes=10))
def transform_epoch_to_datetime(gps_data_df):
    """
    Transforma coluna dataHora de Epochs (milliseconds) para Datetime
    """
    convert_epoch_to_datetime = lambda dt_epoch : datetime.datetime.fromtimestamp( dt_epoch / 1000.0 )

    gps_data_df['dataHora'] = gps_data_df['dataHora'].apply(convert_epoch_to_datetime)

    return gps_data_df


@task(max_retries=3, retry_delay=datetime.timedelta(minutes=10))
def transform_rename_columns(gps_data_df):
    """
    Renomeia colunas do DF para mapeamento com colunas do banco de dados
    """
    gps_data_df.rename(
        columns = {'dataHora' : 'datahora'}, 
        inplace = True
    )
    return gps_data_df


@task(max_retries=3, retry_delay=datetime.timedelta(minutes=10))
def load_data_to_csv(gps_data_df):
    """
    Salva Dataframe em CSV de forma acumulativa
    """
    CSV_PATH = "outputs/registros.csv"
    logger = prefect.context.get("logger")

    try:
        current_data = pd.read_csv(CSV_PATH)
        logger.info("Abrindo arquivo CSV com registros para expansão")
    except FileNotFoundError:
        logger.info("Iniciando novo arquivo CSV para registros")
        current_data = pd.DataFrame()

    logger.info("Acumulando Dados no CSV")
    accumulated_data = pd.concat([current_data, gps_data_df])
    accumulated_data.to_csv(CSV_PATH, index=False, header=True)

    pass 


@task(max_retries=3, retry_delay=datetime.timedelta(minutes=10))
def load_data_to_db(table_name, dataframe):
    """
    Recebe Dataframe e insere seus dados no banco de dados
    """
    logger = prefect.context.get("logger")

    logger.info("Envio de Dados")
    dataframe.to_sql(
        name        = table_name, 
        con         = create_engine('postgresql://admin:password@localhost:5432/brt_gps'),
        method      = 'multi',
        index       = False,
        if_exists   = 'append'
    )

    pass


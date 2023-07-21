from prefect import Flow
from prefect.run_configs import DockerRun

from pipelines.brt.schedules import brt_gps_extraction_schedule
from pipelines.brt.tasks import \
    extract_brt_gps_data_json, \
    transform_json_data_to_df, \
    transform_identify_empty_values, \
    transform_epoch_to_datetime, \
    transform_rename_columns, \
    load_data_to_csv, \
    load_data_to_db



with Flow(
        name        = "BRTRio - Extração de Dados GPS", 
        schedule    = brt_gps_extraction_schedule, 
        run_config  = DockerRun(image="test:latest")
    ) as flow:

    # NOTE: Data Extraction
    brt_gps_data_json = extract_brt_gps_data_json()

    # NOTE: Data Transformation
    brt_gps_data_df = transform_json_data_to_df(brt_gps_data_json)
    brt_gps_data_df = transform_identify_empty_values(brt_gps_data_df)
    brt_gps_data_df = transform_epoch_to_datetime(brt_gps_data_df)
    brt_gps_data_df = transform_rename_columns(brt_gps_data_df)

    # NOTE: Data Loading
    load_data_to_csv(brt_gps_data_df)
    load_data_to_db("registro_brt", brt_gps_data_df)

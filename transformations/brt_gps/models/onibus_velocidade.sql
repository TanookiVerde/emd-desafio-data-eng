{{ config(materialized='table') }}

select 
    placa, 
    latitude, 
    longitude, 
    velocidade
from registros_brt
{{ config(materialized='table') }}

select 
    codigo, 
    latitude, 
    longitude, 
    velocidade
from registro_brt
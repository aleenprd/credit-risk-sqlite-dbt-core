{{ config(
    materialized = 'table',
    sort = 'id',
    dist = 'id'
) }}


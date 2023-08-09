{{ config(
    materialized = "table",
    sort = 'id',
    dist = 'id'
) }}

WITH src_newcustomer AS (

    SELECT
        *
    FROM
        {{ ref('src_newcustomer') }}
),
src_subgrade AS (
    SELECT
        *
    FROM
        {{ ref('src_subgrade') }}
),
src_homeownership AS (
    SELECT
        *
    FROM
        {{ ref('src_homeownership') }}
),
src_purpose AS (
    SELECT
        *
    FROM
        {{ ref('src_purpose') }}
),
src_state AS (
    SELECT
        *
    FROM
        {{ ref('src_state') }}
),
src_verificationstatus AS (
    SELECT
        *
    FROM
        {{ ref('src_verificationstatus') }}
),
seed_fico AS (
    SELECT
        *
    FROM
        {{ ref('seed_fico') }}
)
SELECT
    *
from src_newcustomer nc
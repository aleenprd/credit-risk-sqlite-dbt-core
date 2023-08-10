{{ config(
    materialized = 'table',
    sort = 'id',
    dist = 'id'
) }}

WITH fct_oldcustomer AS (

    SELECT
        *
    FROM
        {{ ref('fct_oldcustomer') }}
),
fct_newcustomer AS (
    SELECT
        *
    FROM
        {{ ref('fct_newcustomer') }}
)
SELECT
    *
FROM
    fct_oldcustomer
UNION ALL
SELECT
    *
FROM
    fct_newcustomer

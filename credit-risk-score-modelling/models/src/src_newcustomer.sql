WITH raw_newcustomer AS (
    SELECT
        *
    FROM
        {{ source('adc-dev', 'newcustomer') }}
)
SELECT
    *
FROM
    raw_newcustomer

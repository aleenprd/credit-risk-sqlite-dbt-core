WITH raw_oldcustomer AS (
    SELECT
        *
    FROM
        {{ source('adc-dev', 'oldcustomer') }}
)
SELECT
    *
FROM
    raw_oldcustomer

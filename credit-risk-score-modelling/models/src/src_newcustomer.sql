WITH raw_newcustomer AS (
    SELECT
        *
    FROM
        {{ source('raw', 'newcustomer') }}
)
SELECT
    *
FROM
    raw_newcustomer

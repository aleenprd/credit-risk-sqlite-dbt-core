WITH raw_oldcustomer AS (
    SELECT
        *
    FROM
        {{ source('raw', 'oldcustomer') }}
)
SELECT
    *
FROM
    raw_oldcustomer

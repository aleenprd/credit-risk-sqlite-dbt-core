WITH raw_subgrade AS (
    SELECT
        *
    FROM
        {{ source('adc-dev', 'subgrade') }}
)
SELECT
    *
FROM
    raw_subgrade

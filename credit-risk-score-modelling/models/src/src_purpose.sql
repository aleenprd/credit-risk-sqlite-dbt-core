WITH raw_purpose AS (
    SELECT
        *
    FROM
        {{ source('adc-dev', 'purpose') }}
)
SELECT
    *
FROM
    raw_purpose

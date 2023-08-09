WITH raw_verifications AS (
    SELECT
        *
    FROM
        {{ source('adc-dev', 'verifications') }}
)
SELECT
    *
FROM
    raw_verifications

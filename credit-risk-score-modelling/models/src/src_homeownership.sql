WITH src_homeownership AS (
    SELECT
        *
    FROM
        {{ source('adc-dev', 'homeownership') }}
)
SELECT
    *
FROM
    src_homeownership

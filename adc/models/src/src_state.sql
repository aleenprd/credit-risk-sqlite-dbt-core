WITH raw_state AS (
    SELECT
        *
    FROM
        {{ source('adc-dev', 'state') }}
)
SELECT
    *
FROM
    raw_state

WITH raw_state AS (
    SELECT
        *
    FROM
        {{ source('raw', 'state') }}
)
SELECT
    *
FROM
    raw_state

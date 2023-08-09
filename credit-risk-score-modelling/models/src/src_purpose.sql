WITH raw_purpose AS (
    SELECT
        *
    FROM
        {{ source('raw', 'purpose') }}
)
SELECT
    *
FROM
    raw_purpose

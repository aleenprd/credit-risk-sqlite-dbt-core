WITH raw_subgrade AS (
    SELECT
        *
    FROM
        {{ source('raw', 'subgrade') }}
)
SELECT
    *
FROM
    raw_subgrade

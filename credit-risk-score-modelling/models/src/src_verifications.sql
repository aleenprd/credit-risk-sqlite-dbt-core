WITH raw_verifications AS (
    SELECT
        *
    FROM
        {{ source('raw', 'verifications') }}
)
SELECT
    *
FROM
    raw_verifications

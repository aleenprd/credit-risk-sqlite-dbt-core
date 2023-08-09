WITH raw_verifications AS (
    SELECT
        *
    FROM
        {{ source('raw', 'verificationstatus') }}
)
SELECT
    *
FROM
    raw_verifications

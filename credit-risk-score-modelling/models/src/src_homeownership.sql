WITH src_homeownership AS (
    SELECT
        *
    FROM
        {{ source('raw', 'homeownership') }}
)
SELECT
    *
FROM
    src_homeownership

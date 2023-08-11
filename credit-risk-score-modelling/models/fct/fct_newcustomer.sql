{{ config(
    sort = 'id',
    dist = 'id'
) }}

WITH src_newcustomer AS (

    SELECT
        *
    FROM
        {{ ref('src_newcustomer') }}
),
src_subgrade AS (
    SELECT
        *
    FROM
        {{ ref('src_subgrade') }}
),
src_homeownership AS (
    SELECT
        *
    FROM
        {{ ref('src_homeownership') }}
),
src_purpose AS (
    SELECT
        *
    FROM
        {{ ref('src_purpose') }}
),
src_state AS (
    SELECT
        *
    FROM
        {{ ref('src_state') }}
),
src_verificationstatus AS (
    SELECT
        *
    FROM
        {{ ref('src_verificationstatus') }}
) {#,
seed_fico AS (
    SELECT
        *
    FROM
        {{ ref('seed_fico') }}
) #}
SELECT
    cust.id AS id,
    CAST(
        loan_status AS bool
    ) AS loan_status,
    CAST(
        CASE
            WHEN loan_amnt IS NULL THEN 0.0
            ELSE CAST(REPLACE(loan_amnt, "k", "") AS float64) * 1000
        END AS INTEGER
    ) AS loan_amnt,
    CAST(
        CASE
            WHEN term IS NULL THEN 0.0
            ELSE CAST(REPLACE(term, "Y", "") AS float64)
        END AS INTEGER
    ) AS term,
    CAST(
        int_rate AS float64
    ) AS int_rate,
    CAST(
        installment AS float64
    ) AS installment,
    sub_grade_id AS sub_grade,
    CASE
        WHEN employment_length IS NULL THEN 0
        ELSE employment_length
    END AS emp_length,
    home_ownership_id AS home_ownership,
    CASE
        WHEN home_ownership_id = 1 THEN TRUE
        ELSE FALSE
    END AS is_mortgage,
    CASE
        WHEN home_ownership_id = 2 THEN TRUE
        ELSE FALSE
    END AS is_rent,
    CASE
        WHEN home_ownership_id = 3 THEN TRUE
        ELSE FALSE
    END AS is_own,
    CASE
        WHEN home_ownership_id = 6 THEN TRUE
        ELSE FALSE
    END AS is_any,
    CASE
        WHEN home_ownership_id IN (
            4,
            5
        ) THEN TRUE
        ELSE FALSE
    END AS is_other,
    annual_inc,
    verification_status_id AS verification_status,
    CASE
        WHEN verification_status_id IN (
            3,
            1
        ) THEN TRUE
        WHEN verification_status_id = 2 THEN FALSE
        ELSE NULL
    END AS is_verified,
    CASE
        WHEN verification_status_id IN (
            3,
            1
        ) THEN TRUE
        WHEN verification_status_id = 2 THEN FALSE
        ELSE NULL
    END AS is_not_verified,
    CASE
        WHEN verification_status_id = 1 THEN TRUE
        WHEN verification_status_id != 1 THEN FALSE
        ELSE NULL
    END AS is_source_verified,
    CASE
        WHEN regexp_contains(
            issued,
            "\\d{4}-\\d{2}-\\d{2}"
        )
        AND LENGTH(issued) = 10 THEN DATE(issued)
        WHEN regexp_contains(
            issued,
            "\\d{4}-\\d{2}-\\d{2}"
        )
        AND LENGTH(issued) = 19 THEN DATE(parse_datetime('%Y-%m-%d %H:%M:%S', issued))
        WHEN regexp_contains(
            issued,
            "\\d{4}-\\d{2}"
        )
        AND LENGTH(issued) = 7 THEN DATE(CONCAT(issued, "-01"))
        WHEN regexp_contains(
            issued,
            "\\d{2}-[A-Za-z]{3}-\\d{2}"
        )
        AND LENGTH(issued) = 9 THEN DATE(
            format_date("%Y-%m-%d", parse_date('%d-%b-%y', issued))
        )
        ELSE NULL
    END AS issue_d,
    purpose_id AS purpose,
    addr_state_id + 51 AS addr_state,
    CAST(
        dti AS float64
    ) AS dti,
    CAST(
        CASE
            WHEN fico_range_low IS NULL THEN fico_range_high
            ELSE fico_range_low
        END AS INTEGER
    ) AS fico_range_low,
    CAST(
        CASE
            WHEN fico_range_high IS NULL THEN fico_range_low
            ELSE fico_range_high
        END AS INTEGER
    ) AS fico_range_high,
    CASE
        WHEN open_acc IS NULL THEN 0
        ELSE CAST(
            open_acc AS INTEGER
        )
    END AS open_acc,
    CASE
        WHEN pub_rec IS NULL THEN 0
        ELSE CAST(
            pub_rec AS INTEGER
        )
    END AS pub_rec,
    CASE
        WHEN revol_bal IS NULL THEN 0
        ELSE CAST(
            revol_bal AS INTEGER
        )
    END AS revol_bal,
    CASE
        WHEN revol_util IS NULL THEN 0.0
        ELSE revol_util
    END AS revol_util,
    CASE
        WHEN mort_acc IS NULL THEN 0
        ELSE CAST(
            mort_acc AS INTEGER
        )
    END AS mort_acc,
    CASE
        WHEN pub_rec_bankruptcies IS NULL THEN 0
        ELSE CAST(
            pub_rec_bankruptcies AS INTEGER
        )
    END AS pub_rec_bankruptcies,
    CAST(
        age AS INTEGER
    ) AS age,
    CAST(
        payment_status AS INTEGER
    ) AS pay_status
FROM
    src_newcustomer cust {# CASE
    WHEN fico_range_low IS NULL THEN (
        SELECT
            avg_fico_low
        FROM
            seed_fico
    )
    ELSE CAST(
        fico_range_low AS INTEGER
    )
END AS fico_range_low,
CASE
    WHEN fico_range_high IS NULL THEN (
        SELECT
            avg_fico_high
        FROM
            seed_fico
    )
    ELSE CAST(
        fico_range_high AS INTEGER
    )
END AS fico_range_high,
#}

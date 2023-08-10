{{ config(
    materialized = "table",
    sort = 'id',
    dist = 'id'
) }}

WITH src_oldcustomer AS (

    SELECT
        *
    FROM
        {{ ref('src_oldcustomer') }}
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
),
src_oldcustomer_annual_inc AS (
    SELECT
        id,
        CAST(REPLACE(SUBSTR(annual_inc, 1, pos - 1), ',', '') AS float64) AS first_number,
        CAST(SUBSTR(annual_inc, pos + 1) AS float64) AS last_number
    FROM
        (
            SELECT
                id,
                SUBSTR(annual_inc, 2, LENGTH(annual_inc) - 2) AS annual_inc,
                instr(
                    annual_inc,
                    ','
                ) AS pos
            FROM
                src_oldcustomer
        )
),
seed_fico AS (
    SELECT
        *
    FROM
        {{ ref('seed_fico') }}
)
SELECT
    cust.id AS id,
    CAST(
        loan_status AS bool
    ) AS loan_status,
    CAST(
        CASE
            WHEN loan_amnt IS NULL THEN 0
            ELSE loan_amnt
        END AS INTEGER
    ) AS loan_amnt,
    CASE
        WHEN term IS NULL THEN 0
        ELSE term
    END AS term,
    CAST(
        int_rate AS float64
    ) AS int_rate,
    CAST(
        installment AS float64
    ) AS installment,
    s.id AS sub_grade,
    CASE
        WHEN emp_length IS NULL THEN 0
        ELSE emp_length
    END AS emp_length,
    ho.id AS home_ownership,
    CASE
        WHEN home_ownership = 'MORTGAGE' THEN TRUE
        ELSE FALSE
    END AS is_mortgage,
    CASE
        WHEN home_ownership = 'RENT' THEN TRUE
        ELSE FALSE
    END AS is_rent,
    CASE
        WHEN home_ownership = 'OWN' THEN TRUE
        ELSE FALSE
    END AS is_own,
    CASE
        WHEN home_ownership = 'ANY' THEN TRUE
        ELSE FALSE
    END AS is_any,
    CASE
        WHEN home_ownership IN (
            'OTHER',
            'NONE'
        ) THEN TRUE
        ELSE FALSE
    END AS is_other,
    CAST(
        ROUND((custai.first_number + custai.last_number) / 2) AS INTEGER
    ) AS annual_inc,
    vs.id AS verification_status,
    CASE
        WHEN UPPER(verification_status) IN (
            'VERIFIED',
            'SOURCE VERIFIED'
        ) THEN TRUE
        WHEN UPPER(verification_status) IN ("NOT VERIFIED") THEN FALSE
        ELSE NULL
    END AS is_verified,
    CASE
        WHEN UPPER(verification_status) IN (
            'VERIFIED',
            'SOURCE VERIFIED'
        ) THEN TRUE
        WHEN UPPER(verification_status) IN ("NOT VERIFIED") THEN FALSE
        ELSE NULL
    END AS is_not_verified,
    CASE
        WHEN UPPER(verification_status) = 'SOURCE VERIFIED' THEN TRUE
        WHEN UPPER(verification_status) != 'SOURCE VERIFIED' THEN FALSE
        ELSE NULL
    END AS is_source_verified,
    DATE(REPLACE(STRING(issue_d), "00:00:00", "")) AS issue_d,
    p.id AS purpose,
    st.id AS addr_state,
    CAST(
        dti AS float64
    ) AS dti,
    CASE
        WHEN fico_range_low IS NULL THEN (
            SELECT
                avg_fico_low
            FROM
                seed_fico
        )
        ELSE fico_range_low
    END AS fico_range_low,
    CASE
        WHEN fico_range_high IS NULL THEN (
            SELECT
                fico_range_high
            FROM
                seed_fico
        )
        ELSE fico_range_high
    END AS fico_range_high,
    CASE
        WHEN open_acc IS NULL THEN 0
        ELSE open_acc
    END AS open_acc,
    CASE
        WHEN pub_rec IS NULL THEN 0
        ELSE pub_rec
    END AS pub_rec,
    CASE
        WHEN revol_bal IS NULL THEN 0
        ELSE revol_bal
    END AS revol_bal,
    CASE
        WHEN revol_util IS NULL THEN 0.0
        ELSE revol_util
    END AS revol_util,
    CASE
        WHEN mort_acc IS NULL THEN 0
        ELSE mort_acc
    END AS mort_acc,
    CASE
        WHEN pub_rec_bankruptcies IS NULL THEN 0
        ELSE pub_rec_bankruptcies
    END AS pub_rec_bankruptcies,
    age,
    pay_status
FROM
    src_oldcustomer cust
    LEFT JOIN src_subgrade s
    ON cust.sub_grade = s.name
    LEFT JOIN src_homeownership ho
    ON cust.home_ownership = ho.name
    LEFT JOIN src_purpose p
    ON LOWER(
        p.name
    ) LIKE CONCAT(
        cust.purpose,
        "%"
    )
    LEFT JOIN src_verificationstatus vs
    ON cust.verification_status = vs.name
    LEFT JOIN src_state st
    ON cust.addr_state = SUBSTR(
        st.name,
        5
    )
    LEFT JOIN src_oldcustomer_annual_inc custai
    ON cust.id = custai.id

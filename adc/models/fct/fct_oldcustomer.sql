{# just do the transformations here #}
WITH src_oldcustomer AS (
    SELECT
        *
    FROM
        {{ ref('src_oldcustomer') }}
)
SELECT
    id,
    {# There is no boolean type in SQLite. The closest thing would probably
    ust be USING an INTEGER column
    AND storing 0 for FALSE
    AND 1 for TRUE.https:// stackoverflow.com / questions / 46210704 / CAST - BOOLEAN - TO - INT - IN - sqlite #}
    loan_status,
    loan_amnt,
    term,
    int_rate,
    {# here we need to fetch the corresponding id from the src_subgrade table#}
    installment,
    sub_grade,
    CASE
        WHEN emp_length IS NULL THEN 0
        ELSE emp_length
    END AS emp_length,
    home_ownership,
    {# here we need to fetch the corresponding id from the src_homeownership table#}
    if home_ownership == 'MORTGAGE' THEN TRUE
    ELSE FALSE
END AS is_mortgage,
if is_rent == 'RENT' THEN TRUE
ELSE FALSE
END AS is_rent,
0 AS is_own,
0 AS is_any,
0 AS is_other,
{# aici la old customer tre sa spargem in doua numere, sa luam punctul de mijloc media
si aia e,
la nou lasam asa,
CHECK NOT NULL #}
0 AS annual_inc,
verification_status {# here we need to fetch the corresponding id from the src_verification table#},
CASE
    WHEN verification_status IN (
        'VERIFIED',
        'SOURCE_VERIFIED'
    ) THEN 1
    ELSE 0
END AS is_verified,
is_not_verified,
is_source_verified,
{# oldcustomer called issue_d seems to contain only timestamps at 00:00:00, bring them to common format 
YYYY-mm-dd and check if string of date corresponds to this regex or see if yhere are datetime functions idk #}
issue_d,
{# here we need to fetch the corresponding id from the src_purpose table#}
purpose,
{# here we need to fetch the corresponding id from the src_state table#}
addr_state,
dti,
{# {{ seed('statistics.plm') }} 
definetely use seeds and argument that we don't want this number to change dinamically. 
make an argument for version control #} 
case when fico_range_low is null then round(avg(max(fico_range_low), min(fico_range_low))) end as fico_range_low,
case when fico_range_high is null then round(avg(max(fico_range_high), min(fico_range_high))) end as fico_range_high fico_range_high,
case when open_acc is null then 0 end as open_acc,
case when pub_rec is null then 0 end as pub_rec,
case when revo_bal is null then 0 end as revo_bal,
case when revol_util is null then 0.0 end as revol_util,
case when mort_acc is null then 0 end as mort_acc,
case when pub_rec_bankruptcies is null then 0 end as pub_rec_bankruptcies,
age,
pay_status
FROM
    src_oldcustomer

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
    AND 1 for TRUE.https:// stackoverflow.com / questions / 46210704 / CAST - BOOLEAN - TO - INT - IN - sqlite 
    select cast(loan_status as bool) as loan_status from raw.api_newcustomer; #}
    loan_status,
    loan_amnt,
    term,
    int_rate, # select cast(int_rate as FLOAT64) from raw.api_oldcustomer;
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
is_not_verified, {# very fucking redundant #}
is_source_verified,
{# oldcustomer called issue_d seems to contain only timestamps at 00:00:00, bring them to common format 
YYYY-mm-dd and check if string of date corresponds to this regex or see if yhere are datetime functions idk #}
issue_d, {# select substr(issue_d, 1, instr(issue_d, ' ')) as issue_d from main.api_oldcustomer limit 5 THERE IS NO DATE TYPE IN SQLITE #}
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


select 
	first_number,
	last_number,
	round((first_number + last_number) / 2) as annual_inc from (
	select
		cast(replace(substr(annual_inc, 1, pos - 1), ',', '') as real) +1 as first_number,
		cast(substr(annual_inc, pos + 1) as real) as last_number
	from (
		select 
			substr(annual_inc, 2, length(annual_inc) - 2) as annual_inc,
			instr(annual_inc,',') AS pos from main.api_oldcustomer
		)
	)
limit 5;


{# SQLITE doesn't come with regexp by default so we can 
we can install the fucking perl module with
sudo apt-get install sqlite3-pcre
we can use this select statement to load the fucking module
SELECT load_extension('/usr/lib/sqlite3/pcre.so');
alternatively, fuck this shit and let's use a real database or DWH like PostGres or Snowflake or BQ or anything really
I could impress by having this sqlite database running with docker compose and we connect to it I guess
#}

# Case YYYY-mm-dd
select date(replace(issued, "00:00:00", "")) as issue_d 
from `credit-risk-395413.raw.api_newcustomer`
where regexp_contains(issued, r"\b\d{4}-\d{2}-\d{2}\b")
and length(issued) = 10
limit 5;

select issued from `credit-risk-395413.raw.api_newcustomer` limit 20;
# 05-Jan-11
# 2016-01

# Case YYYY-mm
select date(concat(replace(issued, "00:00:00", ""), "-01")) as issue_d 
from `credit-risk-395413.raw.api_newcustomer`
where regexp_contains(issued, r"\b\d{4}-\d{2}\b") 
and length(issued) = 7
limit 5;

# Case mm-Mon-YYYY (dates are from 2007 onwards so no need to worry about pre-21st century)
select FORMAT_DATE("%mm-%dd-/%yyyy",PARSE_DATE('%d-%b-%y', issued))
  (concat(
  substr(issued, 1, 7),
  "20",
  substr(issued, 1, 2)
  )) as issue_d 
from `credit-risk-395413.raw.api_newcustomer`
where regexp_contains(issued, r"\b\d{2}-[A-Za-z]{3}-\d{2}\b") 
and length(issued) = 9
limit 20;

select issued, FORMAT_DATE("%Y-%m-%d",PARSE_DATE('%d-%b-%y', issued)) 
from `credit-risk-395413.raw.api_newcustomer`
where regexp_contains(issued, r"\b\d{2}-[A-Za-z]{3}-\d{2}\b") 
and length(issued) = 9
limit 10;

### AICI ASTA E FINALUL
select 
  case 
    when regexp_contains(issued, r"\b\d{2}-[A-Za-z]{3}-\d{2}\b") and length(issued) = 9
      then date(FORMAT_DATE("%Y-%m-%d",PARSE_DATE('%d-%b-%y', issued)))
    when regexp_contains(issued, r"\b\d{4}-\d{2}\b") and length(issued) = 7
      then date(concat(replace(issued, "00:00:00", ""), "-01"))
    when regexp_contains(issued, r"\b\d{4}-\d{2}-\d{2}\b") and length(issued) = 10
      then date(replace(issued, "00:00:00", ""))
    else null
  end as issued
from `credit-risk-395413.raw.api_newcustomer`
limit 10


select 
verification_status,
CASE
    WHEN upper(verification_status) IN (
        'VERIFIED',
        'SOURCE VERIFIED'
    ) THEN 1
    WHEN upper(verification_status) in ("NOT VERIFIED")
      then 0
    else null
END AS is_verified
from `credit-risk-395413.raw.api_oldcustomer`
limit 50;

select
verification_status,
CASE 
  WHEN upper(verification_status) IN (
        'VERIFIED',
        'SOURCE VERIFIED'
    ) THEN 0
    WHEN upper(verification_status) in ("NOT VERIFIED")
      then 1
    else null
END AS is_not_verified
from `credit-risk-395413.raw.api_oldcustomer`
limit 50;


select emp_length,
CASE
  WHEN emp_length IS NULL THEN 0
  ELSE emp_length
end as emp_length
from `credit-risk-395413.raw.api_oldcustomer`
limit 50;

select r.id as addr_state
from `credit-risk-395413.raw.api_oldcustomer` l
left join raw.api_state r on l.addr_state = substr(r.name, 5) 
limit 5;

select r.id as sub_grade
from `credit-risk-395413.raw.api_oldcustomer` l
left join raw.api_subgrade r on l.sub_grade = r.name 
limit 5;

select r.id as home_ownership
from `credit-risk-395413.raw.api_oldcustomer` l
left join raw.api_homeownership r on l.home_ownership = r.name 
limit 5;

select
case when home_ownership = 'MORTGAGE' THEN TRUE
    ELSE FALSE
END AS is_mortgage
from `credit-risk-395413.raw.api_oldcustomer`
limit 10;

select r.id as purpose
from `credit-risk-395413.raw.api_oldcustomer` l
left join raw.api_purpose r on regexp_contains(lower(r.name), l.purpose)
limit 20;
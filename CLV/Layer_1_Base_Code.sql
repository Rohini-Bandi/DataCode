--the hardcoded dates are the tier1 event dates historically provided by marketing business team, for now we are looking at the
--data for c360 post '2020-10-31'

drop table if exists np_marketinganalytics.DACIMDS_c360_DIM_CALENDAR;
create table np_marketinganalytics.DACIMDS_c360_DIM_CALENDAR as
with base as
(
    SELECT
    distinct
    a.cal_dt,a.fsc_yr_nbr,a.fsc_mon_nbr,a.fsc_mon_day_nbr,a.fsc_wk_nbr,a.wk_day_nbr,a.fsc_yr_bgn_dt, a.fsc_yr_end_dt,
    a.wk_in_mon_cnt,a.fsc_mon_wk_nbr, a.fsc_mon_bgn_dt,a.fsc_mon_end_dt,a.fsc_wk_bgn_dt,a.fsc_wk_end_dt,a.fsc_day_nbr,
    a.jul_day_nbr,a.fsc_yr_per_nbr,a.fsc_yr_wk_nbr,a.ly_cal_dt,b.fsc_qtr_wk_nbr,b.fsc_qtr_nbr,
    case when
    --2016
(a.cal_dt between '2016-03-31' and '2016-04-06') or
(a.cal_dt between '2016-04-07' and '2016-04-27') or
(a.cal_dt between '2016-05-19' and '2016-05-25') or
(a.cal_dt between '2016-05-26' and '2016-06-06') or
(a.cal_dt between '2016-06-09' and '2016-06-15') or
(a.cal_dt between '2016-06-16' and '2016-06-22') or
(a.cal_dt between '2016-06-23' and '2016-06-29') or
(a.cal_dt between '2016-06-30' and '2016-07-06') or
(a.cal_dt between '2016-08-25' and '2016-08-31') or
(a.cal_dt between '2016-09-01' and '2016-09-07') or
(a.cal_dt between '2016-11-17' and '2016-11-23') or
(a.cal_dt between '2016-11-24' and '2016-11-30') or
--2017
(a.cal_dt between '2017-03-30' and '2017-04-05') or
(a.cal_dt between '2017-04-06' and '2017-04-10') or
(a.cal_dt between '2017-05-18' and '2017-05-24') or
(a.cal_dt between '2017-05-25' and '2017-06-07') or
(a.cal_dt between '2017-06-08' and '2017-06-14') or
(a.cal_dt between '2017-06-15' and '2017-06-21') or
(a.cal_dt between '2017-06-22' and '2017-06-28') or
(a.cal_dt between '2017-06-29' and '2017-07-05') or
(a.cal_dt between '2017-08-24' and '2017-08-30') or
(a.cal_dt between '2017-08-31' and '2017-09-13') or
(a.cal_dt between '2017-11-16' and '2017-11-22') or
(a.cal_dt between '2017-11-23' and '2017-11-29') or
--2018
(a.cal_dt between '2018-03-15' and '2018-03-21') or
(a.cal_dt between '2018-03-22' and '2018-03-28') or
(a.cal_dt between '2018-05-17' and '2018-05-23') or
(a.cal_dt between '2018-05-24' and '2018-06-06') or
(a.cal_dt between '2018-06-07' and '2018-06-13') or
(a.cal_dt between '2018-06-14' and '2018-06-20') or
(a.cal_dt between '2018-06-21' and '2018-06-27') or
(a.cal_dt between '2018-06-28' and '2018-07-07') or
(a.cal_dt between '2018-08-23' and '2018-08-29') or
(a.cal_dt between '2018-08-30' and '2018-09-05') or
(a.cal_dt between '2018-11-15' and '2018-11-21') or
(a.cal_dt between '2018-11-22' and '2018-11-28') or
--2019
(a.cal_dt between '2019-04-04' and '2019-04-17') or
(a.cal_dt between '2019-04-11' and '2019-04-17') or
(a.cal_dt between '2019-05-16' and '2019-05-22') or
(a.cal_dt between '2019-05-23' and '2019-05-29') or
(a.cal_dt between '2019-06-06' and '2019-06-19') or
(a.cal_dt between '2021-06-13' and '2019-06-19') or
(a.cal_dt between '2019-06-27' and '2019-07-10') or
(a.cal_dt between '2019-07-04' and '2019-07-10') or
(a.cal_dt between '2019-08-22' and '2019-08-28') or
(a.cal_dt between '2019-08-29' and '2019-09-04') or
(a.cal_dt between '2019-11-21' and '2019-11-27') or
(a.cal_dt between '2019-11-28' and '2019-12-04') or
--2020
(a.cal_dt between '2020-04-02' and '2020-04-15') or
(a.cal_dt between '2020-05-14' and '2020-05-20') or
(a.cal_dt between '2020-05-21' and '2020-05-27') or
(a.cal_dt between '2020-06-11' and '2020-06-24') or
(a.cal_dt between '2020-06-25' and '2020-07-08') or
(a.cal_dt between '2020-08-27' and '2020-09-09') or
(a.cal_dt between '2020-11-19' and '2020-11-25') or
(a.cal_dt between '2020-11-26' and '2020-12-02') or
--2021
(a.cal_dt between '2021-04-08' and '2021-04-21') or
(a.cal_dt between '2021-05-20' and '2021-06-02') or
(a.cal_dt between '2021-06-10' and '2021-06-23') or
(a.cal_dt between '2021-06-24' and '2021-07-07') or
(a.cal_dt between '2021-08-26' and '2021-09-01') or
(a.cal_dt between '2021-09-02' and '2021-09-08') or
(a.cal_dt between '2021-11-18' and '2021-11-24') or
(a.cal_dt between '2021-11-25' and '2021-12-01') or
--2022
(a.cal_dt between '2022-03-14' and '2022-03-25') or
(a.cal_dt between '2022-04-07' and '2022-05-04') or
(a.cal_dt between '2022-05-19' and '2022-06-01') or
(a.cal_dt between '2022-06-09' and '2022-07-06') or
(a.cal_dt between '2022-07-11' and '2022-07-22') or
(a.cal_dt between '2022-08-25' and '2022-09-07') or
(a.cal_dt between '2022-09-12' and '2022-09-23') or
(a.cal_dt between '2022-10-27' and '2022-11-30') or
(a.cal_dt between '2022-12-01' and '2022-12-24') or
(a.cal_dt between '2022-11-18' and '2022-11-24') or
(a.cal_dt between '2022-11-25' and '2022-12-01') or
(a.cal_dt between '2023-01-16' and '2023-01-27') or
--2023
(a.cal_dt between '2023-03-20' and '2023-03-30') or
(a.cal_dt between '2023-04-06' and '2023-05-31') or
(a.cal_dt between '2023-06-08' and '2023-07-12') or
(a.cal_dt between '2023-07-17' and '2023-07-27') or
(a.cal_dt between '2023-08-24' and '2023-09-13') or
(a.cal_dt between '2023-09-18' and '2023-09-28') or
(a.cal_dt between '2023-10-26' and '2023-12-24') or
(a.cal_dt between '2024-01-15' and '2024-01-25')
then 1 else 0 end as Tier_1_Flag
from lowes_tables.i0036_fsc_cal_cnv a
join lowes_tables.I0293_WK_TO_QTR b
on a.fsc_wk_end_dt = cast(b.fsc_wk_end_dt as varchar)
WHERE a.cal_dt >=  '2020-10-31'
AND a.cal_dt <= (SELECT cast(date_add('day',-1,cast(fsc_mon_bgn_dt as date)) as varchar)
FROM lowes_tables.i0036_fsc_cal_cnv
WHERE cal_dt = (SELECT max(fsc_wk_end_dt) FROM customer_360.transaction_view_full_custrfm))
),
month_mapping as
(
    select
    fsc_yr_nbr,
    FSC_MON_NBR,
    row_number() over (partition by x order by fsc_yr_nbr desc, FSC_MON_NBR desc)  as roll_FSC_MON_NBR
    from
    (
        SELECT
        distinct
        a.fsc_yr_nbr,
        a.FSC_MON_NBR,
        1 as x
        FROM base a
    ) a
)
SELECT distinct
a.cal_dt,a.fsc_yr_nbr,a.fsc_mon_nbr,a.fsc_mon_day_nbr,a.fsc_wk_nbr,a.wk_day_nbr,a.fsc_yr_bgn_dt,a.fsc_yr_end_dt,a.wk_in_mon_cnt, a.fsc_mon_wk_nbr,a.fsc_mon_bgn_dt,a.fsc_mon_end_dt,
a.fsc_wk_bgn_dt,a.fsc_wk_end_dt,a.fsc_day_nbr,a.jul_day_nbr,a.fsc_yr_per_nbr, a.fsc_yr_wk_nbr,a.ly_cal_dt,fsc_qtr_wk_nbr,fsc_qtr_nbr, roll_FSC_MON_NBR,
ceil(roll_FSC_MON_NBR/3) as roll_fsc_qtr_nbr, ceil(roll_FSC_MON_NBR/6) as roll_fsc_hlf_yr_nbr, ceil(roll_FSC_MON_NBR/12) as roll_fsc_yr_nbr, Tier_1_Flag
FROM base a
join month_mapping b
on a.fsc_mon_nbr = b.fsc_mon_nbr
and a.fsc_yr_nbr = b.fsc_yr_nbr;

--table which maps the rolling fiscal quarter to actual fiscal years/fiscal quarter combination - in this logic,
--rolling fiscal quarter = 0 is FY2022 Qtr4 and rolling fiscal quarter = 8 is FY2020 Qtr4 - total 9 quarters of data:
drop table np_marketinganalytics.DACIMDS_c360_DIM_CALENDAR_YEAR_QUARTER_MAP;
create table np_marketinganalytics.DACIMDS_c360_DIM_CALENDAR_YEAR_QUARTER_MAP as
select fsc_yr_nbr, fsc_qtr_nbr, roll_fsc_qtr_nbr
from np_marketinganalytics.DACIMDS_c360_DIM_CALENDAR
group by fsc_yr_nbr, fsc_qtr_nbr, roll_fsc_qtr_nbr

--the sample of population that this dataset pipeline is generated for. In production, this piece of code needs to be removed
--and the logic where this table is joined in subsequent queries below needs to be removed
drop table np_marketinganalytics.customer_sample_base_for_pipeline;
create table np_marketinganalytics.customer_sample_base_for_pipeline as
select
concat(coalesce(c360_id,'unknown'),'---',coalesce(customer_type,'unknown'),'---',coalesce(organization_id, 'unknown')) as ID,
c360_id,
customer_type,
organization_id
from
(
    select
    concat(coalesce(c360_id,'unknown'),'---',coalesce(customer_type,'unknown'),'---',coalesce(organization_id, 'unknown')) as ID,
    c360_id,
    customer_type,
    organization_id
    from customer_360.transaction_view_full_custrfm
    group by
    concat(coalesce(c360_id,'unknown'),'---',coalesce(customer_type,'unknown'),'---',coalesce(organization_id, 'unknown')),
    c360_id,
    customer_type,
    organization_id
    having count(distinct concat(cast(fsc_yr_nbr as varchar), cast(fsc_qtr_nbr as varchar))) = 12
) a
group by concat(coalesce(c360_id,'unknown'),'---',coalesce(customer_type,'unknown'),'---',coalesce(organization_id, 'unknown')),
c360_id,
customer_type,
organization_id;

--there are duplicate zip_codes found for same 'ID' combination, so going by arbitrary picking of zipcodes
--similar erroneous behavior noticed in the card holder flags and military flags,
--going by the logic, once a flag is noticed against the record, flag the entire record
--demogs data:
drop table if exists np_marketinganalytics.MDS_C360_DEMOGS;
create table np_marketinganalytics.MDS_C360_DEMOGS as
with base as
(
select
concat(coalesce(a.c360_id,'unknown'),'---',coalesce(a.customer_type,'unknown'),'---',coalesce(a.organization_id, 'unknown')) as ID,
a.c360_id,
a.customer_type,
a.organization_id,
current_classification,
military_flag,
lba_cardholder,
lar_cardholder,
pb_cardholder,
lbr_cardholder,
lcc_cardholder,
age,
household_size,
gender,
number_of_children,
household_income,
ethnicity,
postal_code
from customer_360.customer_360_view a
join np_marketinganalytics.customer_sample_base_for_pipeline b
on concat(coalesce(a.c360_id,'unknown'),'---',coalesce(a.customer_type,'unknown'),'---',coalesce(a.organization_id, 'unknown')) = b.ID
group by concat(coalesce(a.c360_id,'unknown'),'---',coalesce(a.customer_type,'unknown'),'---',coalesce(a.organization_id, 'unknown')),
a.c360_id, a.customer_type, a.organization_id, current_classification, military_flag, lba_cardholder, lar_cardholder, pb_cardholder, lbr_cardholder, lcc_cardholder, age, household_size, gender, number_of_children, household_income, ethnicity, postal_code
)
select
ID,
c360_id,
customer_type,
organization_id,
current_classification,
max(case when military_flag = True then 1 else 0 end) as military_flag,
max(case when lba_cardholder = True then 1 else 0 end) as lba_cardholder,
max(case when lar_cardholder = True then 1 else 0 end) as lar_cardholder,
max(case when pb_cardholder = True then 1 else 0 end) as pb_cardholder,
max(case when lbr_cardholder = True then 1 else 0 end) as lbr_cardholder,
max(case when lcc_cardholder = True then 1 else 0 end) as lcc_cardholder,
max(postal_code) as postal_code,
age,
household_size,
gender,
number_of_children,
household_income,
ethnicity

from base
group by ID, c360_id, customer_type, organization_id, current_classification, age, household_size, gender, number_of_children, household_income, ethnicity;
--------

--base tables for layer_2 aggregation:
--sales behavior
--rolling up count, qty, dollar value for orders, returns and cancelled at a customer, rolling quarter, channel level:
drop table if exists np_marketinganalytics.MDS_C360_BEHAVIOR_CUSTOMER_ROLLINGQUARTER_CHANNEL_LEVEL_ORDER_COUNT_QTY_VALUE_MARGIN;
create table np_marketinganalytics.MDS_C360_BEHAVIOR_CUSTOMER_ROLLINGQUARTER_CHANNEL_LEVEL_ORDER_COUNT_QTY_VALUE_MARGIN as
select
concat(coalesce(c360_id,'unknown'),'---',coalesce(customer_type,'unknown'),'---',coalesce(organization_id, 'unknown')) as ID,
c360_id,
customer_type,
organization_id,
a.fsc_yr_nbr,
a.fsc_qtr_nbr,
roll_fsc_qtr_nbr,
sum(case when trim(channel) = 'Online' then original_order_count end) as online_order_count,
sum(case when trim(channel) = 'Online' then original_order_quantity end) as online_order_qty,
sum(case when trim(channel) = 'Online' then original_order_value end) as online_order_value,
sum(case when trim(channel) = 'Online' then return_count end) as online_return_count,
sum(case when trim(channel) = 'Online' then return_quantity end) as online_return_quantity,
sum(case when trim(channel) = 'Online' then return_value end) as online_return_value,
sum(case when trim(channel) = 'Online' then cancelled_count end) as online_cancelled_count,
sum(case when trim(channel) = 'Online' then cancelled_quantity end) as online_cancelled_quantity,
sum(case when trim(channel) = 'Online' then cancelled_value end) as online_cancelled_value,
sum(case when trim(channel) = 'Online' then total_margin end) as online_total_margin,

sum(case when trim(channel) = 'Store' then original_order_count end) as store_order_count,
sum(case when trim(channel) = 'Store' then original_order_quantity end) as store_order_qty,
sum(case when trim(channel) = 'Store' then original_order_value end) as store_order_value,
sum(case when trim(channel) = 'Store' then return_count end) as store_return_count,
sum(case when trim(channel) = 'Store' then return_quantity end) as store_return_quantity,
sum(case when trim(channel) = 'Store' then return_value end) as store_return_value,
sum(case when trim(channel) = 'Store' then cancelled_count end) as store_cancelled_count,
sum(case when trim(channel) = 'Store' then cancelled_quantity end) as store_cancelled_quantity,
sum(case when trim(channel) = 'Store' then cancelled_value end) as store_cancelled_value,
sum(case when trim(channel) = 'Store' then total_margin end) as store_total_margin,

sum(case when trim(channel) is null then original_order_count end) as other_order_count,
sum(case when trim(channel) is null then original_order_quantity end) as other_order_qty,
sum(case when trim(channel) is null then original_order_value end) as other_order_value,
sum(case when trim(channel) is null then return_count end) as other_return_count,
sum(case when trim(channel) is null then return_quantity end) as other_return_quantity,
sum(case when trim(channel) is null then return_value end) as other_return_value,
sum(case when trim(channel) is null then cancelled_count end) as other_cancelled_count,
sum(case when trim(channel) is null then cancelled_quantity end) as other_cancelled_quantity,
sum(case when trim(channel) is null then cancelled_value end) as other_cancelled_value,
sum(case when trim(channel) is null then total_margin end) as other_total_margin

from customer_360.transaction_view_full_custrfm a
join np_marketinganalytics.DACIMDS_c360_DIM_CALENDAR_YEAR_QUARTER_MAP b
on a.fsc_yr_nbr = b.fsc_yr_nbr
and a.fsc_qtr_nbr = b.fsc_qtr_nbr
join np_marketinganalytics.customer_sample_base_for_pipeline c
on concat(coalesce(a.c360_id,'unknown'),'---',coalesce(a.customer_type,'unknown'),'---',coalesce(a.organization_id, 'unknown')) = c.ID

group by
concat(coalesce(c360_id,'unknown'),'---',coalesce(customer_type,'unknown'),'---',coalesce(organization_id, 'unknown')),
c360_id,
customer_type,
organization_id,
a.fsc_yr_nbr,
a.fsc_qtr_nbr,
roll_fsc_qtr_nbr;
--
--numbers matching between created table roll-up and final table roll-up:
--QC-queries:
--for created table fsc_qtr_level:
--select roll_fsc_qtr_nbr, sum(online_order_value), sum(store_order_value), sum(other_order_value), count(distinct c360_id)
--from np_marketinganalytics.MDS_C360_BEHAVIOR_CUSTOMER_ROLLINGQUARTER_CHANNEL_LEVEL_ORDER_COUNT_QTY_VALUE_MARGIN
--group by 1
--queries:
--for source table fsc_qtr_level:
-- select
-- fsc_yr_nbr,
-- fsc_qtr_nbr,
-- sum(original_order_value)
-- from customer_360.transaction_view_full_custrfm a
-- join np_marketinganalytics.customer_sample_base_for_pipeline c
-- on concat(coalesce(a.c360_id,'unknown'),'---',coalesce(a.customer_type,'unknown'),'---',coalesce(a.organization_id, 'unknown')) = c.ID
-- group by fsc_yr_nbr,fsc_qtr_nbr
-- order by fsc_yr_nbr desc, fsc_qtr_nbr desc
--

--rolling up final order dollar value for each customer rollingfiscal quarter combination across tender type,store/online:
drop table if exists np_marketinganalytics.MDS_C360_BEHAVIOR_CUSTOMER_ROLLINGQUARTER_LEVEL_TENDER_STORE_ONLINE_ORDER_VALUE;
create table np_marketinganalytics.MDS_C360_BEHAVIOR_CUSTOMER_ROLLINGQUARTER_LEVEL_TENDER_STORE_ONLINE_ORDER_VALUE as
select
concat(coalesce(c360_id,'unknown'),'---',coalesce(customer_type,'unknown'),'---',coalesce(organization_id, 'unknown')) as ID,
a.c360_id,
a.customer_type,
a.organization_id,
a.fsc_yr_nbr,
a.fsc_qtr_nbr,
roll_fsc_qtr_nbr,
sum(case when payment_method in ('LAR','LBA','LBR','P&B') then final_order_value end) as pro_final_order_value,
sum(case when payment_method in ('LCC') then final_order_value end) as diy_final_order_value,
sum(case when payment_method in ('Cash + cheque','GiftCard','other payments') then final_order_value end) as other_final_order_value,

sum(case when payment_method in ('LAR','LBA','LBR','P&B') then final_order_value_online end) as pro_final_order_value_online,
sum(case when payment_method in ('LCC') then final_order_value_online end) as diy_final_order_value_online,
sum(case when payment_method in ('Cash + cheque','GiftCard','other payments') then final_order_value_online end) as other_final_order_value_online,

sum(case when payment_method in ('LAR','LBA','LBR','P&B') then final_order_value_store end) as pro_final_order_value_store,
sum(case when payment_method in ('LCC') then final_order_value_store end) as diy_final_order_values_store,
sum(case when payment_method in ('Cash + cheque','GiftCard','other payments') then final_order_value_store end) as other_final_order_value_store

from customer_360.transaction_view_payments_fiscal_custrfm a
join np_marketinganalytics.DACIMDS_c360_DIM_CALENDAR_YEAR_QUARTER_MAP b
on a.fsc_yr_nbr = cast(b.fsc_yr_nbr as string)
and a.fsc_qtr_nbr = cast(b.fsc_qtr_nbr as string)
join np_marketinganalytics.customer_sample_base_for_pipeline c
on concat(coalesce(a.c360_id,'unknown'),'---',coalesce(a.customer_type,'unknown'),'---',coalesce(a.organization_id, 'unknown')) = c.ID

group by
concat(coalesce(a.c360_id,'unknown'),'---',coalesce(a.customer_type,'unknown'),'---',coalesce(a.organization_id, 'unknown')),
a.c360_id,
a.customer_type,
a.organization_id,
a.fsc_yr_nbr,
a.fsc_qtr_nbr,
roll_fsc_qtr_nbr;
----QC-queries:
-- for create table rollup:
--select roll_fsc_qtr_nbr, sum(pro_final_order_value+diy_final_order_value+other_final_order_value),
-- sum(pro_final_order_value_online+diy_final_order_value_online+other_final_order_value_online),
-- sum(pro_final_order_value_store+diy_final_order_values_store+other_final_order_value_store)
-- from np_marketinganalytics.MDS_C360_BEHAVIOR_CUSTOMER_ROLLINGQUARTER_LEVEL_TENDER_STORE_ONLINE_ORDER_VALUE
-- group by 1
-- for source table rollup:
-- select fsc_yr_nbr, fsc_qtr_nbr, sum(final_order_value)
-- from customer_360.transaction_view_payments_fiscal_custrfm a
-- join np_marketinganalytics.customer_sample_base_for_pipeline c
-- on concat(coalesce(a.c360_id,'unknown'),'---',coalesce(a.customer_type,'unknown'),'---',coalesce(a.organization_id, 'unknown')) = c.ID
-- where fsc_wk_end_dt >=  '2020-10-31'
-- group by fsc_yr_nbr, fsc_qtr_nbr
-- order by fsc_yr_nbr desc, fsc_qtr_nbr desc

--rolling up count, qty, dollar value for orders, returns and cancelled at a customer, rolling quarter, channel, merch_category_id level:
drop table if exists np_marketinganalytics.MDS_C360_BEHAVIOR_CUSTOMER_ROLLINGQUARTER_CHANNEL_MD_LEVEL_ORDER_COUNT_QTY_VALUE;
create table np_marketinganalytics.MDS_C360_BEHAVIOR_CUSTOMER_ROLLINGQUARTER_CHANNEL_MD_LEVEL_ORDER_COUNT_QTY_VALUE as
select
concat(coalesce(a.c360_id,'unknown'),'---',coalesce(a.customer_type,'unknown'),'---',coalesce(a.organization_id, 'unknown')) as ID,
a.c360_id,
a.customer_type,
a.organization_id,
a.fsc_yr_nbr,
a.fsc_qtr_nbr,
roll_fsc_qtr_nbr,
merch_category_id,
sum(case when channel = 'Online' then original_order_count end) as online_order_count,
sum(case when channel = 'Online' then original_order_quantity end) as online_order_qty,
sum(case when channel = 'Online' then original_order_value end) as online_order_value,
sum(case when channel = 'Online' then return_count end) as online_return_count,
sum(case when channel = 'Online' then return_quantity end) as online_return_quantity,
sum(case when channel = 'Online' then return_value end) as online_return_value,
sum(case when channel = 'Online' then cancelled_count end) as online_cancelled_count,
sum(case when channel = 'Online' then cancelled_quantity end) as online_cancelled_quantity,
sum(case when channel = 'Online' then cancelled_value end) as online_cancelled_value,

sum(case when channel = 'Store' then original_order_count end) as store_order_count,
sum(case when channel = 'Store' then original_order_quantity end) as store_order_qty,
sum(case when channel = 'Store' then original_order_value end) as store_order_value,
sum(case when channel = 'Store' then return_count end) as store_return_count,
sum(case when channel = 'Store' then return_quantity end) as store_return_quantity,
sum(case when channel = 'Store' then return_value end) as store_return_value,
sum(case when channel = 'Store' then cancelled_count end) as store_cancelled_count,
sum(case when channel = 'Store' then cancelled_quantity end) as store_cancelled_quantity,
sum(case when channel = 'Store' then cancelled_value end) as store_cancelled_value,

sum(case when channel is null then original_order_count end) as other_order_count,
sum(case when channel is null then original_order_quantity end) as other_order_qty,
sum(case when channel is null then original_order_value end) as other_order_value,
sum(case when channel is null then return_count end) as other_return_count,
sum(case when channel is null then return_quantity end) as other_return_quantity,
sum(case when channel is null then return_value end) as other_return_value,
sum(case when channel is null then cancelled_count end) as other_cancelled_count,
sum(case when channel is null then cancelled_quantity end) as other_cancelled_quantity,
sum(case when channel is null then cancelled_value end) as other_cancelled_value

from customer_360.transaction_view_merch_div_weekly a
join np_marketinganalytics.DACIMDS_c360_DIM_CALENDAR_YEAR_QUARTER_MAP b
on a.fsc_yr_nbr = b.fsc_yr_nbr
and a.fsc_qtr_nbr = b.fsc_qtr_nbr
join np_marketinganalytics.customer_sample_base_for_pipeline c
on concat(coalesce(a.c360_id,'unknown'),'---',coalesce(a.customer_type,'unknown'),'---',coalesce(a.organization_id, 'unknown')) = c.ID

group by
concat(coalesce(a.c360_id,'unknown'),'---',coalesce(a.customer_type,'unknown'),'---',coalesce(a.organization_id, 'unknown')),
a.c360_id,
a.customer_type,
a.organization_id,
a.fsc_yr_nbr,
a.fsc_qtr_nbr,
roll_fsc_qtr_nbr,
merch_category_id;

--rolling up count, qty, dollar value for orders, returns and cancelled at a customer, rolling quarter, channel, merch_sub_category_id level:

--stage_4: count/qty/sales for original/return/cancelled (final = original - return - cancelled) for different merch sub div categ:
drop table if exists np_marketinganalytics.MDS_C360_BEHAVIOR_CUSTOMER_ROLLINGQUARTER_CHANNEL_MSD_LEVEL_ORDER_COUNT_QTY_VALUE;
create table np_marketinganalytics.MDS_C360_BEHAVIOR_CUSTOMER_ROLLINGQUARTER_CHANNEL_MSD_LEVEL_ORDER_COUNT_QTY_VALUE as
select
concat(coalesce(a.c360_id,'unknown'),'---',coalesce(a.customer_type,'unknown'),'---',coalesce(a.organization_id, 'unknown')) as ID,
a.c360_id,
a.customer_type,
a.organization_id,
a.fsc_yr_nbr,
a.fsc_qtr_nbr,
roll_fsc_qtr_nbr,
merch_sub_category_id,
sum(case when channel = 'Online' then original_order_count end) as online_order_count,
sum(case when channel = 'Online' then original_order_quantity end) as online_order_qty,
sum(case when channel = 'Online' then original_order_value end) as online_order_value,
sum(case when channel = 'Online' then return_count end) as online_return_count,
sum(case when channel = 'Online' then return_quantity end) as online_return_quantity,
sum(case when channel = 'Online' then return_value end) as online_return_value,
sum(case when channel = 'Online' then cancelled_count end) as online_cancelled_count,
sum(case when channel = 'Online' then cancelled_quantity end) as online_cancelled_quantity,
sum(case when channel = 'Online' then cancelled_value end) as online_cancelled_value,

sum(case when channel = 'Store' then original_order_count end) as store_order_count,
sum(case when channel = 'Store' then original_order_quantity end) as store_order_qty,
sum(case when channel = 'Store' then original_order_value end) as store_order_value,
sum(case when channel = 'Store' then return_count end) as store_return_count,
sum(case when channel = 'Store' then return_quantity end) as store_return_quantity,
sum(case when channel = 'Store' then return_value end) as store_return_value,
sum(case when channel = 'Store' then cancelled_count end) as store_cancelled_count,
sum(case when channel = 'Store' then cancelled_quantity end) as store_cancelled_quantity,
sum(case when channel = 'Store' then cancelled_value end) as store_cancelled_value,

sum(case when channel is null then original_order_count end) as other_order_count,
sum(case when channel is null then original_order_quantity end) as other_order_qty,
sum(case when channel is null then original_order_value end) as other_order_value,
sum(case when channel is null then return_count end) as other_return_count,
sum(case when channel is null then return_quantity end) as other_return_quantity,
sum(case when channel is null then return_value end) as other_return_value,
sum(case when channel is null then cancelled_count end) as other_cancelled_count,
sum(case when channel is null then cancelled_quantity end) as other_cancelled_quantity,
sum(case when channel is null then cancelled_value end) as other_cancelled_value

from customer_360.transaction_view_merch_subdiv_weekly a
join np_marketinganalytics.DACIMDS_c360_DIM_CALENDAR_YEAR_QUARTER_MAP b
on a.fsc_yr_nbr = b.fsc_yr_nbr
and a.fsc_qtr_nbr = b.fsc_qtr_nbr
join np_marketinganalytics.customer_sample_base_for_pipeline c
on concat(coalesce(a.c360_id,'unknown'),'---',coalesce(a.customer_type,'unknown'),'---',coalesce(a.organization_id, 'unknown')) = c.ID

group by
concat(coalesce(a.c360_id,'unknown'),'---',coalesce(a.customer_type,'unknown'),'---',coalesce(a.organization_id, 'unknown')),
a.c360_id,
a.customer_type,
a.organization_id,
a.fsc_yr_nbr,
a.fsc_qtr_nbr,
roll_fsc_qtr_nbr,
merch_sub_category_id;

to_date(a.fsc_wk_end_dt,'yyyy-mm-dd')

--temporal variables:
--includes the creation of features like average inter-transaction time, std dev, min, max:
drop table if exists np_marketinganalytics.MDS_C360_BEHAVIOR_CUSTOMER_ROLLINGQUARTER_LEVEL_TENURE_AVGITT;
create table np_marketinganalytics.MDS_C360_BEHAVIOR_CUSTOMER_ROLLINGQUARTER_LEVEL_TENURE_AVGITT as
with base as
(
select
concat(coalesce(a.c360_id,'unknown'),'---',coalesce(a.customer_type,'unknown'),'---',coalesce(a.organization_id, 'unknown')) as ID,
a.c360_id,
a.customer_type,
a.organization_id,
a.fsc_yr_nbr,
a.fsc_qtr_nbr,
roll_fsc_qtr_nbr,
min(to_date(a.fsc_wk_end_dt)) as FIRST_DATE,
max(to_date(a.fsc_wk_end_dt)) as LAST_DATE,
sum(original_order_count) as trips
from customer_360.transaction_view_full_custrfm a
join np_marketinganalytics.DACIMDS_c360_DIM_CALENDAR_YEAR_QUARTER_MAP b
on a.fsc_yr_nbr = b.fsc_yr_nbr
and a.fsc_qtr_nbr = b.fsc_qtr_nbr
join np_marketinganalytics.customer_sample_base_for_pipeline c
on concat(coalesce(a.c360_id,'unknown'),'---',coalesce(a.customer_type,'unknown'),'---',coalesce(a.organization_id, 'unknown')) = c.ID

group by
concat(coalesce(a.c360_id,'unknown'),'---',coalesce(a.customer_type,'unknown'),'---',coalesce(a.organization_id, 'unknown')),
a.c360_id,
a.customer_type,
a.organization_id,
a.fsc_yr_nbr,
a.fsc_qtr_nbr,
roll_fsc_qtr_nbr
)
select
ID,
c360_id,
customer_type,
organization_id,
fsc_yr_nbr,
fsc_qtr_nbr,
roll_fsc_qtr_nbr,
FIRST_DATE, LAST_DATE, trips,
datediff(LAST_DATE,FIRST_DATE) as Tenure,
case when trips = 1 then 0 else ((datediff(LAST_DATE,FIRST_DATE))*1.000)/ ((Trips*1.000)-1) end as AVG_ITT
from base;
--------------------------------------------------------------------------------------------------------------------------------
--std deviation:
drop table if exists np_marketinganalytics.MDS_C360_BEHAVIOR_CUSTOMER_ROLLINGQUARTER_LEVEL_MIN_MAX_STDDEV_ITT;
create table np_marketinganalytics.MDS_C360_BEHAVIOR_CUSTOMER_ROLLINGQUARTER_LEVEL_MIN_MAX_STDDEV_ITT as
with cust_date as
(
select
concat(coalesce(a.c360_id,'unknown'),'---',coalesce(a.customer_type,'unknown'),'---',coalesce(a.organization_id, 'unknown')) as ID,
a.c360_id,
a.customer_type,
a.organization_id,
a.fsc_yr_nbr,
a.fsc_qtr_nbr,
roll_fsc_qtr_nbr,
a.fsc_wk_end_dt

from customer_360.transaction_view_full_custrfm a
join np_marketinganalytics.DACIMDS_c360_DIM_CALENDAR_YEAR_QUARTER_MAP b
on a.fsc_yr_nbr = b.fsc_yr_nbr
and a.fsc_qtr_nbr = b.fsc_qtr_nbr
join np_marketinganalytics.customer_sample_base_for_pipeline c
on concat(coalesce(a.c360_id,'unknown'),'---',coalesce(a.customer_type,'unknown'),'---',coalesce(a.organization_id, 'unknown')) = c.ID
group by concat(coalesce(a.c360_id,'unknown'),'---',coalesce(a.customer_type,'unknown'),'---',coalesce(a.organization_id, 'unknown')),
a.c360_id,
a.customer_type,
a.organization_id,
a.fsc_yr_nbr,
a.fsc_qtr_nbr,
roll_fsc_qtr_nbr,
a.fsc_wk_end_dt
),
cust_dbt as
(
select
ID,
c360_id,
customer_type,
organization_id,
fsc_yr_nbr,
fsc_qtr_nbr,
roll_fsc_qtr_nbr,
to_date(fsc_wk_end_dt) as curr_date,
lag(to_date(fsc_wk_end_dt)) OVER (PARTITION BY ID, fsc_qtr_nbr ORDER BY fsc_wk_end_dt ASC) AS prev_date
from cust_date
)
select
ID,
c360_id,
customer_type,
organization_id,
fsc_yr_nbr,
fsc_qtr_nbr,
roll_fsc_qtr_nbr,
STDDEV_POP(datediff(curr_date, prev_date)) as STD_DEV_ITT,
min(datediff(curr_date, prev_date)) as min_DAYS_BTWN_TRANS,
max(datediff(curr_date, prev_date)) as max_DAYS_BTWN_TRANS
from cust_dbt
where (curr_date <> prev_date)
and prev_date is not null
group by ID,
c360_id,
customer_type,
organization_id,
fsc_yr_nbr,
fsc_qtr_nbr,
roll_fsc_qtr_nbr
--------------------------------------------------------------------------------------------------------------------------------
--DAYS BEFORE FIRST TRANSACTION, DAYS AFTER LAST TRANSACTION:
drop table if exists np_marketinganalytics.MDS_C360_BEHAVIOR_CUSTOMER_ROLLINGQUARTER_LEVEL_LEADING_TRAILING_DAYS_COUNT;
create table np_marketinganalytics.MDS_C360_BEHAVIOR_CUSTOMER_ROLLINGQUARTER_LEVEL_LEADING_TRAILING_DAYS_COUNT as
with cal_fd_ld as
(
select fsc_yr_nbr, fsc_qtr_nbr, roll_fsc_qtr_nbr, min(to_date(cal_dt)) as fd, max(to_date(cal_dt)) as ld
from np_marketinganalytics.DACIMDS_c360_DIM_CALENDAR
group by fsc_yr_nbr, fsc_qtr_nbr, roll_fsc_qtr_nbr
),
cust_date as
(
select
concat(coalesce(a.c360_id,'unknown'),'---',coalesce(a.customer_type,'unknown'),'---',coalesce(a.organization_id, 'unknown')) as ID,
a.c360_id,
a.customer_type,
a.organization_id,
a.fsc_yr_nbr,
a.fsc_qtr_nbr,
b.roll_fsc_qtr_nbr,
min(to_date(a.fsc_wk_end_dt)) as fpd,
min(to_date(a.fsc_wk_end_dt)) as lpd

from customer_360.transaction_view_full_custrfm a
join np_marketinganalytics.DACIMDS_c360_DIM_CALENDAR_YEAR_QUARTER_MAP b
on a.fsc_yr_nbr = b.fsc_yr_nbr
and a.fsc_qtr_nbr = b.fsc_qtr_nbr
join np_marketinganalytics.customer_sample_base_for_pipeline c
on concat(coalesce(a.c360_id,'unknown'),'---',coalesce(a.customer_type,'unknown'),'---',coalesce(a.organization_id, 'unknown')) = c.ID
join cal_fd_ld d
on a.fsc_yr_nbr = d.fsc_yr_nbr
and a.fsc_qtr_nbr = d.fsc_qtr_nbr

group by concat(coalesce(a.c360_id,'unknown'),'---',coalesce(a.customer_type,'unknown'),'---',coalesce(a.organization_id, 'unknown')),
a.c360_id,
a.customer_type,
a.organization_id,
a.fsc_yr_nbr,
a.fsc_qtr_nbr,
b.roll_fsc_qtr_nbr
)

select
ID,
c360_id,
customer_type,
organization_id,
a.fsc_yr_nbr,
a.fsc_qtr_nbr,
a.roll_fsc_qtr_nbr,
fpd, lpd,
b.fd, b.ld,
datediff(fd, fpd) as delta_fd_fpd,
datediff(lpd, ld) as delta_lpd_ld

from cust_date a
join cal_fd_ld b
on a.fsc_yr_nbr = b.fsc_yr_nbr
and a.fsc_qtr_nbr = b.fsc_qtr_nbr
group by ID,
c360_id,
customer_type,
organization_id,
a.fsc_yr_nbr,
a.fsc_qtr_nbr,
a.roll_fsc_qtr_nbr,
fpd, lpd,
b.fd, b.ld;
--------------------------------------------------------------------------------------------------------------------------------
--final table which has all the temporal vars:
drop table if exists np_marketinganalytics.MDS_C360_BEHAVIOR_CUSTOMER_ROLLINGQUARTER_LEVEL_TEMPORAL_BEHAVIOR;
create table np_marketinganalytics.MDS_C360_BEHAVIOR_CUSTOMER_ROLLINGQUARTER_LEVEL_TEMPORAL_BEHAVIOR as
select
a.id
,a.c360_id
,a.customer_type
,a.organization_id
,a.fsc_yr_nbr
,a.fsc_qtr_nbr
,a.roll_fsc_qtr_nbr
,tenure
,fd
,fpd
,delta_fd_fpd
,ld
,lpd
,delta_lpd_ld
,min_days_btwn_trans
,avg_itt
,std_dev_itt
,max_days_btwn_trans

from np_marketinganalytics.MDS_C360_BEHAVIOR_CUSTOMER_ROLLINGQUARTER_LEVEL_LEADING_TRAILING_DAYS_COUNT A
left join np_marketinganalytics.MDS_C360_BEHAVIOR_CUSTOMER_ROLLINGQUARTER_LEVEL_MIN_MAX_STDDEV_ITT B
on a.ID = b.ID
and a.c360_id = b.c360_id
and a.customer_type = b.customer_type
and a.organization_id = b.organization_id
and a.fsc_yr_nbr = b.fsc_yr_nbr
and a.fsc_qtr_nbr = b.fsc_qtr_nbr
and a.roll_fsc_qtr_nbr = b.roll_fsc_qtr_nbr
left join np_marketinganalytics.MDS_C360_BEHAVIOR_CUSTOMER_ROLLINGQUARTER_LEVEL_TENURE_AVGITT C
on a.ID = c.ID
and a.c360_id = c.c360_id
and a.customer_type = c.customer_type
and a.organization_id = c.organization_id
and a.fsc_yr_nbr = c.fsc_yr_nbr
and a.fsc_qtr_nbr = c.fsc_qtr_nbr
and a.roll_fsc_qtr_nbr = c.roll_fsc_qtr_nbr;
--------------------------------------------------------------------------------------------------------------------------------

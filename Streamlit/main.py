import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import streamlit as st
import os
load_dotenv()


engine = create_engine(
    'snowflake://{user}:{password}@{account_identifier}/'.format(
        user=os.getenv("SNOWFLAKE_USERNAME"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account_identifier=os.getenv("SNOWFLAKE_ACCOUNT"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    )
)

def Q1 (year, state):
    sql_Q1 = f"""with customer_total_return as (select cr_returning_customer_sk as ctr_customer_sk ,ca_state as ctr_state,
sum(cr_return_amt_inc_tax) as ctr_total_return from catalog_returns,date_dim
     ,customer_address
 where cr_returned_date_sk = d_date_sk 
   and d_year = ('{year}')
   and cr_returning_addr_sk = ca_address_sk 
 group by cr_returning_customer_sk
         ,ca_state )
  select  c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name
                   ,ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset
                  ,ca_location_type,ctr_total_return
 from customer_total_return ctr1
     ,customer_address  
     ,customer
 where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
 			  from customer_total_return ctr2 
                  	  where ctr1.ctr_state = ctr2.ctr_state)
       and ca_address_sk = c_current_addr_sk
       and ca_state = ('{state}')
       and ctr1.ctr_customer_sk = c_customer_sk
 order by c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name
                   ,ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset
                  ,ca_location_type,ctr_total_return
 limit 100;"""
    return sql_Q1

def Q2(manufact_1, manufact_2, manufact_3, manufact_4, Price_Lower, Price_Upper, Date):
    sql_q2= f"""select  i_item_id
       ,i_item_desc
       ,i_current_price
 from item, inventory, date_dim, store_sales
 where i_current_price between '{Price_Lower}' and '{Price_Upper}'
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
 and d_date between cast('{Date}' as date) and dateadd(day,60,to_date('{Date}'))
 and i_manufact_id in ('{manufact_1}','{manufact_2}','{manufact_3}','{manufact_4}')
 and inv_quantity_on_hand between 100 and 500
 and ss_item_sk = i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
 limit 100;"""
    return sql_q2
def Q3(date_1, date_2, date_3):
    sql_Q3 = f"""with sr_items as
 (select i_item_id item_id, sum(sr_return_quantity) sr_item_qty
 from store_returns,item, date_dim
 where sr_item_sk = i_item_sk
 and   d_date    in 
	(select d_date
	from date_dim
	where d_week_seq in 
		(select d_week_seq
		from date_dim
	  where d_date in ('{date_1}','{date_2}','{date_3}')))
 and   sr_returned_date_sk   = d_date_sk
 group by i_item_id),
 cr_items as
 (select i_item_id item_id,
        sum(cr_return_quantity) cr_item_qty
 from catalog_returns,
      item,
      date_dim
 where cr_item_sk = i_item_sk
 and   d_date    in 
	(select d_date
	from date_dim
	where d_week_seq in 
		(select d_week_seq
		from date_dim
	  where d_date in ('{date_1}','{date_2}','{date_3}')))
 and   cr_returned_date_sk   = d_date_sk
 group by i_item_id),
 wr_items as
 (select i_item_id item_id,
        sum(wr_return_quantity) wr_item_qty
 from web_returns,
      item,
      date_dim
 where wr_item_sk = i_item_sk
 and   d_date    in 
	(select d_date
	from date_dim
	where d_week_seq in 
		(select d_week_seq
		from date_dim
		where d_date in ('{date_1}','{date_2}','{date_3}')))
 and   wr_returned_date_sk   = d_date_sk
 group by i_item_id)
  select  sr_items.item_id
       ,sr_item_qty
       ,sr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 sr_dev
       ,cr_item_qty
       ,cr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 cr_dev
       ,wr_item_qty
       ,wr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 wr_dev
       ,(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 averagestr
 from sr_items
     ,cr_items
     ,wr_items
 where sr_items.item_id=cr_items.item_id
   and sr_items.item_id=wr_items.item_id 
 order by sr_items.item_id
         ,sr_item_qty
 limit 100;"""
    return sql_Q3

def Q4(Lower_Income, Upper_Income, City):
    sql_Q4 = f""" select  c_customer_id as customer_id
       , coalesce(c_last_name,'') || ', ' || coalesce(c_first_name,'') as customername
 from customer
     ,customer_address
     ,customer_demographics
     ,household_demographics
     ,income_band
     ,store_returns
 where ca_city	        = '{City}'
   and c_current_addr_sk = ca_address_sk
   and ib_lower_bound   >=  '{Lower_Income}'
   and ib_upper_bound   <=  '{Upper_Income}'
   and ib_income_band_sk = hd_income_band_sk
   and cd_demo_sk = c_current_cdemo_sk
   and hd_demo_sk = c_current_hdemo_sk
   and sr_cdemo_sk = cd_demo_sk
 order by c_customer_id
 limit 1; """
    return sql_Q4

def Q5(year):

    sql_Q5 = f""" select  substr(r_reason_desc,1,20)

       ,avg(ws_quantity)

       ,avg(wr_refunded_cash)

       ,avg(wr_fee)

from web_sales, web_returns, web_page, customer_demographics cd1,

      customer_demographics cd2, customer_address, date_dim, reason

where ws_web_page_sk = wp_web_page_sk

   and ws_item_sk = wr_item_sk

   and ws_order_number = wr_order_number

   and ws_sold_date_sk = d_date_sk and d_year ='{year}'

   and cd1.cd_demo_sk = wr_refunded_cdemo_sk

   and cd2.cd_demo_sk = wr_returning_cdemo_sk

   and ca_address_sk = wr_refunded_addr_sk

   and r_reason_sk = wr_reason_sk

   and

   (

    (

     cd1.cd_marital_status = 'D'

     and

     cd1.cd_marital_status = cd2.cd_marital_status

     and

     cd1.cd_education_status = 'Primary'

     and

     cd1.cd_education_status = cd2.cd_education_status

     and

     ws_sales_price between 100.00 and 150.00

    )

   or

    (

     cd1.cd_marital_status = 'U'

     and

     cd1.cd_marital_status = cd2.cd_marital_status

     and

     cd1.cd_education_status = '4 yr Degree'

     and

     cd1.cd_education_status = cd2.cd_education_status

     and

     ws_sales_price between 50.00 and 100.00

    )

   or

    (

     cd1.cd_marital_status = 'W'

     and

     cd1.cd_marital_status = cd2.cd_marital_status

     and

     cd1.cd_education_status = 'Advanced Degree'

     and

     cd1.cd_education_status = cd2.cd_education_status

     and

     ws_sales_price between 150.00 and 200.00

    )

   )

   and

   (

    (

     ca_country = 'United States'

     and

     ca_state in ('LA', 'CO', 'TX')

     and ws_net_profit between 100 and 200  

    )

    or

    (

     ca_country = 'United States'

     and

     ca_state in ('OH', 'VA', 'MO')

     and ws_net_profit between 150 and 300  

    )

    or

    (

     ca_country = 'United States'

     and

     ca_state in ('FL', 'OK', 'MS')

     and ws_net_profit between 50 and 250  

    )

   )

group by r_reason_desc

order by substr(r_reason_desc,1,20)

        ,avg(ws_quantity)

        ,avg(wr_refunded_cash)

        ,avg(wr_fee)

limit 5; """

    return sql_Q5

 

def Q6(DMS):

    sql_Q6 = f"""

    select   

    sum(ws_net_paid) as total_sum

   ,i_category

   ,i_class

   ,grouping(i_category)+grouping(i_class) as lochierarchy

   ,rank() over (

    partition by grouping(i_category)+grouping(i_class),

    case when grouping(i_class) = 0 then i_category end

    order by sum(ws_net_paid) desc) as rank_within_parent

from

    web_sales

   ,date_dim       d1

   ,item

where

    d1.d_month_seq between '{DMS}' and '{DMS}'+11

and d1.d_date_sk = ws_sold_date_sk

and i_item_sk  = ws_item_sk

group by rollup(i_category,i_class)

order by

   lochierarchy desc,

   case when lochierarchy = 0 then i_category end,

   rank_within_parent

limit 10;

"""

    return sql_Q6

 

 

def Q7(DMS):

    sql_Q7 = f""" select count(*)

        from ((select distinct c_last_name, c_first_name, d_date

       from store_sales, date_dim, customer

       where store_sales.ss_sold_date_sk = date_dim.d_date_sk

         and store_sales.ss_customer_sk = customer.c_customer_sk

         and d_month_seq between '{DMS}' and '{DMS}'+11)

       except

      (select distinct c_last_name, c_first_name, d_date

       from catalog_sales, date_dim, customer

       where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk

         and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk

         and d_month_seq between '{DMS}' and '{DMS}'+11)

       except

      (select distinct c_last_name, c_first_name, d_date

       from web_sales, date_dim, customer

       where web_sales.ws_sold_date_sk = date_dim.d_date_sk

         and web_sales.ws_bill_customer_sk = customer.c_customer_sk

         and d_month_seq between '{DMS}' and '{DMS}'+11)

        ) cool_cust

; """

    return sql_Q7

def Q8():

    sql_Q8 = f""" select  *

from

(select count(*) h8_30_to_9

from store_sales, household_demographics , time_dim, store

where ss_sold_time_sk = time_dim.t_time_sk   

     and ss_hdemo_sk = household_demographics.hd_demo_sk

     and ss_store_sk = s_store_sk

     and time_dim.t_hour = 8

     and time_dim.t_minute >= 30

     and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or

          (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2) or

          (household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2))

     and store.s_store_name = 'ese') s1,

(select count(*) h9_to_9_30

from store_sales, household_demographics , time_dim, store

where ss_sold_time_sk = time_dim.t_time_sk

     and ss_hdemo_sk = household_demographics.hd_demo_sk

     and ss_store_sk = s_store_sk

     and time_dim.t_hour = 9

     and time_dim.t_minute < 30

     and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or

          (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2) or

          (household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2))

     and store.s_store_name = 'ese') s2,

(select count(*) h9_30_to_10

from store_sales, household_demographics , time_dim, store

where ss_sold_time_sk = time_dim.t_time_sk

     and ss_hdemo_sk = household_demographics.hd_demo_sk

     and ss_store_sk = s_store_sk

     and time_dim.t_hour = 9

     and time_dim.t_minute >= 30

     and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or

          (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2) or

          (household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2))

     and store.s_store_name = 'ese') s3,

(select count(*) h10_to_10_30

from store_sales, household_demographics , time_dim, store

where ss_sold_time_sk = time_dim.t_time_sk

     and ss_hdemo_sk = household_demographics.hd_demo_sk

     and ss_store_sk = s_store_sk

     and time_dim.t_hour = 10

     and time_dim.t_minute < 30

     and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or

          (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2) or

          (household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2))

     and store.s_store_name = 'ese') s4,

(select count(*) h10_30_to_11

from store_sales, household_demographics , time_dim, store

where ss_sold_time_sk = time_dim.t_time_sk

     and ss_hdemo_sk = household_demographics.hd_demo_sk

     and ss_store_sk = s_store_sk

     and time_dim.t_hour = 10

     and time_dim.t_minute >= 30

     and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or

          (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2) or

          (household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2))

     and store.s_store_name = 'ese') s5,

(select count(*) h11_to_11_30

from store_sales, household_demographics , time_dim, store

where ss_sold_time_sk = time_dim.t_time_sk

     and ss_hdemo_sk = household_demographics.hd_demo_sk

     and ss_store_sk = s_store_sk

     and time_dim.t_hour = 11

     and time_dim.t_minute < 30

     and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or

          (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2) or

          (household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2))

     and store.s_store_name = 'ese') s6,

(select count(*) h11_30_to_12

from store_sales, household_demographics , time_dim, store

where ss_sold_time_sk = time_dim.t_time_sk

     and ss_hdemo_sk = household_demographics.hd_demo_sk

     and ss_store_sk = s_store_sk

     and time_dim.t_hour = 11

     and time_dim.t_minute >= 30

     and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or

          (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2) or

          (household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2))

     and store.s_store_name = 'ese') s7,

(select count(*) h12_to_12_30

from store_sales, household_demographics , time_dim, store

where ss_sold_time_sk = time_dim.t_time_sk

     and ss_hdemo_sk = household_demographics.hd_demo_sk

     and ss_store_sk = s_store_sk

     and time_dim.t_hour = 12

     and time_dim.t_minute < 30

     and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or

          (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2) or

          (household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2))

     and store.s_store_name = 'ese') s8

;"""

    return sql_Q8

 

def Q9(year, class_category_mappings, cat_category_mappings):

    sql_Q9 = f""" select  *

from(

select i_category, i_class, i_brand,

       s_store_name, s_company_name,

       d_moy,

       sum(ss_sales_price) sum_sales,

       avg(sum(ss_sales_price)) over

         (partition by i_category, i_brand, s_store_name, s_company_name)

         avg_monthly_sales

from item, store_sales, date_dim, store

where ss_item_sk = i_item_sk and

      ss_sold_date_sk = d_date_sk and

      ss_store_sk = s_store_sk and

       d_year in ({year}) and

                        ((i_category in ({','.join(["'" + category + "'" for category in cat_category_mappings])}) and

                          i_class in ({','.join(["'" + clazz + "'" for clazz in class_category_mappings])})

                         )

                      )

group by i_category, i_class, i_brand,

         s_store_name, s_company_name, d_moy) tmp1

where case when (avg_monthly_sales <> 0) then (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales) else null end > 0.1

order by sum_sales - avg_monthly_sales, s_store_name

limit 5; """

    return sql_Q9

 

 

def Q10(Hour_AM, Hour_PM, Percent):

    sql_Q10 = f""" select  cast(amc as decimal(15,4))/cast(pmc as decimal(15,4)) am_pm_ratio

from ( select count(*) amc

       from web_sales, household_demographics , time_dim, web_page

       where ws_sold_time_sk = time_dim.t_time_sk

         and ws_ship_hdemo_sk = household_demographics.hd_demo_sk

         and ws_web_page_sk = web_page.wp_web_page_sk

         and time_dim.t_hour between '{Hour_AM}' and '{Hour_AM}'+1

         and household_demographics.hd_dep_count = '{Percent}'

         and web_page.wp_char_count between 5000 and 5200) at,

      ( select count(*) pmc

       from web_sales, household_demographics , time_dim, web_page

       where ws_sold_time_sk = time_dim.t_time_sk

         and ws_ship_hdemo_sk = household_demographics.hd_demo_sk

         and ws_web_page_sk = web_page.wp_web_page_sk

         and time_dim.t_hour between '{Hour_PM}' and '{Hour_PM}'+1

         and household_demographics.hd_dep_count = '{Percent}'

         and web_page.wp_char_count between 5000 and 5200) pt

order by am_pm_ratio

limit 10;"""

    return sql_Q10

# Streamlit UI
st.title("Snowflake Query Runner")

# Dropdown to select a query
selected_query = st.selectbox("Select a Query", ['Query 1', 'Query 2', 'Query 3', 'Query 4', 'Query 5', 'Query 6', 'Query 7', 'Query 8', 'Query 9', 'Query 10'])

# Input parameters based on the selected query
if selected_query == "Query 1":
    st.write(""" Find customers and their detailed customer data who have returned items bought from the catalog more than 20 percent the average customer returns for customers in a given state in a given time period. Order output by customer data.""")
    year = st.text_input("Enter the year:", "")
    state = st.text_input("Enter the state:", "")

elif selected_query == "Query 2":
    st.write(""" Find customers who tend to spend more money (net-paid) on-line than in stores""")
    manufact_1 = st.text_input("Enter the Manufacturing ID - 1:", "")
    manufact_2 = st.text_input("Enter the Manufacturing ID - 2:", "")
    manufact_3 = st.text_input("Enter the Manufacturing ID - 3:", "")
    manufact_4 = st.text_input("Enter the Manufacturing ID - 4:", "")
    Price_Lower = st.text_input("Enter Price_Lower:", "")
    Price_Upper = st.text_input("Enter Price_Upper,:", "")
    Date = st.text_input("Enter Date:", "")

elif selected_query == "Query 3":
    st.write("""Retrieve the items with the highest number of returns where the number of returns was approximately
                equivalent across all store, catalog and web channels (within a tolerance of +/- 10%), within the week ending a
                given date.""")
    date_1 = st.text_input("Enter date_1:", "")
    date_2 = st.text_input("Enter date_2:", "")
    date_3 = st.text_input("Enter date_3:", "")

elif selected_query == "Query 4":
    st.write("""List all customers living in a specified city, with an income between 2 values""")
    Lower_Income = st.text_input("Enter Lower_Income:", "")
    Upper_Income = st.text_input("Enter Upper_Income:", "")
    City = st.text_input("Enter City:", "")

elif selected_query == "Query 5":
    st.write("""For all web return reason calculate the average sales, average refunded cash and average return fee by different
                combinations of customer and sales types """)
    year = st.text_input("Enter YEAR:", "")
elif selected_query == "Query 6":
    st.write("""Rollup the web sales for a given year by category and class, and rank the sales among peers within the parent,
                for each group compute sum of sales, location with the hierarchy and rank within the group.""")
    DMS = st.text_input("Enter DMS:", "")

elif selected_query == "Query 7":
    st.write("""Count how many customers have ordered on the same day items on the web and the catalog and on the same
                day have bought items in a store.""")
    DMS = st.text_input("Enter DMS:", "")

elif selected_query == "Query 8":
    st.write("""How many items do we sell between pacific times of a day in certain stores to customers with one dependent
                count and 2 or less vehicles registered or 2 dependents with 4 or fewer vehicles registered or 3 dependents and
                five or less vehicles registered. In one row break the counts into sells from 8:30 to 9, 9 to 9:30, 9:30 to 10 ... 12
                to 12:30""")

elif selected_query == "Query 9":
    st.write("""Within a year list all month and combination of item categories, classes and brands that have had monthly sales
                larger than 0.1 percent of the total yearly sales""")
    year = st.text_input("Enter YEAR.01:", "")
    class_category_mappings = st.multiselect("Select CLASS codes:", ["dresses", "birdal", "shirts", "football", "stereo", "computers"])
    cat_category_mappings = st.multiselect("Select CAT codes:", ["Women", "Jewelry", "Men", "Sports", "Electronics", "Books"])

else:
    st.write("""What is the ratio between the number of items sold over the internet in the morning (8 to 9am) to the number of
                items sold in the evening (7 to 8pm) of customers with a specified number of dependents. Consider only
                websites with a high amount of content.""")
    Hour_PM = st.text_input("Enter Hour_PM:", "")
    Hour_AM = st.text_input("Enter Hour_AM:", "")
    Percent = st.text_input("Enter Percent:", "")

#Execute queries
if st.button("Run Query"):
    if selected_query == "Query 1":
        query = Q1(year, state)
    elif selected_query == "Query 2":
        query = Q2(manufact_1, manufact_2, manufact_3, manufact_4, Price_Lower, Price_Upper, Date)
    elif selected_query == "Query 3":
        query = Q3(date_1, date_2, date_3)
    elif selected_query == "Query 4":
        query = Q4(Lower_Income, Upper_Income, City)
    elif selected_query == "Query 5":
        query = Q5(year)
    elif selected_query == "Query 6":
        query = Q6(DMS)
    elif selected_query == "Query 7":
        query = Q7(DMS)
    elif selected_query == "Query 8":
        query = Q8()
    elif selected_query == "Query 9":
        query = Q9(year, class_category_mappings, cat_category_mappings)
    else:
        query = Q10(Hour_AM, Hour_PM, Percent)


try:

    with engine.connect() as conn:
    # Execute the USE SCHEMA statement first
        conn.execute("USE SCHEMA snowflake_sample_data.tpcds_sf10tcl")
        result = conn.execute(query)
        data = pd.DataFrame(result.fetchall())      


        if not data.empty:
          st.write(data)
        else:
         st.write("No results found.")

except:
    st.write('Loading...')


    
import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import streamlit as st

##Define the engine for running  sql queries 
engine = create_engine(URL(
    account = 'qjb57177.us-east-1',
    user = 'PyAshishMhatre',
    password = '!Ashish123',
    database = 'SNOWFLAKE_SAMPLE_DATA',
    schema = 'TPCDS_SF10TCL',
    warehouse = 'COMPUTE_WH',
    role='ACCOUNTADMIN',
))

st.title('Running custom TPCDS SQL Queries')

st.subheader('Query 1 - Compute the total revenue and the ratio of total revenue to revenue by item class for specified item categories and time periods')

with st.expander("Edit Substitution Parameters"):
    category = st.selectbox('Select Category',('Sports', 'Books', 'Home'))
    date = st.text_input('Entire sales date', 'YYYY-MM-DD')
    
    

def query1(option,date):
    option = str(option)
    date = str(date)
    try:
        connection = engine.connect()
        df = pd.read_sql_query(f"""  select i_category 
       ,i_class 
       ,i_current_price
       ,sum(cs_ext_sales_price) as itemrevenue 
       ,sum(cs_ext_sales_price)*100/sum(sum(cs_ext_sales_price)) over
           (partition by i_class) as revenueratio
 from	catalog_sales
     ,item 
     ,date_dim
 where cs_item_sk = i_item_sk 
   and i_category in (\'{option}\')
   and cs_sold_date_sk = d_date_sk
 and d_date between cast(\'{date}\' as date) 
                                and dateadd(day, 30, cast(\'{date}\' as date))
 group by i_category
         ,i_class
         ,i_current_price
 order by i_category
         ,i_class
         ,revenueratio
 limit 100;""", engine)
        
        return df

    finally:
        connection.close()
        engine.dispose()

button1 = st.button('Run')
if button1:
    df = query1(category,date)
    try:
        st.write(df)
    except:
        st.write('Error in query execution')

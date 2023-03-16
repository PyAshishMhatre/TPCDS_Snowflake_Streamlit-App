import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


# Set the Snowflake connection parameters
conn = snowflake.connector.connect(
        user='ASHWIN',
        password='Ashwin@8767',
        account='dx27098.us-east4.gcp',
        warehouse='HP',
        database='SNOWFLAKE_SAMPLE_DATA',
        schema='TPCDS_SF10TCL'
        
    )


engine = create_engine(URL(
    account = 'qjb57177.us-east-1',
    user = 'PyAshishMhatre',
    password = '!Ashish123',
    database = 'SNOWFLAKE_SAMPLE_DATA',
    schema = 'TPCDS_SF10TCL',
    warehouse = 'COMPUTE_WH',
    role='ACCOUNTADMIN',
))
################Question 21######################################

st.title('Running custom TPCDS SQL Queries')

st.text('')
st.text('')

st.markdown('***:red[Query 1]*** - Compute the total revenue and the ratio of total revenue to revenue by item class for specified item categories and time periods')

with st.expander("Edit Substitution Parameters"):
    category = st.selectbox('Select Category',('Sports', 'Books', 'Home'), key='q1select')
    date = st.text_input('Enter sales date', 'YYYY-MM-DD', key='q1text')
    
    

def query1(option,date):
    with st.spinner('Querying Snowflake database...'):
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

button1 = st.button('Run', key='q1button')
if button1:
    df = query1(category,date)
    try:
        st.success('Query Success', icon="✅")
        st.write(df)
    except:
        st.write('Error in query execution')




################Question 22######################################
def plot_inventory_data(d_month_seq, x_column):
    # Connect to the database

    # Get the list of valid d_month_seq values from the database
    query = "SELECT DISTINCT d_month_seq FROM date_dim"
    valid_d_month_seq = [row[0] for row in conn.cursor().execute(query).fetchall()]

    # Check that the user-provided value is valid
    if d_month_seq not in valid_d_month_seq:
        st.error(f"Invalid d_month_seq value: {d_month_seq}")
    else:
        # Execute the query
        query = f"""
        SELECT i_product_name, i_brand, i_class, i_category, AVG(inv_quantity_on_hand) AS qoh
        FROM inventory, date_dim, item, warehouse
        WHERE inv_date_sk = d_date_sk AND inv_item_sk = i_item_sk AND inv_warehouse_sk = w_warehouse_sk
        AND d_month_seq = {d_month_seq}
        GROUP BY ROLLUP(i_product_name, i_brand, i_class, i_category)
        ORDER BY qoh, i_product_name, i_brand, i_class, i_category  limit 100
        """

        # Read the query results into a Pandas DataFrame
        df = pd.read_sql_query(query, conn)

        # Specify the column to use for x-axis
        x_column = x_column

        # Create the bar chart
        fig = px.bar(df, x=x_column, y='QOH', barmode='group')

        # Set the chart title
        fig.update_layout(title='Quantity on Hand by ' + x_column)

        # Show the chart
        st.plotly_chart(fig)


###############Question 23#########################################
def Q23_1(d_year,d_moy):

    # Get the valid years from the date_dim table
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT d_year FROM date_dim")
    valid_years = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT DISTINCT d_moy FROM date_dim")
    valid_mon = [row[0] for row in cursor.fetchall()]

    cursor.close()

    # Check if the input year is valid
    if int(d_year) not in valid_years:
        st.write(f"Invalid year. Please enter a year in {valid_years}.")
        conn.close()
        return None
    elif int(d_moy) not in valid_mon:
        st.write(f"Invalid year. Please enter a month in {valid_mon}.")
        conn.close()
    else:
        # Execute the query with the parameterized d_year
        query = """
        WITH frequent_ss_items 
        AS (SELECT Substr(i_item_desc, 1, 30) itemdesc, 
                i_item_sk                  item_sk, 
                d_date                     solddate, 
                Count(*)                   cnt 
         FROM   store_sales, 
                date_dim, 
                item 
         WHERE  ss_sold_date_sk = d_date_sk 
                AND ss_item_sk = i_item_sk 
                AND d_year IN (%s, %s, %s, %s) 
         GROUP  BY 1, 
                   i_item_sk, 
                   d_date 
         HAVING Count(*) > 4), 
        max_store_sales 
        AS (SELECT Max(csales) tpcds_cmax 
        FROM   (SELECT c_customer_sk, 
                        Sum(ss_quantity * ss_sales_price) csales 
                 FROM   store_sales, 
                        customer, 
                        date_dim 
                 WHERE  ss_customer_sk = c_customer_sk 
                        AND ss_sold_date_sk = d_date_sk 
                        AND d_year IN (%s, %s, %s, %s) 
                 GROUP  BY c_customer_sk)), 
        best_ss_customer 
        AS (SELECT c_customer_sk, 
                Sum(ss_quantity * ss_sales_price) ssales 
         FROM   store_sales, 
                customer 
         WHERE  ss_customer_sk = c_customer_sk 
         GROUP  BY c_customer_sk 
         HAVING Sum(ss_quantity * ss_sales_price) > 
                ( 95 / 100.0 ) * (SELECT * 
                                  FROM   max_store_sales)) 
        SELECT Sum(sales) 
        FROM   (SELECT cs_quantity * cs_list_price sales 
        FROM   catalog_sales, 
               date_dim 
        WHERE  d_year = %s 
               AND d_moy = %s 
               AND cs_sold_date_sk = d_date_sk 
               AND cs_item_sk IN (SELECT item_sk 
                                  FROM   frequent_ss_items) 
               AND cs_bill_customer_sk IN (SELECT c_customer_sk 
                                           FROM   best_ss_customer) 
        UNION ALL 
        SELECT ws_quantity * ws_list_price sales 
        FROM   web_sales, 
               date_dim 
        WHERE  d_year = %s 
               AND d_moy = %s 
               AND ws_sold_date_sk = d_date_sk 
               AND ws_item_sk IN (SELECT item_sk 
                                  FROM   frequent_ss_items) 
               AND ws_bill_customer_sk IN (SELECT c_customer_sk 
                                           FROM   best_ss_customer)) LIMIT 100;
        """

        # Fetch the results into a Pandas dataframe
        cursor = conn.cursor()
        cursor.execute(query, (d_year, d_year + 1, d_year + 2, d_year + 3, d_year, d_year + 1, d_year + 2, d_year + 3, d_year, d_moy, d_year, d_moy))
        # cursor.execute(query, (str(d_year), str(d_year + 1), str(d_year + 2), str(d_year + 3), str(d_year), str(d_year + 1), str(d_year + 2), str(d_year + 3), str(d_year), str(d_mon), str(d_year), str(d_mon)))
        result = cursor.fetchone()[0]
        df = pd.DataFrame({"Total Sales": [result]})
        cursor.close()

        # Show the dataframe in the app
        st.write(df)
        conn.close()
        return df
    

###############################################################################################################
st.text('')
st.text('')

st.markdown(""" ***:red[Query 2-3]*** - : Select dropdown to run query 2 or 3 """)


# Create the sidebar with the app options
app_selection = st.selectbox(
    "Select Query",
    ("Average Quantity on Hand", "Total web and catalog sales")
)

# Call the appropriate app based on the user's selection
if app_selection == "Average Quantity on Hand":

    st.write("<h3 style='font-size:16px'>Average quantity on hand by class and category</h3>", unsafe_allow_html=True)
    # Create an input field for the d_month_seq value
    d_month_seq = st.text_input("Enter a Month Seq value", value='1200')
    x_column = st.selectbox("Choose an Rollup", options=['I_CATEGORY', 'I_CLASS'])

    # Convert the input value to an integer
    d_month_seq = int(d_month_seq)

    # Call the plot_inventory_data function with the input value  
    plot_inventory_data(d_month_seq, x_column)
else:
    # st.header("Find frequently sold items that were sold more than 4 times on any day during four consecutive years through the store sales channel",3)
    st.write("<h3 style='font-size:16px'>Total web and catalog sales in a particular month made by our best store customers buying our most frequent store items.</h3>", unsafe_allow_html=True)

    # Add an input box for the valid year
    # d_year = st.text_input("Enter a valid year",key="T1")
    # d_year = int(d_year)

    d_year = st.text_input("Enter a valid year", key="T1")
    if d_year != '':
        d_year = int(d_year)


    d_moy = st.text_input("Enter a valid year", key="T2")
    if d_moy != '':
        d_moy = int(d_moy)

    if st.button("Run Query",key="B1"):
        Q23_1(d_year,d_moy)


    
 ################Question 24######################################   
st.text('')
st.text('')
st.markdown(""" ***:red[Query 4]*** - : Calculate the total specified monetary value of items in a specific color for store sales transactions
by customer name and store, in a specific market, from customers who currently dont live in their birth
countries and in the neighborhood of the store, and list only those customers for whom the total specified
monetary value is greater than average of the average value """)

with st.expander("Edit Substitution Parameters"):
    market = st.selectbox('Select Market',(1,2,3,4,5,6,7,8,9,10), key='q4select')
    color = st.text_input('Enter color', 'black', key='q4text')

def query4(market,color):
    market = int(market)
    color = str(color)
    try:
        with st.spinner('Querying Snowflake database...'):
            connection = engine.connect()
            print('Query')
            df_color = pd.read_sql_query('select distinct i_color from item;', engine)
            print('Exit')
            print(df_color['i_color'])
            if color in df_color['i_color'].values:
                try:
                    connection = engine.connect()
                    df = pd.read_sql_query(f"""  with ssales as
            (select c_last_name
                ,c_first_name
                ,s_store_name
                ,ca_state
                ,s_state
                ,i_color
                ,i_current_price
                ,i_manager_id
                ,i_units
                ,i_size
                ,sum(ss_net_paid) netpaid
            from store_sales
                ,store_returns
                ,store
                ,item
                ,customer
                ,customer_address
            where ss_ticket_number = sr_ticket_number
            and ss_item_sk = sr_item_sk
            and ss_customer_sk = c_customer_sk
            and ss_item_sk = i_item_sk
            and ss_store_sk = s_store_sk
            and c_current_addr_sk = ca_address_sk
            and c_birth_country <> upper(ca_country)
            and s_zip = ca_zip
            and s_market_id= {market}
            group by c_last_name
                    ,c_first_name
                    ,s_store_name
                    ,ca_state
                    ,s_state
                    ,i_color
                    ,i_current_price
                    ,i_manager_id
                    ,i_units
                    ,i_size
                    limit 50)
            select c_last_name
                ,c_first_name
                ,s_store_name
                ,sum(netpaid) paid
            from ssales
            where i_color = \'{color}\'
            group by c_last_name
                    ,c_first_name
                    ,s_store_name
            having sum(netpaid) > (select avg(netpaid)
                                            from ssales)
            order by c_last_name
                    ,c_first_name
                    ,s_store_name
            limit 50; """, engine)
                    
                    return df

                finally:
                    connection.close()
                    engine.dispose()
            else:
                st.write('Enter correct color as per data')
    finally:
        connection.close()
        engine.dispose()
    
button4 = st.button('Run', key='q4button')
if button4:
    df = query4(market,color)
    try:
        st.success('Query Success', icon="✅")
        st.write(df)
    except:
        st.write('Error in query execution')
        
        
################Question 25######################################   
st.text('')
st.text('')

st.markdown('''***:red[Query 5]*** - : Get all items that were sold in stores in a particular month and year and returned and re-purchased by 
            the customer through the catalog channel in the same month and in the six following months. For these items, compute the sum of net 
            profit of store sales, net loss of store loss and net profit of catalog . Group this information by item and store. ''')

with st.expander("Edit Substitution Parameters"):
    agg_ = st.selectbox('Select Aggregation',('sum','avg'), key='q5select')
    year_ = st.text_input('Enter year', '2001', key='q5text')
    month_ = st.selectbox('Select Month',(1,2,3,4,5,6,7,8,9,10,11,12), key='q5selectmonth')
    
def query5(agg_,year_,month_):
    year = int(year_)
    month = int(month_)
    agg = str(agg_)
    
    if month > 6:
        endmonth = 12
    else:
        endmonth = month + 6
    
    try:
        with st.spinner('Querying Snowflake database...'):
                connection = engine.connect()
                df = pd.read_sql_query(f"""  select  
                                                i_item_id
                                                ,i_item_desc
                                                ,s_store_id
                                                ,s_store_name
                                                ,{agg}(ss_net_profit) as store_sales_profit
                                                ,{agg}(sr_net_loss) as store_returns_loss
                                                ,{agg}(cs_net_profit) as catalog_sales_profit
                                                from
                                                store_sales
                                                ,store_returns
                                                ,catalog_sales
                                                ,date_dim d1
                                                ,date_dim d2
                                                ,date_dim d3
                                                ,store
                                                ,item
                                                where
                                                d1.d_moy = {month}
                                                and d1.d_year = {year}
                                                and d1.d_date_sk = ss_sold_date_sk
                                                and i_item_sk = ss_item_sk
                                                and s_store_sk = ss_store_sk
                                                and ss_customer_sk = sr_customer_sk
                                                and ss_item_sk = sr_item_sk
                                                and ss_ticket_number = sr_ticket_number
                                                and sr_returned_date_sk = d2.d_date_sk
                                                and d2.d_moy               between {month} and  {endmonth}
                                                and d2.d_year              = {year}
                                                and sr_customer_sk = cs_bill_customer_sk
                                                and sr_item_sk = cs_item_sk
                                                and cs_sold_date_sk = d3.d_date_sk
                                                and d3.d_moy               between {month} and  {endmonth} 
                                                and d3.d_year              = {year}
                                                group by
                                                i_item_id
                                                ,i_item_desc
                                                ,s_store_id
                                                ,s_store_name
                                                order by
                                                i_item_id
                                                ,i_item_desc
                                                ,s_store_id
                                                ,s_store_name
                                                limit 100; """, engine)
                
                return df
    finally:
        connection.close()
        engine.dispose()
    
button4 = st.button('Run', key='q5button')
if button4:
    df = query5(agg_,year_,month_)
    try:
        st.success('Query Success', icon="✅")
        st.write(df)
        
    except:
        st.write('Error in query execution')
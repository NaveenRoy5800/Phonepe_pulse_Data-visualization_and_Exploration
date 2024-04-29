import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector as db
import pandas as pd
import plotly.express as px
from PIL import Image
import requests
import json
import matplotlib.pyplot as plt


#DataFrames creation

#Aggregated Transactions Dataframe

mydb = db.connect(host="localhost",
                        user="root",
                        password="Spidey@123$",
                        database="phonepe",
                        port = 3306)

mycursor = mydb.cursor()

mycursor.execute("SELECT * from aggregated_transactions")
table1 = mycursor.fetchall()
mydb.commit()

Agg_Trans = pd.DataFrame(table1, columns = ("State", "Year", "Quarter", "Transaction_Type",
                                             "Transaction_Count", "Transaction_Amount"))

#Aggregated Users Dataframe

mycursor.execute("SELECT * from aggregated_users")
table2 = mycursor.fetchall()
mydb.commit()

Agg_Users = pd.DataFrame(table2, columns = ("State", "Year", "Quarter", "Brand",
                                             "Registered_Users_Brand", "Percentage"))

#Map Transactions Dataframe

mycursor.execute("SELECT * from map_transactions")
table3 = mycursor.fetchall()
mydb.commit()

Map_Transactions = pd.DataFrame(table3, columns = ("State", "Year", "Quarter", "District",
                                                    "Transaction_Count", "Transaction_Amount"))

#Map Users Dataframe

mycursor.execute("SELECT * from map_users")
table4 = mycursor.fetchall()
mydb.commit()

Map_Users = pd.DataFrame(table4, columns = ("State", "Year", "Quarter", "District",
                                             "Registered_Users", "App_Open"))

#Top Transactions Dataframe

mycursor.execute("SELECT * from top_transactions")
table5 = mycursor.fetchall()
mydb.commit()

Top_Transactions = pd.DataFrame(table5, columns = ("State", "Year", "Quarter", "Pincode",
                                                    "Transaction_Count", "Transaction_Amount"))

#Top Users Dataframe

mycursor.execute("SELECT * from top_users")
table6 = mycursor.fetchall()
mydb.commit()

Top_Users = pd.DataFrame(table6, columns = ("State", "Year", "Quarter", "Pincode", "Registered_Users"))

#Aggregated Users Quarter wise registered users Dataframe

mycursor.execute("SELECT * from aggregated_users_quarter")
table7 = mycursor.fetchall()
mydb.commit()

Agg_Users_Quarter = pd.DataFrame(table7, columns = ("State", "Year", "Quarter","Registered_Users",
                                                     "App_Opens"))

#Choropleth Map to display transaction details for states based on selected Year and the Quarter

def make_choropleth(df, year, quarter):
    
    Trans_amt_year = df[df['Year'] == year]
    Trans_amt_year.reset_index(drop=True, inplace=True)

    Trans_amt_year_Quart = Trans_amt_year[Trans_amt_year['Quarter'] == quarter]
    Trans_amt_year_Quart.reset_index(drop=True, inplace=True)
    Trans_amt_year_Quart_g = Trans_amt_year_Quart.groupby('State')[["Transaction_Amount", "Transaction_Count"]].sum()
    df1 = pd.DataFrame(Trans_amt_year_Quart_g)
    df1.reset_index(inplace=True)

    url= "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response= requests.get(url)
    data1= json.loads(response.content)
    states_name= []
    for feature in data1["features"]:
        states_name.append(feature["properties"]["ST_NM"])

    states_name.sort()

    fig_india_1= px.choropleth(df1, geojson= data1, locations= "State", featureidkey= "properties.ST_NM",
                                    color= "Transaction_Amount", color_continuous_scale= "haline_r",
                                    range_color= (df1["Transaction_Amount"].min(), df1["Transaction_Amount"].max()),
                                    hover_name= "State",hover_data=('Transaction_Count'),
                                    title= f"Transactions for {year} Q{quarter}", fitbounds= "locations",
                                    height= 600,width= 600)
    fig_india_1.update_geos(projection_type="mercator")
    fig_india_1.update_layout(geo=dict(bgcolor='#1e0d39'))
    st.plotly_chart(fig_india_1)

# Choropleth map to display the count of Registered users based on the year and quarter selected

def registered_users_map(df, year, quarter):
    
    rumy = df[df['Year'] == year]
    rumy.reset_index(drop=True, inplace=True)

    rumyq = rumy[rumy['Quarter'] == quarter]
    rumyq.reset_index(drop=True, inplace=True)
    rumyq_1 = rumyq.groupby('State')["Registered_Users"].sum()
    df1 = pd.DataFrame(rumyq_1)
    df1.reset_index(inplace=True)

    url= "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response= requests.get(url)
    data1= json.loads(response.content)
    states_name= []
    for feature in data1["features"]:
        states_name.append(feature["properties"]["ST_NM"])

    states_name.sort()

    fig_india_2= px.choropleth(df1, geojson= data1, locations= "State", featureidkey= "properties.ST_NM",
                                    color= "Registered_Users", color_continuous_scale= "YlGnBu",
                                    range_color= (df1["Registered_Users"].min(), df1["Registered_Users"].max()),
                                    hover_name= "State", title= f"Registered Users for {year} Q{quarter}",
                                    fitbounds= "locations", height= 600,width= 600)
    fig_india_2.update_geos(visible= False)
    fig_india_2.update_layout(height=600, width=600)
    fig_india_2.update_geos(projection_type="mercator")
    fig_india_2.update_layout(geo=dict(bgcolor='#1e0d39'))
    st.plotly_chart(fig_india_2)

# DataFrame to display the top 10 states for Aggregated Transactions

def agg_trans_cnt(df, year, quarter):

    atcy = df[df['Year'] == year]
    atcy.reset_index(drop=True, inplace=True)

    atcyq = atcy[atcy['Quarter'] == quarter]
    atcyq.reset_index(drop=True, inplace=True)
    atcyq_1 = atcyq.groupby('State')["Transaction_Count"].sum()
    df1 = pd.DataFrame(atcyq_1)
    df1.reset_index(inplace=True)
    sorted_trans = df1.sort_values(by='Transaction_Count', ascending=False).head(10)
    sorted_trans.reset_index(drop=True, inplace=True)
    st.dataframe(sorted_trans.set_index(sorted_trans.columns[0]))

#plot to show the transactions for all states

def agg_trans_cnt_all(df, year, quarter):

    atcy = df[df['Year'] == year]
    atcy.reset_index(drop=True, inplace=True)

    atcyq = atcy[atcy['Quarter'] == quarter]
    atcyq.reset_index(drop=True, inplace=True)
    atcyq_1 = atcyq.groupby('State')[["Transaction_Count", "Transaction_Amount"]].sum()
    df1 = pd.DataFrame(atcyq_1)
    df1.reset_index(inplace=True)

    fig = px.bar(df1, x='State', y='Transaction_Count',
                hover_data=['State', 'Transaction_Count', 'Transaction_Amount'], 
                height=600, color_discrete_sequence=px.colors.sequential.Rainbow_r,
                title=f"Transactions of all the States for {year}  Q{quarter}: ")
    st.plotly_chart(fig)

#Plot1
def agg_trans_cnt_plot(df, year, quarter):

    atcy = df[df['Year'] == year]
    atcy.reset_index(drop=True, inplace=True)

    atcyq = atcy[atcy['Quarter'] == quarter]
    atcyq.reset_index(drop=True, inplace=True)
    atcyq_1 = atcyq.groupby('State')["Transaction_Count"].sum()
    df1 = pd.DataFrame(atcyq_1)
    df1.reset_index(inplace=True)
    sorted_trans = df1.sort_values(by='Transaction_Count', ascending=False).head(10)
    sorted_trans.reset_index(drop=True, inplace=True)
    
    fig = px.bar(sorted_trans, x='State', y='Transaction_Count', hover_data=['State', 'Transaction_Count'], 
             height=400, color_discrete_sequence=px.colors.sequential.Rainbow_r)
    st.plotly_chart(fig)

# DataFrame to display the top 10 districts for Map Transactions

def map_trans_cnt(df, year, quarter):

    mtcy = df[df['Year'] == year]
    mtcy.reset_index(drop=True, inplace=True)

    mtcyq = mtcy[mtcy['Quarter'] == quarter]
    mtcyq.reset_index(drop=True, inplace=True)
    mtcyq_1 = mtcyq.groupby('District')['Transaction_Count'].sum()
    df2 = pd.DataFrame(mtcyq_1)
    df2.reset_index(inplace=True)
    sorted_df2 = df2.sort_values(by='Transaction_Count', ascending=False).head(10)
    sorted_df2.reset_index(drop=True, inplace=True)
    st.dataframe(sorted_df2.set_index(sorted_df2.columns[0]))

#plot2

def map_trans_cnt_plot(df, year, quarter):

    mtcy = df[df['Year'] == year]
    mtcy.reset_index(drop=True, inplace=True)

    mtcyq = mtcy[mtcy['Quarter'] == quarter]
    mtcyq.reset_index(drop=True, inplace=True)
    mtcyq_1 = mtcyq.groupby('District')["Transaction_Count"].sum()
    df2 = pd.DataFrame(mtcyq_1)
    df2.reset_index(inplace=True)
    sorted_df2 = df2.sort_values(by="Transaction_Count", ascending=False).head(10)
    
    fig = px.bar(sorted_df2, x='District', y='Transaction_Count', hover_data=['District', 'Transaction_Count'], 
             height=400, color_discrete_sequence=px.colors.sequential.haline_r)
    st.plotly_chart(fig)

#DataFrame to display the top 10 Postal codes for Top Transactions

def top_trans_cnt(df, year, quarter):

    ttcy = df[df['Year'] == year]
    ttcy.reset_index(drop=True, inplace=True)

    ttcyq = ttcy[ttcy['Quarter'] == quarter]
    ttcyq.reset_index(drop=True, inplace=True)
    ttcyq_1 = ttcyq.groupby('Pincode')["Transaction_Count"].sum()
    df3 = pd.DataFrame(ttcyq_1)
    df3.reset_index(inplace=True)
    df3['Pincode'] = df3['Pincode'].astype(str).str.zfill(5)
    sorted_df3 = df3.sort_values(by="Transaction_Count", ascending=False).head(10)
    sorted_df3.reset_index(drop=True, inplace=True)
    st.dataframe(sorted_df3.set_index(sorted_df3.columns[0]))

#plot_3
def top_trans_cnt_plot(df, year, quarter):

    ttcy = df[df['Year'] == year]
    ttcy.reset_index(drop=True, inplace=True)

    ttcyq = ttcy[ttcy['Quarter'] == quarter]
    ttcyq.reset_index(drop=True, inplace=True)
    ttcyq_1 = ttcyq.groupby('Pincode')["Transaction_Count"].sum()
    df3 = pd.DataFrame(ttcyq_1)
    df3.reset_index(inplace=True)
    df3['Pincode'] = df3['Pincode'].astype(str).str.zfill(5)
    df3['Pincode'] = df3['Pincode'].astype(str)
    sorted_df3 = df3.sort_values(by="Transaction_Count", ascending=False).head(10)
    fig = px.bar(sorted_df3, x='Pincode', y='Transaction_Count', hover_data=['Pincode', 'Transaction_Count'], 
             height=400, color_discrete_sequence=px.colors.sequential.haline_r)
    fig.update_xaxes(type='category')
    fig.update_layout(xaxis={'categoryorder':'total descending'})
    st.plotly_chart(fig)

#DataFrame to display the transactions based on categories for the selected year and quarter

def categories_agg_trans(df, year, quarter):
    
    caty = df[df['Year'] == year]
    caty.reset_index(drop=True, inplace=True)
    
    catyq = caty[caty['Quarter'] == quarter]
    catyq.reset_index(drop=True, inplace=True)

    catyq_1 = catyq.groupby('Transaction_Type')["Transaction_Count"].sum()
    df3 = pd.DataFrame(catyq_1)
    df3.reset_index(inplace=True)
    sorted_df3 = df3.sort_values(by='Transaction_Count', ascending=False)
    st.dataframe(sorted_df3.set_index(sorted_df3.columns[0]))

    st.markdown("#### :orange[Categories Transaction Count]")
    fig_pie_C= px.pie(sorted_df3, names= "Transaction_Type", values= "Transaction_Count",
                            width= 400, color_discrete_sequence=px.colors.sequential.Magenta_r)
    st.plotly_chart(fig_pie_C)

#Display Statewise Registered Users count

def agg_user_reg(df, year, quarter):
    aury = df[df['Year'] == year]
    aury.reset_index(drop=True, inplace=True)
    
    auryq = aury[aury['Quarter'] == quarter]
    auryq.reset_index(drop=True, inplace=True)

    auryq_1 = auryq.groupby('State')["Registered_Users"].sum()
    df3 = pd.DataFrame(auryq_1)
    df3.reset_index(inplace=True)
    sorted_df3 = df3.sort_values(by='Registered_Users', ascending=False)
    st.dataframe(sorted_df3.set_index(sorted_df3.columns[0]))

#plot to Display Statewise Registered Users count

def agg_user_reg_plot(df, year, quarter):
    aury = df[df['Year'] == year]
    aury.reset_index(drop=True, inplace=True)
    
    auryq = aury[aury['Quarter'] == quarter]
    auryq.reset_index(drop=True, inplace=True)

    auryq_1 = auryq.groupby('State')["Registered_Users"].sum()
    df3 = pd.DataFrame(auryq_1)
    df3.reset_index(inplace=True)
    fig_pie_1= px.pie(data_frame= df3, names= "State", values= "Registered_Users",
                            width= 600)
    st.plotly_chart(fig_pie_1)

#Display District wise Registered Users count

def map_user_reg(df, year, quarter):
    mury = df[df['Year'] == year]
    mury.reset_index(drop=True, inplace=True)
    
    muryq = mury[mury['Quarter'] == quarter]
    muryq.reset_index(drop=True, inplace=True)

    muryq_1 = muryq.groupby('District')["Registered_Users"].sum()
    df3 = pd.DataFrame(muryq_1)
    df3.reset_index(inplace=True)
    sorted_df3 = df3.sort_values(by='Registered_Users', ascending=False)
    st.dataframe(sorted_df3.set_index(sorted_df3.columns[0]))

#plot to Display Districts Registered Users count

def map_user_reg_plot(df, year, quarter):
    mury = df[df['Year'] == year]
    mury.reset_index(drop=True, inplace=True)
    
    muryq = mury[mury['Quarter'] == quarter]
    muryq.reset_index(drop=True, inplace=True)

    muryq_1 = muryq.groupby('District')["Registered_Users"].sum()
    df3 = pd.DataFrame(muryq_1)
    df3.reset_index(inplace=True)
    sorted_df3 = df3.sort_values(by='Registered_Users', ascending=False).head(10)
    fig_pie_1= px.pie(data_frame= sorted_df3, names= "District", values= "Registered_Users",
                            width= 400)
    st.plotly_chart(fig_pie_1)

#Display Pincode wise Registered Users count

def top_user_reg(df, year, quarter):
    tury = df[df['Year'] == year]
    tury.reset_index(drop=True, inplace=True)
    
    turyq = tury[tury['Quarter'] == quarter]
    turyq.reset_index(drop=True, inplace=True)

    turyq_1 = turyq.groupby('Pincode')["Registered_Users"].sum()
    df3 = pd.DataFrame(turyq_1)
    df3.reset_index(inplace=True)
    df3['Pincode'] = df3['Pincode'].astype(str).str.zfill(5)
    sorted_df3 = df3.sort_values(by='Registered_Users', ascending=False)
    st.dataframe(sorted_df3.set_index(sorted_df3.columns[0]))

#plot to Display Pincodes Registered Users count

def top_user_reg_plot(df, year, quarter):
    tury = df[df['Year'] == year]
    tury.reset_index(drop=True, inplace=True)
    
    turyq = tury[tury['Quarter'] == quarter]
    turyq.reset_index(drop=True, inplace=True)

    turyq_1 = turyq.groupby('Pincode')["Registered_Users"].sum()
    df3 = pd.DataFrame(turyq_1)
    df3.reset_index(inplace=True)
    df3['Pincode'] = df3['Pincode'].astype(str).str.zfill(5)
    sorted_df3 = df3.sort_values(by='Registered_Users', ascending=False).head(10)
    fig_pie_1= px.pie(data_frame= sorted_df3, names= "Pincode", values= "Registered_Users",
                            width= 400)
    st.plotly_chart(fig_pie_1)

#Display transactions based on the Brand of Phone

def agg_user_brand(df, year, quarter):
    auby = df[df['Year'] == year]
    auby.reset_index(drop=True, inplace=True)
    
    aubyq = auby[auby['Quarter'] == quarter]
    aubyq.reset_index(drop=True, inplace=True)

    aubyq_1 = aubyq.groupby('Brand')["Registered_Users_Brand"].sum()
    df3 = pd.DataFrame(aubyq_1)
    df3.reset_index(inplace=True)
    sorted_df3 = df3.sort_values(by='Registered_Users_Brand', ascending=False)
    st.dataframe(sorted_df3.set_index(sorted_df3.columns[0]))



#plot to display transactions based on Brand of phone

def agg_user_brand_plot(df, year, quarter):
    auby = df[df['Year'] == year]
    auby.reset_index(drop=True, inplace=True)
    
    aubyq = auby[auby['Quarter'] == quarter]
    aubyq.reset_index(drop=True, inplace=True)

    aubyq_1 = aubyq.groupby('Brand')["Registered_Users_Brand"].sum()
    df3 = pd.DataFrame(aubyq_1)
    df3.reset_index(inplace=True)
    sorted_df3 = df3.sort_values(by='Registered_Users_Brand', ascending=False)
    st.markdown("#### :orange[Brand wise transactions]")
    fig_pie_1= px.pie(data_frame= sorted_df3, names= "Brand", values= "Registered_Users_Brand",
                            width= 600)
    st.plotly_chart(fig_pie_1)


#Streamlit coding part

img1 = Image.open(r"C:/Users/navee/OneDrive/Desktop/DataScience/phonepe/phonepe.png")

img =Image.open(r"C:/Users/navee/OneDrive/Desktop/DataScience/phonepe/pulse_logo.gif")

st.set_page_config(page_title="PhonePe Pulse", page_icon=img, layout="wide", initial_sidebar_state="expanded")

logo = Image.open(r"C:/Users/navee/OneDrive/Desktop/DataScience/phonepe/phonepe.png")

col1, col2 =st.columns((0.1,2))
with col1:
    st.image(logo, width=40)

with col2:
    st.markdown("#### :white[PhonePe Pulse | THE BEAT OF PROGRESS]")

st.write("")

with st.sidebar:
    selected = option_menu(menu_title="PhonePe Pulse", menu_icon='view-stacked',
                           options = ["Home", "Explore Data", "Insights"],
                           icons= ["house", "globe-central-south-asia", "bar-chart"])

img_india = Image.open(r"C:/Users/navee/OneDrive/Desktop/DataScience/phonepe/india.png")
img_india_1 = Image.open(r"C:/Users/navee/OneDrive/Desktop/DataScience/phonepe/india1.png")

if selected == "Home":
    col1, col2 = st.columns(2)
    with col1:
        st.image(img_india, width=400)
    with col2:
        st.markdown('''PhonePe Pulse is a window to the world of how India transacts with interesting trends,
                     deep insights and in-depth analysis based on the data put together by the PhonePe team.''')
        st.markdown('''The Indian digital payments story has truly captured the world's imagination.
                     From the largest towns to the remotest villages, there is a payments revolution being
                     driven by the penetration of mobile phones and data.''')
        st.markdown('''When PhonePe started 5 years back, the team was constantly looking for definitive data
                     sources on digital payments in India. Some of the questions team were seeking answers to were
                     - How are consumers truly using digital payments? What are the top cases? Are kiranas across
                     Tier 2 and 3 getting a facelift with the penetration of QR codes?
                     This year as we became India's largest digital payments platform with 46% UPI market share,
                     we decided to demystify the what, why and how of digital payments in India.''')

    st.write("")
    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 =st.columns(2)

    with col1:
        st.markdown('''PhonePe Pulse platform showcases data from more than 2,000 crore transactions by
                     digital payment consumers in India, starting 2018. It will be updating this data quarterly,
                     and will open up platform application programming interfaces (APIs) to help developers
                     build better products from data insights on the platform.''')

    with col2:
        st.image(img_india_1, width=400)

    with st.expander('About', expanded=False):
        st.write('''
            - Data: [Data Link](https://github.com/PhonePe/pulse).
            - :orange[**By**]: Naveen Roy
            - :orange[**Project**]: Phonepe Pulse Data Visualization and Exploration
            - :orange[**Inspired From**]: [PhonePe Pulse](https://www.phonepe.com/pulse/)
            ''')
    
if selected == "Explore Data":

    with st.sidebar:
        st.title("All India")

        data_base = st.selectbox("Select the Type:", options=["Transactions", "Users"])

        year_list = list(Agg_Trans.Year.unique())

        quarter_list = list(Agg_Trans.Quarter.unique())

        selected_year = st.selectbox("Select the Year:", year_list)

        selected_quarter = st.selectbox("Select the Quarter:", quarter_list)
        
    if data_base == "Transactions":
        col = st.columns((4, 0.4, 3), gap='medium')

        with col[0]:
            st.markdown('#### :orange[Transactions]')
            make_choropleth(Agg_Trans, selected_year, selected_quarter)

        with col[2]:
           st.markdown("#### :orange[Categories]")
           categories_agg_trans(Agg_Trans, selected_year, selected_quarter)

        agg_trans_cnt_all(Agg_Trans, selected_year, selected_quarter)

        radio_s = st.radio("Select the option:", ('States', 'Districts', 'Postal codes'), horizontal=True)
        if radio_s == 'States':
            col = st.columns((5,1,2.5), gap='medium')

            with col[0]:
                st.markdown("#### :orange[Top 10 States for Transaction Count]")
                agg_trans_cnt_plot(Agg_Trans, selected_year, selected_quarter)

            with col[2]:
                agg_trans_cnt(Agg_Trans, selected_year, selected_quarter)

        if radio_s == 'Districts':
            col = st.columns((5,1,2.5), gap='medium')

            with col[0]:
                st.markdown("#### :orange[Top 10 Districts for Transaction Count]")
                map_trans_cnt_plot(Map_Transactions, selected_year, selected_quarter)
                
            with col[2]:
                map_trans_cnt(Map_Transactions, selected_year, selected_quarter)

        if radio_s == 'Postal codes':
            col = st.columns((5,1,2.5), gap='medium')

            with col[0]:
                st.markdown("#### :orange[Top 10 Postal codes for Transaction Count]")
                top_trans_cnt_plot(Top_Transactions, selected_year, selected_quarter)
                
            with col[2]:
                top_trans_cnt(Top_Transactions, selected_year, selected_quarter)


    if data_base == "Users":
        
        st.markdown('### :orange[Users]')
        registered_users_map(Agg_Users_Quarter, selected_year, selected_quarter)

        radio_s = st.radio("Select the option:", ('States', 'Districts', 'Postal codes'), horizontal=True)
        if radio_s == 'States':
            col = st.columns((5,1,5), gap='medium')

            with col[0]:
                st.markdown("#### :orange[States wise Registered Users Count]")
                agg_user_reg(Agg_Users_Quarter, selected_year, selected_quarter)

            with col[2]:
                st.markdown("#### :orange[Registered users Count for the States]")
                agg_user_reg_plot(Agg_Users_Quarter, selected_year, selected_quarter)

        if radio_s == 'Districts':
            col = st.columns((5,1,5), gap='medium')

            with col[0]:
                st.markdown("#### :orange[Districts wise Registered Users Count]")
                map_user_reg(Map_Users, selected_year, selected_quarter)
                
            with col[2]:
                st.markdown("##### :orange[Top 10 Districts Registered Users count]")
                map_user_reg_plot(Map_Users, selected_year, selected_quarter)

        if radio_s == 'Postal codes':
            col = st.columns((5,1,5), gap='medium')

            with col[0]:
                st.markdown("#### :orange[Postal codes wise Registered Users Count]")
                top_user_reg(Top_Users, selected_year, selected_quarter)
                
            with col[2]:
                st.markdown("#### :orange[Top 10 Postal codes for Registered Users Count]")
                top_user_reg_plot(Top_Users, selected_year, selected_quarter)

if selected == "Insights":

    # SQL Connection
    mydb = db.connect(host="localhost",
                            user="root",
                            password="Spidey@123$",
                            database="phonepe",
                            port=3306)

    mycursor = mydb.cursor()

    col = st.columns((3, 3, 3), gap='medium')

    with col[0]:
        year1 = st.selectbox("Select the Year:", options=('2018', '2019', '2020', '2021', '2022', '2023'))
        st.write("##### :blue[Peer-to-peer payments shows the highest transaction value every year]")
        Query1 = f'''select transaction_type, sum(transaction_amount) as Total_amt,
                sum(transaction_count) as Total_transactions, year from aggregated_transactions where year={year1}
                group by transaction_type, year order by year, Total_transactions desc'''

        mycursor.execute(Query1)
        mydb.commit()

        t1 = mycursor.fetchall()
        df_1 = pd.DataFrame(t1, columns=("Transaction_type", "Transaction_Amount", "Total_Transactions", "Year"))

        fig_1 = px.bar(df_1, x="Transaction_type", y="Transaction_Amount",
                       title="Transaction amount for each Category", hover_name="Transaction_type",
                       color_discrete_sequence=px.colors.sequential.Bluered_r, height=300, width=300)
        st.plotly_chart(fig_1)

    with col[1]:
        st.write("##### :blue[As more Indian consumers prioritise contactless payments, the behaviour is bound to solidify with time - growing Merchant payments by leaps.]")
        Query2 = f'''select transaction_type, sum(transaction_amount) as tot_amt,
                     sum(transaction_count) as Total_transactions, year from aggregated_transactions
                     group by transaction_type, year order by year, tot_amt desc'''

        mycursor.execute(Query2)
        mydb.commit()

        t2 = mycursor.fetchall()
        df_2 = pd.DataFrame(t2, columns=("Transaction_type", "Transaction_Amount", "Total_Transactions", "Year"))

        fig_2 = px.bar(df_2, x="Transaction_type", y="Total_Transactions",
                       title="Transaction count for each Category", hover_name="Transaction_type",
                       color_discrete_sequence=px.colors.sequential.Viridis_r, height=300, width=300)
        st.plotly_chart(fig_2)

    with col[2]:
        st.write("##### :blue[Phenomenal growth of Phonepe transactions from past few years]")

        Query3 = '''select sum(transaction_count) as Transactions, year from aggregated_transactions
                     group by year order by year'''
        mycursor.execute(Query3)
        mydb.commit()

        t3 = mycursor.fetchall()
        df_3 = pd.DataFrame(t3, columns=("Transactions", "Year"))
        df_3['Year'] = df_3['Year'].astype(str).str.zfill(4)
        df_3['Year'] = df_3['Year'].astype(str)
        fig_3 = px.line(df_3, x="Year", y="Transactions", title="Transaction Count for each Year",
                        hover_data=['Year', 'Transactions'],
                        color_discrete_sequence=px.colors.sequential.YlGn, height=300, width=300)
        fig_3.update_xaxes(type='category')
        fig_3.update_layout(xaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig_3)

    col = st.columns((6, 3), gap='medium')

    with col[0]:
        st.write("##### :blue[Top 10 districts that registered app]")

        Query4 = '''select sum(registered_users) as Registered_users, sum(app_open) as App_open,
                     District from map_users 
                    group by district order by Registered_users desc limit(10)'''

        mycursor.execute(Query4)
        mydb.commit()

        t4 = mycursor.fetchall()
        df_4 = pd.DataFrame(t4, columns=("Registered_users", "App_open", "District"))
        fig_4 = px.pie(df_4, values='Registered_users', names='District', hover_data='District',
                       hover_name='District',
                       color_discrete_sequence=px.colors.sequential.Rainbow, height=300, width=600)
        st.plotly_chart(fig_4)

    with col[1]:
        st.write("##### :blue[Top 10 Postal codes that registered app]")

        Query5 = '''select pincode, sum(registered_users) as Registered_users from top_users
                     group by pincode order by Registered_users desc limit(10)'''

        mycursor.execute(Query5)
        mydb.commit()

        t5 = mycursor.fetchall()
        df_5 = pd.DataFrame(t5, columns=("pincode", "Registered_users"))
        fig_5 = px.pie(df_5, values='Registered_users', names='pincode', hover_data='pincode', hover_name='pincode',
                       color_discrete_sequence=px.colors.sequential.Viridis_r, height=300, width=300)
        st.plotly_chart(fig_5)

    col = st.columns((3, 6), gap='medium')

    with col[1]:
        st.write("##### :blue[Brandwise Registered Users till 2022 Q2]")

        Query6 = '''select Brand, sum(registered_users_brand) as Registered_users from aggregated_users
                     group by brand order by registered_users'''
        mycursor.execute(Query6)
        mydb.commit()

        t6 = mycursor.fetchall()
        df_6 = pd.DataFrame(t6, columns=("Brand", "Registered_users"))
        fig_6 = px.bar(df_6, x="Brand", y="Registered_users", title="Brand wise Registered users till 2022 Q2",
                       hover_data=['Brand', 'Registered_users'],
                       color_discrete_sequence=px.colors.sequential.Rainbow_r, height=300, width=300)
        st.plotly_chart(fig_6)

    with col[2]:
        st.write("##### :blue[State wise Registered users]")

        Query7 = '''select State, sum(registered_users) as Registered_users from aggregated_users_quarter
                     group by state order by Registered_users'''

        mycursor.execute(Query7)
        mydb.commit()

        t7 = mycursor.fetchall()
        df_7 = pd.DataFrame(t7, columns=("State", "Registered_users"))
        fig_7 = px.bar(df_7, x='Registered_users', y='State', hover_data='Registered_users', hover_name='State',
                       color_discrete_sequence=px.colors.sequential.PuBu, height=500, width=600, orientation='h')
        st.plotly_chart(fig_7)

    col = st.columns((3, 1, 3), gap='medium')

    with col[1]:

        st.write("##### :blue[App opens and Registered users increases each year]")

        Query8 = '''select sum(registered_users) as Registered_users, sum(app_opens) as App_opens,
                     Year from aggregated_users_quarter group by year order by year'''

        mycursor.execute(Query8)
        mydb.commit()

        t8 = mycursor.fetchall()
        df_8 = pd.DataFrame(t8, columns=("Registered_users", "App_opens", "Year"))
        df_8['Year'] = df_8['Year'].astype(str).str.zfill(4)
        df_8['Year'] = df_8['Year'].astype(str)
        fig_8 = px.line(df_8, x='Year', y=['Registered_users', 'App_opens'], width=400)
        st.plotly_chart(fig_8)

    with col[2]:
        st.write("##### :blue[Metro cities of Hyderabad and Bengaluru are the largest contributers for 2023 Q4]")

        Query9 = '''select Pincode, sum(transaction_amount) as Transactions_amount from top_transactions
                     where year=2023 and quarter=4 group by pincode order by Transactions_amount desc limit(10)'''

        mycursor.execute(Query9)
        mydb.commit()

        t9 = mycursor.fetchall()
        df_9 = pd.DataFrame(t9, columns=("Pincode", "Transactions_amount"))
        df_9['Pincode'] = df_9['Pincode'].astype(str).str.zfill(5)
        df_9['Pincode'] = df_9['Pincode'].astype(str)
        fig_9 = px.line(df_9, x='Pincode', y='Transactions_amount', width=400)
        fig_9.update_xaxes(type='category')
        fig_9.update_layout(xaxis={'categoryorder': 'total descending'})
        st.plotly_chart(fig_9)

    mycursor.close()
    mydb.close()

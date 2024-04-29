import os
import json
import pandas as pd
import mysql.connector as db
#aggregate_transaction

path1 = "C:/Users/navee/OneDrive/Desktop/DataScience/phonepe/pulse/data/aggregated/transaction/country/india/state/"
aggregate_trans_list = os.listdir(path1)

columns1 = {"State":[], "Year":[], "Quarter":[], "Transaction_Type":[], "Transaction_Count":[], "Transaction_Amount":[]}

for state in aggregate_trans_list:
    current_states = path1+state+"/"
    aggregate_year_list = os.listdir(current_states)

    for year in aggregate_year_list:
        current_year = current_states+year+"/"
        aggregate_file_list = os.listdir(current_year)
        
        for quarter in aggregate_file_list:
            current_quarter = current_year+quarter
            data = open(current_quarter, "r")

            A = json.load(data)
            for i in A["data"]["transactionData"]:
                name = i["name"]
                count = i["paymentInstruments"][0]["count"]
                amount = i["paymentInstruments"][0]["amount"]
                columns1["Transaction_Type"].append(name)
                columns1["Transaction_Count"].append(count)
                columns1["Transaction_Amount"].append(amount)
                columns1["State"].append(state)
                columns1["Year"].append(year)
                columns1["Quarter"].append(int(quarter.strip(".json")))

Aggregate_Transactions = pd.DataFrame(columns1)
Aggregate_Transactions["State"] = Aggregate_Transactions['State'].str.replace("andaman-&-nicobar-islands", "Andaman & Nicobar Islands")
Aggregate_Transactions["State"] = Aggregate_Transactions['State'].str.replace('-', ' ')
Aggregate_Transactions["State"] = Aggregate_Transactions['State'].str.title()
Aggregate_Transactions["State"] = Aggregate_Transactions['State'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

import os
import json
import pandas as pd

# Aggregate users quarter wise
path2 = "C:/Users/navee/OneDrive/Desktop/DataScience/phonepe/pulse/data/aggregated/user/country/india/state/"
aggregate_user_list = os.listdir(path2)

columns2 = {"State": [], "Year": [], "Quarter": [], "Registered_Users": [], "App_Opens": []}

for state in aggregate_user_list:
    current_states = os.path.join(path2, state) + "/"
    aggregate_year_list = os.listdir(current_states)

    for year in aggregate_year_list:
        current_year = os.path.join(current_states, year) + "/"
        aggregate_file_list = os.listdir(current_year)

        for quarter in aggregate_file_list:
            current_quarter = os.path.join(current_year, quarter)
            try:
                with open(current_quarter, "r") as data_file:
                    B = json.load(data_file)
                    aggregated_data = B.get("data", {}).get("aggregated", {})
                    users = aggregated_data.get("registeredUsers")
                    appopens = aggregated_data.get("appOpens")
                    if users is not None and appopens is not None:
                        columns2["Registered_Users"].append(users)
                        columns2["App_Opens"].append(appopens)
                        columns2["State"].append(state)
                        columns2["Year"].append(year)
                        columns2["Quarter"].append(int(quarter.strip(".json")))
            except Exception as e:
                print("Error processing file:", current_quarter, "-", e)

# Create DataFrame
try:
    Aggregate_Users_1 = pd.DataFrame(columns2)
    # Replace state names
    Aggregate_Users_1["State"] = Aggregate_Users_1['State'].str.replace("andaman-&-nicobar-islands", "Andaman & Nicobar Islands")
    Aggregate_Users_1["State"] = Aggregate_Users_1['State'].str.replace('-', ' ')
    Aggregate_Users_1["State"] = Aggregate_Users_1['State'].str.title()
    Aggregate_Users_1["State"] = Aggregate_Users_1['State'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
except AttributeError as attr_err:
    print(f"Attribute Error: {attr_err}")

#aggregate_user

path2 = "C:/Users/navee/OneDrive/Desktop/DataScience/phonepe/pulse/data/aggregated/user/country/india/state/"
aggregate_user_list = os.listdir(path2)

columns2 = {"State":[], "Year":[], "Quarter":[], "Brand":[], "Registered_Users_Brand":[], "Percentage":[]}

for state in aggregate_user_list:
    current_states = path2+state+"/"
    aggregate_year_list = os.listdir(current_states)

    for year in aggregate_year_list:
        current_year = current_states+year+"/"
        aggregate_file_list = os.listdir(current_year)
        
        for quarter in aggregate_file_list:
            current_quarter = current_year+quarter
            data = open(current_quarter, "r")

            B = json.load(data)
            try:
                for i in B["data"]["usersByDevice"]:                    
                    brand = i["brand"]
                    count = i["count"]
                    percentage = i["percentage"]
                    columns2["Brand"].append(brand)
                    columns2["Registered_Users_Brand"].append(count)
                    columns2["Percentage"].append(percentage)
                    columns2["State"].append(state)
                    columns2["Year"].append(year)
                    columns2["Quarter"].append(int(quarter.strip(".json")))
            except:
                pass

Aggregate_Users = pd.DataFrame(columns2)
Aggregate_Users["State"] = Aggregate_Users['State'].str.replace("andaman-&-nicobar-islands", "Andaman & Nicobar Islands")
Aggregate_Users["State"] = Aggregate_Users['State'].str.replace('-', ' ')
Aggregate_Users["State"] = Aggregate_Users['State'].str.title()
Aggregate_Users["State"] = Aggregate_Users['State'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

#map_transaction

import os
import json
import pandas as pd

path3 = "C:/Users/navee/OneDrive/Desktop/DataScience/phonepe/pulse/data/map/transaction/hover/country/india/state/"
map_trans_list = os.listdir(path3)

columns3 = {"State":[], "Year":[], "Quarter":[], "District":[], "Transaction_Count":[], "Transaction_Amount":[]}

for state in map_trans_list:
    current_states = os.path.join(path3, state) + "/"
    map_year_list = os.listdir(current_states)

    for year in map_year_list:
        current_year = os.path.join(current_states, year) + "/"
        map_file_list = os.listdir(current_year)
        
        for quarter in map_file_list:
            current_quarter = os.path.join(current_year, quarter)
            try:
                with open(current_quarter, "r") as data_file:
                    C = json.load(data_file)
                    if "data" in C and "hoverDataList" in C["data"]:
                        for i in C["data"]["hoverDataList"]:
                            name = i.get("name")
                            count = i.get("metric", [{"count": None}])[0].get("count")
                            amount = i.get("metric", [{"amount": None}])[0].get("amount")
                            if name is not None and count is not None and amount is not None:
                                columns3["District"].append(name)
                                columns3["Transaction_Count"].append(count)
                                columns3["Transaction_Amount"].append(amount)
                                columns3["State"].append(state)
                                columns3["Year"].append(year)
                                columns3["Quarter"].append(int(quarter.strip(".json")))
            except Exception as e:
                print("Error processing file:", current_quarter, "-", e)

# Create DataFrame
Map_Transactions = pd.DataFrame(columns3)

# Replace state names
Map_Transactions["State"] = Map_Transactions['State'].str.replace("andaman-&-nicobar-islands", "Andaman & Nicobar Islands")
Map_Transactions["State"] = Map_Transactions['State'].str.replace('-', ' ')
Map_Transactions["State"] = Map_Transactions['State'].str.title()
Map_Transactions["State"] = Map_Transactions['State'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
 
#map_user

path4 = "C:/Users/navee/OneDrive/Desktop/DataScience/phonepe/pulse/data/map/user/hover/country/india/state/"
map_user_list = os.listdir(path4)

columns4 = {"State":[], "Year":[], "Quarter":[], "District":[], "Registered_Users":[], "App_Open":[]}

for state in map_user_list:
    current_states = path4+state+"/"
    map_year_list = os.listdir(current_states)

    for year in map_year_list:
        current_year = current_states+year+"/"
        map_file_list = os.listdir(current_year)
        
        for quarter in map_file_list:
            current_quarter = current_year+quarter
            data = open(current_quarter, "r")

            D = json.load(data)
            for i in D["data"]["hoverData"].items():
                district = i[0]
                registeredUsers = i[1]["registeredUsers"]
                appOpens = i[1]["appOpens"]
                columns4["District"].append(district)
                columns4["Registered_Users"].append(registeredUsers)
                columns4["App_Open"].append(appOpens)
                columns4["State"].append(state)
                columns4["Year"].append(year)
                columns4["Quarter"].append(int(quarter.strip(".json")))

Map_Users = pd.DataFrame(columns4)
Map_Users["State"] = Map_Users['State'].str.replace("andaman-&-nicobar-islands", "Andaman & Nicobar Islands")
Map_Users["State"] = Map_Users['State'].str.replace('-', ' ')
Map_Users["State"] = Map_Users['State'].str.title()
Map_Users["State"] = Map_Users['State'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

#top_transaction

path5 = "C:/Users/navee/OneDrive/Desktop/DataScience/phonepe/pulse/data/top/transaction/country/india/state/"
top_trans_list = os.listdir(path5)

columns5 = {"State":[], "Year":[], "Quarter":[], "Pincode":[], "Transaction_Count":[], "Transaction_Amount":[]}

for state in top_trans_list:
    current_states = path5+state+"/"
    top_year_list = os.listdir(current_states)

    for year in top_year_list:
        current_year = current_states+year+"/"
        top_file_list = os.listdir(current_year)
        
        for quarter in top_file_list:
            current_quarter = current_year+quarter
            data = open(current_quarter, "r")

            E = json.load(data)
            for i in E["data"]["pincodes"]:
                entityname = i["entityName"]
                count = i["metric"]["count"]
                amount = i["metric"]["amount"]
                columns5["Pincode"].append(entityname)
                columns5["Transaction_Count"].append(count)
                columns5["Transaction_Amount"].append(amount)
                columns5["State"].append(state)
                columns5["Year"].append(year)
                columns5["Quarter"].append(int(quarter.strip(".json")))

Top_Transactions = pd.DataFrame(columns5)
Top_Transactions["State"] = Top_Transactions['State'].str.replace("andaman-&-nicobar-islands", "Andaman & Nicobar Islands")
Top_Transactions["State"] = Top_Transactions['State'].str.replace('-', ' ')
Top_Transactions["State"] = Top_Transactions['State'].str.title()
Top_Transactions["State"] = Top_Transactions['State'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")  

Top_Transactions.fillna(method='ffill', inplace=True)

Top_Transactions.isnull().values.sum()

#top_user

path6 = "C:/Users/navee/OneDrive/Desktop/DataScience/phonepe/pulse/data/top/user/country/india/state/"
top_user_list = os.listdir(path6)

columns6 = {"State":[], "Year":[], "Quarter":[], "Pincode":[], "Registered_Users":[]}

for state in top_user_list:
    current_states = path6+state+"/"
    top_year_list = os.listdir(current_states)

    for year in top_year_list:
        current_year = current_states+year+"/"
        top_file_list = os.listdir(current_year)
        
        for quarter in top_file_list:
            current_quarter = current_year+quarter
            data = open(current_quarter, "r")

            F = json.load(data)
            for i in F["data"]["pincodes"]:
                name = i["name"]
                registeredusers = i["registeredUsers"]
                columns6["Pincode"].append(name)
                columns6["Registered_Users"].append(registeredusers)
                columns6["State"].append(state)
                columns6["Year"].append(year)
                columns6["Quarter"].append(int(quarter.strip(".json")))

Top_Users = pd.DataFrame(columns6)
Top_Users["State"] = Top_Users['State'].str.replace("andaman-&-nicobar-islands", "Andaman & Nicobar Islands")
Top_Users["State"] = Top_Users['State'].str.replace('-', ' ')
Top_Users["State"] = Top_Users['State'].str.title()
Top_Users["State"] = Top_Users['State'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

#SQL Connection
mydb = db.connect(host="127.0.0.1", port="3306", user="root", password="Spidey@123$", database= "phonepe")
mycursor = mydb.cursor()

#Creating table and inserting data in SQL for Aggregated Users Quarter wise

create_table_agg_users_1 = '''CREATE TABLE if not exists Aggregated_Users_Quarter(State varchar(255),
                                                                                    Year int,
                                                                                    Quarter int,
                                                                                    Registered_Users bigint,
                                                                                    App_Opens bigint)'''
mycursor.execute(create_table_agg_users_1)
mydb.commit()

insert_table_agg_users_1 = '''INSERT INTO Aggregated_Users_Quarter(State, Year,
                                                         Quarter,
                                                         Registered_Users,
                                                         App_Opens)
                                                                
                                                         values(%s,%s,%s,%s,%s)'''

data = Aggregate_Users_1.values.tolist()
mycursor.executemany(insert_table_agg_users_1, data)
mydb.commit()

#Tables Creation

#SQL Connection
mycursor = mydb.cursor()
#Creating table and inserting data in SQL for Aggregated Transactions
create_table_agg_trans = '''CREATE TABLE if not exists Aggregated_Transactions(State varchar(255),
                                                                Year int,
                                                                Quarter int,
                                                                Transaction_Type varchar(255),
                                                                Transaction_Count bigint,
                                                                Transaction_Amount bigint)'''
mycursor.execute(create_table_agg_trans)
mydb.commit()

insert_table_agg_trans = '''INSERT INTO Aggregated_Transactions(State, Year,
                                                                Quarter, Transaction_Type,
                                                                Transaction_Count,
                                                                Transaction_Amount)
                                                                
                                                                values(%s,%s,%s,%s,%s,%s)'''

data = Aggregate_Transactions.values.tolist()
mycursor.executemany(insert_table_agg_trans, data)
mydb.commit()

#Creating table and inserting data in SQL for Aggregated Users

create_table_agg_users = '''CREATE TABLE if not exists Aggregated_Users(State varchar(255),
                                                                Year int,
                                                                Quarter int,
                                                                Brand varchar(255),
                                                                Registered_Users_Brand bigint,
                                                                Percentage float)'''
mycursor.execute(create_table_agg_users)
mydb.commit()

insert_table_agg_users = '''INSERT INTO Aggregated_Users(State, Year,
                                                         Quarter, Brand,
                                                         Registered_Users_Brand,
                                                         Percentage)
                                                                
                                                         values(%s,%s,%s,%s,%s,%s)'''

data = Aggregate_Users.values.tolist()
mycursor.executemany(insert_table_agg_users, data)
mydb.commit()

mycursor = mydb.cursor()

#Creating table and inserting data in SQL for Map Transactions

create_table_map_trans = '''CREATE TABLE if not exists Map_Transactions(State varchar(255),
                                                                        Year int,
                                                                        Quarter int,
                                                                        District varchar(255),
                                                                        Transaction_Count bigint,
                                                                        Transaction_Amount bigint)'''
mycursor.execute(create_table_map_trans)
mydb.commit()

insert_table_map_trans = '''INSERT INTO Map_Transactions(State, Year,
                                                        Quarter, District,
                                                        Transaction_Count,
                                                        Transaction_Amount)
                                                        
                                                        values(%s,%s,%s,%s,%s,%s)'''

data = Map_Transactions.values.tolist()
mycursor.executemany(insert_table_map_trans, data)
mydb.commit()

#Creating table and inserting data in SQL for Map Users

create_table_map_users = '''CREATE TABLE if not exists Map_Users(State varchar(255),
                                                                Year int,
                                                                Quarter int,
                                                                District varchar(255),
                                                                Registered_Users bigint,
                                                                App_Open bigint)'''
mycursor.execute(create_table_map_users)
mydb.commit()

insert_table_map_users = '''INSERT INTO Map_Users(State, Year,
                                                Quarter, District,
                                                Registered_Users,
                                                App_Open)
                                                    
                                                values(%s,%s,%s,%s,%s,%s)'''

data = Map_Users.values.tolist()
mycursor.executemany(insert_table_map_users, data)
mydb.commit()

#Creating table and inserting data in SQL for Top Transactions

create_table_top_trans = '''CREATE TABLE if not exists Top_Transactions(State varchar(255),
                                                                        Year int,
                                                                        Quarter int,
                                                                        Pincode int,
                                                                        Transaction_Count bigint,
                                                                        Transaction_Amount bigint)'''
mycursor.execute(create_table_top_trans)
mydb.commit()

insert_table_top_trans = '''INSERT INTO Top_Transactions(State, Year,
                                                        Quarter, Pincode,
                                                        Transaction_Count,
                                                        Transaction_Amount)
                                                        
                                                        values(%s,%s,%s,%s,%s,%s)'''

data = Top_Transactions.values.tolist()
mycursor.executemany(insert_table_top_trans, data)
mydb.commit()

#Creating table and inserting data in SQL for Top Users

create_table_top_users = '''CREATE TABLE if not exists Top_Users(State varchar(255),
                                                                Year int,
                                                                Quarter int,
                                                                Pincode int,
                                                                Registered_Users bigint)'''
mycursor.execute(create_table_top_users)
mydb.commit()

insert_table_top_users = '''INSERT INTO Top_Users(State, Year,
                                                Quarter, Pincode,
                                                Registered_Users)
                                                    
                                                values(%s,%s,%s,%s,%s)'''

data = Top_Users.values.tolist()
mycursor.executemany(insert_table_top_users, data)
mydb.commit()


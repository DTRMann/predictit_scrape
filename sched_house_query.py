import urllib3
import requests
import xml.etree.ElementTree as ET
import pandas as pd
import sqlite3
import apscheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime

def get_data(url):
    header = {'accept': 'application/xml'}
    r = requests.get(url, headers=header)
    tree = ET.fromstring(r.content)
    return tree

def list_markets(ET_object):
    for market in ET_object.findall('./Markets/MarketData'):
        name = market.find('ShortName').text
        ID = market.find('ID').text
        print(name)
        print('ID: ',ID,'\n')
    return

url='https://www.predictit.org/api/marketdata/all/'
tree = get_data(url)

def query_ID(mkt_ID):
    mkt_ID = str(mkt_ID)
    # Populate list with all of the IDs
    l_IDs = []
    for market in tree.findall('./Markets/MarketData'):
        name = market.find('ShortName').text
        ID_apnd = market.find('ID').text
        l_IDs.append(ID_apnd)
    if mkt_ID not in l_IDs:
        print("Ticker not in list.")
        return
    else:
        url = 'https://www.predictit.org/api/marketdata/markets/' + mkt_ID
    mkt_data = get_data(url)
    print(url)
    print(mkt_data.find('ShortName').text)
    return mkt_data 

def market_info(ID):
    prices = {}
    for root in ID.findall('.'):
        print("ID:",root.find('ID').text)
        name = 'ID'
        info = root.find('ID').text
        prices[name]=info   
        print("Name: ",root.find('Name').text)
        name = 'Name'
        info = root.find('Name').text
        prices[name]=info
        print("Timestamp: ",root.find('TimeStamp').text)
        name = 'TimeStamp'
        info = root.find('TimeStamp').text
        prices[name]=info

    for info in ID.findall('./Contracts/MarketContract'):
        name = info.find('Name').text
        price = info.find('LastTradePrice').text
        print(name,':',price)
        prices[name]=price
    return prices

def add_to_db(data_dict): 
    ID_str = str(data_dict['ID']) + '.db'
    con = sqlite3.connect(ID_str)
    data_df = pd.DataFrame([data_dict],columns=data_dict.keys())
    try:
        data_db = pd.read_sql('select * from data', con)
    except:
        pd.DataFrame.to_sql(data_df,con=con,name='data')
        con.commit()
        con.close()
        return print('Done! Database added.')
    data_db = data_db.append(data_df,sort=False)
    data_db_push = data_db.drop_duplicates(subset='TimeStamp')
    pd.DataFrame.to_sql(data_db_push,con=con,name='data', if_exists='replace',index = False)
    con.commit()
    con.close()
    return print('Done!')

def house_data_query(ID = '2704'):
    url='https://www.predictit.org/api/marketdata/all/'
    tree = get_data(url)
    data = query_ID(ID)
    prices = market_info(data)
    add_to_db(prices)
    return

def scheduled_task(function_name):
    sched = BlockingScheduler()
    sched.add_job(function_name, 'interval', minutes=5, start_date='2018-11-04 12:00:00', end_date='2018-11-08 00:00:00')
    sched.start()
    return

scheduled_task(house_data_query)


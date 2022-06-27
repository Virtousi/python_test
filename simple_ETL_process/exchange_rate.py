#Following project is a simple ETL to tranform GBP to USD
import requests
import pandas as pd
import datetime 

#Bank information from the Professional Engineer IBM course
bank_url_1 = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_1.json'
bank_url_2 = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_2.json'
bank_url_list = [bank_url_1,bank_url_2]
#Taken from apilayer.com to get the exchange rate
exchange_rate_url = "https://api.apilayer.com/exchangerates_data/latest?base=EUR&apikey=xnDlYemPmC9AV04CMRerkSI7I8D5FgpC"

dfs = []
pd.set_option("display.max_rows", None, "display.max_columns", None)


#Create a function to extract and convert into dataframe
def extract_from_json(files_to_process) :
    for x in files_to_process:
        dfs.append(pd.read_json(x))
        df = pd.concat(dfs, sort=False)
    return df

#Create a function to get specific exchange rate
def extract_exchangerate(currency_name) :
    r = requests.get(exchange_rate_url)
    rate_data = r.json()
    df_exchange = pd.DataFrame(rate_data,columns=['rates'])
    exchange_rate = df_exchange.loc[currency_name,'rates']
    return exchange_rate

#Create a function to transfrom USD to GDP
def transform(data, exchange_rate1, exchange_rate2):

    usd2gbp = exchange_rate1/exchange_rate2
    data.loc[:, "Market Cap (GBP$ Billion)"] = round(data.iloc[:, 1] / usd2gbp, 3)
    df_gbp = data.drop("Market Cap (US$ Billion)", axis=1)
    return df_gbp, usd2gbp
    
#Create a function to load data into csv files
def load(data):
    data.to_csv('bank_market_cap_gbp.csv')



#main body of the code

#Extract all data from json files
data = extract_from_json(bank_url_list)
#Convert USD to GDP
USD_exchangerate =  extract_exchangerate('USD')
GDP_exchangerate =  extract_exchangerate('GBP')

#Transform Data to convert from USD to GDP
df_gbp, usd2gbp = transform(data,USD_exchangerate,GDP_exchangerate)

#Load Data
load (df_gbp)





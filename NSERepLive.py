import streamlit as st
import requests
import numpy as np
import pandas as pd
import datetime
import schedule
import time
import py_vollib_vectorized
from py_vollib_vectorized import get_all_greeks
# from IPython.display import display, HTML
# from IPython.display import clear_output
#import mysql.connector
# from tabulate import tabulate

placeholder = st.empty()
symbol ="BANKNIFTY"

url ="https://www.nseindia.com/api/option-chain-indices?symbol="+ symbol


headers = {
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "en-US,en;q=0.9",
        "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
}
niftymaindf = pd.DataFrame({"VegaSum":[0],
                            "ThetaSum":[0],
                            "Time":[datetime.datetime.now()]})

r = 0.1
iv = 0.0

expiry_date1 = ['2023-03-16']
expiry_date2 = ['2023-03-23']
expiry_date3 = ['2023-03-29']
Ni_CurrentWeekFix = pd.DataFrame()
Ni_NextWeekFix = pd.DataFrame()
Ni_MonthExpiryFix = pd.DataFrame()
def Main():    
    #Create Nifty morning datacls
    funcGetDataContinues()

def funcGetDataMorning():
    global Ni_CurrentWeekFix
    global Ni_NextWeekFix
    global Ni_MonthExpiryFix
    
    try:   
        session = requests.Session()
        data = session.get(url, headers=headers).json()["records"]["data"]
        ocdata = []
        for i in data:
            for j,k in i.items():
                if j == "CE" or j == "PE":
                    info = k
                    info["instrumentType"] = j
                    ocdata.append(info)
        
        df = pd.DataFrame(ocdata)
        df['implied_volatility'] = 0.0
        df['expiryDate']= pd.to_datetime(df['expiryDate'])
        df['Timestamp'] = session.get(url="https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY", headers=headers).json()["records"]["timestamp"]
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%d-%b-%Y %H:%M:%S')
        

        df.drop(['identifier'], axis=1, inplace=True)
        df.drop(['change'], axis=1, inplace=True)
        df.drop(['changeinOpenInterest'], axis=1, inplace=True)
        df.drop(['pchangeinOpenInterest'], axis=1, inplace=True)
        df.drop(['pChange'], axis=1, inplace=True)
        df.drop(['totalBuyQuantity'], axis=1, inplace=True)
        df.drop(['totalSellQuantity'], axis=1, inplace=True)
        df.drop(['bidQty'], axis=1, inplace=True)
        df.drop(['bidprice'], axis=1, inplace=True)
        df.drop(['askQty'], axis=1, inplace=True)
        df.drop(['askPrice'], axis=1, inplace=True)
        df.drop(['impliedVolatility'], axis=1, inplace=True)
        #df.dropna(subset=['lastPrice'], how='any', inplace=True)
        #df = df[df['lastPrice'] != 0]
       
        Ni_CurrentWeekFix = df[df['expiryDate'].isin(expiry_date1)]
        Ni_NextWeekFix = df[df['expiryDate'].isin(expiry_date2)]
        Ni_MonthExpiryFix = df[df['expiryDate'].isin(expiry_date3)]

        #create 1st expiry data
        Ni_CurrentWeekFix = calculate_implied_volatility(Ni_CurrentWeekFix)
        Ni_CurrentWeekFix = calculate_option_greeks(Ni_CurrentWeekFix)
        Ni_CurrentWeekFix = Ni_CurrentWeekFix.replace(np.nan,0)
        
        #create 2nd expiry data
        Ni_NextWeekFix = calculate_implied_volatility(Ni_NextWeekFix)
        Ni_NextWeekFix = calculate_option_greeks(Ni_NextWeekFix)
        Ni_NextWeekFix = Ni_NextWeekFix.replace(np.nan,0)

        #create 3rd expiry data
        Ni_MonthExpiryFix = calculate_implied_volatility(Ni_MonthExpiryFix)
        Ni_MonthExpiryFix = calculate_option_greeks(Ni_MonthExpiryFix)
        Ni_MonthExpiryFix = Ni_MonthExpiryFix.replace(np.nan,0)
        
        print("\033c", end="")
        print("Morning file run successfully!")
        with placeholder.container():
         st.write("Morning file run successfully!")

    except Exception as e:
        print(e, " Func Error -> Data morning one time!!")

def funcGetDataContinues():    
        # if len(Ni_CurrentWeekFix.index) > 0 and len(Ni_NextWeekFix.index) > 0 and len(Ni_MonthExpiryFix.index) > 0:
            global Ni_CurrentWeek
            global Ni_NextWeek
            global Ni_MonthExpiry
            try:   
                session = requests.Session()
                data = session.get(url, headers=headers).json()["records"]["data"]
                ocdata = []
                for i in data:
                    for j,k in i.items():
                        if j == "CE" or j == "PE":
                            info = k
                            info["instrumentType"] = j
                            ocdata.append(info)
                df = pd.DataFrame(ocdata)
                df['implied_volatility'] = 0.0
                df['expiryDate']= pd.to_datetime(df['expiryDate'])
                df['Timestamp'] = session.get(url="https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY", headers=headers).json()["records"]["timestamp"]
                df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%d-%b-%Y %H:%M:%S')
                df['changeInVega'] = 0.0
                df['changeInOI'] = 0.0
                df['changeInVol'] = 0.0
                

                df.drop(['identifier'], axis=1, inplace=True)
                df.drop(['change'], axis=1, inplace=True)
                df.drop(['changeinOpenInterest'], axis=1, inplace=True)
                df.drop(['pchangeinOpenInterest'], axis=1, inplace=True)
                df.drop(['pChange'], axis=1, inplace=True)
                df.drop(['totalBuyQuantity'], axis=1, inplace=True)
                df.drop(['totalSellQuantity'], axis=1, inplace=True)
                df.drop(['bidQty'], axis=1, inplace=True)
                df.drop(['bidprice'], axis=1, inplace=True)
                df.drop(['askQty'], axis=1, inplace=True)
                df.drop(['askPrice'], axis=1, inplace=True)
                df.drop(['impliedVolatility'], axis=1, inplace=True)
                #df.dropna(subset=['lastPrice'], how='any', inplace=True)
                #df = df[df['lastPrice'] != 0]
                
                Ni_CurrentWeek = df[df['expiryDate'].isin(expiry_date1)]
                Ni_NextWeek = df[df['expiryDate'].isin(expiry_date2)]
                Ni_MonthExpiry = df[df['expiryDate'].isin(expiry_date3)]

                #create 1st expiry data
                Ni_CurrentWeek = calculate_implied_volatility(Ni_CurrentWeek)
                Ni_CurrentWeek = calculate_option_greeks(Ni_CurrentWeek)
                Ni_CurrentWeek = Ni_CurrentWeek.replace(np.nan,0)
                
                #create 2nd expiry data
                Ni_NextWeek = calculate_implied_volatility(Ni_NextWeek)
                Ni_NextWeek = calculate_option_greeks(Ni_NextWeek)
                Ni_NextWeek = Ni_NextWeek.replace(np.nan,0)

                #create 3rd expiry data
                Ni_MonthExpiry = calculate_implied_volatility(Ni_MonthExpiry)
                Ni_MonthExpiry = calculate_option_greeks(Ni_MonthExpiry)
                Ni_MonthExpiry = Ni_MonthExpiry.replace(np.nan,0)
                
                # calculate_Final_Report(Ni_CurrentWeek)    
                # print(Ni_CurrentWeek)
                calculate_Final_Report(Ni_CurrentWeek,Ni_NextWeek,Ni_MonthExpiry,Ni_CurrentWeekFix,Ni_NextWeekFix,Ni_MonthExpiryFix)        
                
            except Exception as e:
                print(e, " Func Error- continue data fetching!!")
        # else:
        #     print("Morning file has no data.")
        #     funcGetDataMorning()
    
def calculate_implied_volatility(df):
    
    try:
        for i, row in df.iterrows():
            option_type = row['instrumentType']
            if option_type == 'CE':
                option_type = 'c'
            else:
                option_type = 'p'
            s = row['underlyingValue']
            k = row['strikePrice']
            #time calculation
            expiry_date = datetime.datetime.strptime(row['expiryDate'].strftime('%d-%b-%Y') + " 15:30:00", '%d-%b-%Y %H:%M:%S')
            t = (expiry_date - row['Timestamp']).total_seconds() / (365 * 24 * 60 * 60)
            df.loc[i,'t'] = t
            price = row['lastPrice']
            iv = py_vollib_vectorized.vectorized_implied_volatility_black(price, s, k, r, t, option_type, return_as='numpy')
            df.loc[i,'implied_volatility'] = iv
        return df 
    except Exception as e:
        print(e, "  IV Calculation error - An exception occurred")
    
def calculate_option_greeks(df):
    try:        
        for i, row in df.iterrows():
            flag = 'c' if row['instrumentType'] == 'CE' else 'p'
            R = 0.1
            greeks = get_all_greeks(flag, row['underlyingValue'], row['strikePrice'],row['t'], R, row['implied_volatility'], model='black_scholes', return_as='dict')
            df.at[i, 'delta'] = greeks['delta']
            df.at[i, 'vega'] = greeks['vega']
        return df
    except Exception as e:
        print(e , "- greek cal func")  

def calculate_Final_Report(Ni_CurrentWeek,Ni_NextWeek,Ni_MonthExpiry,Ni_CurrentWeekFix,Ni_NextWeekFix,Ni_MonthExpiryFix):     
    if Ni_CurrentWeek.empty and Ni_NextWeek.empty and Ni_MonthExpiry.empty:
        with placeholder.container():
          st.write("Second Data Frame Is Blank So Calling funcGetDataContinues() Again!")
        funcGetDataContinues()
    if Ni_CurrentWeekFix.empty and Ni_NextWeekFix.empty and Ni_MonthExpiryFix.empty:
        with placeholder.container():
            st.write("Morning Data Frame Is Blank So Calling funcGetDataContinues() Again!")
        funcGetDataMorning()
    try:
        Ni_CurrentWeek['changeInVega'] = 0
        Ni_CurrentWeek['changeInOI'] = 0
        Ni_CurrentWeek['changeInVol'] = 0
        # iterate over the rows of the second dataframe and perform the subtraction
        for index, row in Ni_CurrentWeek.iterrows():
            # find the matching row in the first dataframe based on the values in the first two columns
            matching_row_cw = Ni_CurrentWeekFix.loc[(Ni_CurrentWeekFix['strikePrice'] == row['strikePrice']) & (Ni_CurrentWeekFix['instrumentType'] == row['instrumentType'])]

            # check if a matching row was found and perform the subtraction
            if len(matching_row_cw) > 0:
                Ni_CurrentWeek.loc[index, 'changeInVega'] = row['vega'] - matching_row_cw.iloc[0]['vega']
                Ni_CurrentWeek.loc[index, 'changeInOI'] = row['openInterest'] - matching_row_cw.iloc[0]['openInterest'] 
                Ni_CurrentWeek.loc[index, 'changeInVol'] = row['totalTradedVolume'] - matching_row_cw.iloc[0]['totalTradedVolume']
            else:
                Ni_CurrentWeek.drop(index, inplace=True)
                
        Ni_NextWeek['changeInVega'] = 0
        Ni_NextWeek['changeInOI'] = 0
        Ni_NextWeek['changeInVol'] = 0

        # iterate over the rows of the second dataframe and perform the subtraction
        for index, row in Ni_NextWeek.iterrows():
            # find the matching row in the first dataframe based on the values in the first two columns
            matching_row_nw = Ni_NextWeekFix.loc[(Ni_NextWeekFix['strikePrice'] == row['strikePrice']) & (Ni_NextWeekFix['instrumentType'] == row['instrumentType'])]
            # check if a matching row was found and perform the subtraction
            if len(matching_row_nw) > 0:
                Ni_NextWeek.loc[index, 'changeInVega'] = row['vega'] - matching_row_nw.iloc[0]['vega'] 
                Ni_NextWeek.loc[index, 'changeInOI'] = row['openInterest'] - matching_row_nw.iloc[0]['openInterest'] 
                Ni_NextWeek.loc[index, 'changeInVol'] = row['totalTradedVolume'] - matching_row_nw.iloc[0]['totalTradedVolume'] 
            else:
                Ni_NextWeek.drop(index, inplace=True)
                       
        Ni_MonthExpiry['changeInVega'] = 0
        Ni_MonthExpiry['changeInOI'] = 0
        Ni_MonthExpiry['changeInVol'] = 0
        # iterate over the rows of the second dataframe and perform the subtraction
        for index, row in Ni_MonthExpiry.iterrows():
            # find the matching row in the first dataframe based on the values in the first two columns
            matching_row_m = Ni_MonthExpiryFix.loc[(Ni_MonthExpiryFix['strikePrice'] == row['strikePrice']) & (Ni_MonthExpiryFix['instrumentType'] == row['instrumentType'])]

            # check if a matching row was found and perform the subtraction
            if len(matching_row_m) > 0:
                Ni_MonthExpiry.loc[index, 'changeInVega'] = row['vega'] - matching_row_m.iloc[0]['vega'] 
                Ni_MonthExpiry.loc[index, 'changeInOI'] = row['openInterest'] - matching_row_m.iloc[0]['openInterest']  
                Ni_MonthExpiry.loc[index, 'changeInVol'] = row['totalTradedVolume'] - matching_row_m.iloc[0]['totalTradedVolume'] 
            else:
                Ni_MonthExpiry.drop(index, inplace=True)
          
        
        VegaCESumCurrWeek = (Ni_CurrentWeek.loc[(Ni_CurrentWeek['instrumentType'] == 'CE') & (Ni_CurrentWeek['delta'] <= 0.6), 'changeInVega'].sum())
        VegaPESumCurrWeek = (Ni_CurrentWeek.loc[(Ni_CurrentWeek['instrumentType'] == 'PE') & (Ni_CurrentWeek['delta'] >= -0.6), 'changeInVega'].sum())
        VegaCESumNextWeek = (Ni_NextWeek.loc[(Ni_NextWeek['instrumentType'] == 'CE') & (Ni_NextWeek['delta'] <= 0.6), 'changeInVega'].sum()) 
        VegaPESumNextWeek = (Ni_NextWeek.loc[(Ni_NextWeek['instrumentType'] == 'PE') & (Ni_NextWeek['delta'] >= -0.6), 'changeInVega'].sum()) 
        VegaCESumMonth = (Ni_MonthExpiry.loc[(Ni_MonthExpiry['instrumentType'] == 'CE') & (Ni_MonthExpiry['delta'] <= 0.6), 'changeInVega'].sum())
        VegaPESumMonth = (Ni_MonthExpiry.loc[(Ni_MonthExpiry['instrumentType'] == 'PE') & (Ni_MonthExpiry['delta'] >= -0.6), 'changeInVega'].sum())
        # OI sum calculation
        OICESumCurrWeek = (Ni_CurrentWeek.loc[(Ni_CurrentWeek['instrumentType'] == 'CE') & (Ni_CurrentWeek['delta'] <= 0.6), 'changeInOI'].sum()) 
        OIPESumCurrWeek = (Ni_CurrentWeek.loc[(Ni_CurrentWeek['instrumentType'] == 'PE') & (Ni_CurrentWeek['delta'] >= -0.6), 'changeInOI'].sum())
        OICESumNextWeek = (Ni_NextWeek.loc[(Ni_NextWeek['instrumentType'] == 'CE') & (Ni_NextWeek['delta'] <= 0.6), 'changeInOI'].sum())
        OIPESumNextWeek = (Ni_NextWeek.loc[(Ni_NextWeek['instrumentType'] == 'PE') & (Ni_NextWeek['delta'] >= -0.6), 'changeInOI'].sum()) 
        OICESumMonth = (Ni_MonthExpiry.loc[(Ni_MonthExpiry['instrumentType'] == 'CE') & (Ni_MonthExpiry['delta'] <= 0.6), 'changeInOI'].sum())
        OIPESumMonth = (Ni_MonthExpiry.loc[(Ni_MonthExpiry['instrumentType'] == 'PE') & (Ni_MonthExpiry['delta'] >= -0.6), 'changeInOI'].sum())
        
        #Volume sum calculation
        VolCESumCurrWeek = (Ni_CurrentWeek.loc[(Ni_CurrentWeek['instrumentType'] == 'CE') & (Ni_CurrentWeek['delta'] <= 0.6), 'changeInVol'].sum())
        VolPESumCurrWeek = (Ni_CurrentWeek.loc[(Ni_CurrentWeek['instrumentType'] == 'PE') & (Ni_CurrentWeek['delta'] >= -0.6), 'changeInVol'].sum())
        VolCESumNextWeek = (Ni_NextWeek.loc[(Ni_NextWeek['instrumentType'] == 'CE') & (Ni_NextWeek['delta'] <= 0.6), 'changeInVol'].sum()) 
        VolPESumNextWeek = (Ni_NextWeek.loc[(Ni_NextWeek['instrumentType'] == 'PE') & (Ni_NextWeek['delta'] >= -0.6), 'changeInVol'].sum())
        VolCESumMonth = (Ni_MonthExpiry.loc[(Ni_MonthExpiry['instrumentType'] == 'CE') & (Ni_MonthExpiry['delta'] <= 0.6), 'changeInVol'].sum()) 
        VolPESumMonth = (Ni_MonthExpiry.loc[(Ni_MonthExpiry['instrumentType'] == 'PE') & (Ni_MonthExpiry['delta'] >= -0.6), 'changeInVol'].sum())
        
        
        #Final report creation
        data = {'Date': [expiry_date1, expiry_date1,expiry_date2, expiry_date2,expiry_date3, expiry_date3],
                'Type': ['Call', 'Put','Call', 'Put', 'Call', 'Put'],
                'Vega': [VegaCESumCurrWeek, VegaPESumCurrWeek,VegaCESumNextWeek,VegaPESumNextWeek,VegaCESumMonth,VegaPESumMonth],
                'OI': [OICESumCurrWeek, OIPESumCurrWeek,OICESumNextWeek,OIPESumNextWeek,OICESumMonth,OIPESumMonth],
                'Volume': [VolCESumCurrWeek, VolPESumCurrWeek,VolCESumNextWeek,VolPESumNextWeek,VolCESumMonth,VolPESumMonth],    
                'Vega_Ratio': [(VegaCESumCurrWeek/min(VegaCESumCurrWeek,VegaPESumCurrWeek)),
                               (VegaPESumCurrWeek/min(VegaCESumCurrWeek,VegaPESumCurrWeek)),
                               (VegaCESumNextWeek/min(VegaCESumNextWeek,VegaPESumNextWeek)),
                               (VegaPESumNextWeek/min(VegaCESumNextWeek,VegaPESumNextWeek)),
                               (VegaCESumMonth/min(VegaCESumMonth,VegaPESumMonth)),
                               (VegaPESumMonth/min(VegaCESumMonth,VegaPESumMonth))],
                'OI_Ratio': [(OICESumCurrWeek/min(OICESumCurrWeek,OIPESumCurrWeek)),
                               (OIPESumCurrWeek/min(OICESumCurrWeek,OIPESumCurrWeek)),
                               (OICESumNextWeek/min(OICESumNextWeek,OIPESumNextWeek)),
                               (OIPESumNextWeek/min(OICESumNextWeek,OIPESumNextWeek)),
                               (OICESumMonth/min(OICESumMonth,OIPESumMonth)),
                               (OIPESumMonth/min(OICESumMonth,OIPESumMonth))],
                'Volume_Ratio': [(VolCESumCurrWeek/min(VolCESumCurrWeek,VolPESumCurrWeek)),
                               (VolPESumCurrWeek/min(VolCESumCurrWeek,VolPESumCurrWeek)),
                               (VolCESumNextWeek/min(VolCESumNextWeek,VolPESumNextWeek)),
                               (VolPESumNextWeek/min(VolCESumNextWeek,VolPESumNextWeek)),
                               (VolCESumMonth/min(VolCESumMonth,VolPESumMonth)),
                               (VolPESumMonth/min(VolCESumMonth,VolPESumMonth))]
                               }          
        #Cumm variable cal
        VegaCECumm = VegaCESumCurrWeek+VegaCESumNextWeek+VegaCESumMonth
        VegaPECumm = VegaPESumCurrWeek+VegaPESumNextWeek+VegaPESumMonth
        OICECumm = OICESumCurrWeek+OICESumNextWeek+OICESumMonth
        OIPECumm = OIPESumCurrWeek+OIPESumNextWeek+OIPESumMonth
        VolCECumm = VolCESumCurrWeek+VolCESumNextWeek+VolCESumMonth
        VolPECumm = VolPESumCurrWeek+VolPESumNextWeek+VolPESumMonth

        cummData ={'cummType' : ['Call','Put'],
                'cummVega' : [VegaCECumm, VegaPECumm],
                'cummOI' : [OICECumm, OIPECumm],
                'cummVolume' : [VolCECumm, VolPECumm],                
                'VegaCummRatio' : [(VegaCECumm/min(VegaCECumm,VegaPECumm)),(VegaPECumm/min(VegaCECumm,VegaPECumm))],
                'OICummRatio' : [(OICECumm/min(OICECumm,OIPECumm)),(OIPECumm/min(OICECumm,OIPECumm))],
                'VolumeCummRatio' : [(VolCECumm/min(VolCECumm,VolPECumm)),(VolPECumm/min(VolCECumm,VolPECumm))]
        }
        
        dfMain = pd.DataFrame(data)
        dfMain = dfMain.replace(np.nan,0)
        dfCummMain = pd.DataFrame(cummData)
        dfCummMain = dfCummMain.replace(np.nan,0)
       
        # clear_output()
        print("\033c", end="") 
        
        session = requests.Session()
        Timestamp = session.get(url="https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY", headers=headers).json()["records"]["timestamp"]
        Nifty_spot = session.get(url="https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY", headers=headers).json()["records"]["underlyingValue"]
        Banknifty_spot = session.get(url="https://www.nseindia.com/api/option-chain-indices?symbol=BANKNIFTY", headers=headers).json()["records"]["underlyingValue"]
        # print(f'Timestamp: {Timestamp}',f'Nifty spot: {Nifty_spot}',f'Banknifty spot: {Banknifty_spot}')
        # # display(HTML(dfMain.to_html()))
        # # display(HTML(dfCummMain.to_html()))
        # print("BANKNIFTY VEGA DATA")
        # print(dfMain)
        # print(dfCummMain)    

        placeholder.empty()
        with placeholder.container():
            st.write(f'Timestamp: {Timestamp}',f'Nifty spot: {Nifty_spot}',f'Banknifty spot: {Banknifty_spot}')
            st.write("BANKNIFTY VEGA DATA")
            st.dataframe(dfMain)
            st.dataframe(dfCummMain)
        # pullDataToSQLDFMain(dfMain)
        # pullDataToSQLDFCUmm(dfCummMain)

    except Exception as e:
        print(e, " Func Error- Final report!!")

# based on timestamp changed -> 1st run time will 9.20(should be today's date) then other 9.21
schedule.every().day.at("12:14").do(funcGetDataMorning)
schedule.every(1).minutes.do(Main)
# NewTimestampNo = datetime.datetime.now()

while True:
    # session = requests.Session()    
    # TimestampNo = session.get(url="https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY", headers=headers).json()["records"]["timestamp"]
    # Timestamp = datetime.datetime.strptime(TimestampNo,'%d-%b-%Y %H:%M:%S').date()
    # todayd = datetime.datetime.today().date()
    # if Timestamp == todayd:
        #Date matched - Report will be available soon.  
    with placeholder.container():
            st.write("App is started")
    schedule.run_pending()
    time.sleep(60)
        
        
     
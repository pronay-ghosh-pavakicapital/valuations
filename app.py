# import modules
# import streamlit as st

import streamlit as st
from tqdm import tqdm
import pandas as pd
import numpy as np
import requests
import time


sheet_id_database_data = '1O04GWbcZztoIadx1KK2CQ8X58ZqPQcRqpv4ck4Yxz54'
sheet_name_database_data = 'sheet2'
url_database_data = f'https://docs.google.com/spreadsheets/d/{sheet_id_database_data}/gviz/tq?tqx=out:csv&sheet={sheet_name_database_data}'
database_data = pd.read_csv(url_database_data)

sheet_id_select_reject = '1eripQyKChiD0co-xrl22mFV6MCwlSJzm7m7DscIubOI'
sheet_name_select_reject = 'data_columns'
url_select_reject = f'https://docs.google.com/spreadsheets/d/{sheet_id_select_reject}/gviz/tq?tqx=out:csv&sheet={sheet_name_select_reject}'
data = pd.read_csv(url_select_reject)


selected_data = data[data["Select/Reject"] == "select"]
selected_data = data[data["Select/Reject"] == "select"]
balance_sheet_corpus = selected_data["data_columns"]
reject_list = data[data["Select/Reject"] == "reject"]


#  This method returns the cik and the ticker given a company name
def return_ticker_data(ticker):
    return_data = pd.DataFrame({"cik_str":[0],
                                "ticker":[0],
                                "title":[0]})
    company_ticker = ticker
    # create request header
    headers = {'User-Agent': "ghosh.pronay18071997@gmail.com"}

    # get all companies data
    companyTickers = requests.get(
        "https://www.sec.gov/files/company_tickers.json",
        headers=headers
        )
    # format response to dictionary and get first key/value
    firstEntry = companyTickers.json()['0']
    # parse CIK // without leading zeros
    directCik = companyTickers.json()['0']['cik_str']
    # dictionary to dataframe
    companyData = pd.DataFrame.from_dict(companyTickers.json(),
                                        orient='index')
    
    for tick in companyData["ticker"]:
        if tick  == company_ticker:
            return_data = companyData[companyData["ticker"]== tick]

    cik = "000"+str(return_data["cik_str"].values[0])
    if len(cik) < 10:
        cik_buffer = 10 - len(cik)
        cik = "0"*cik_buffer +cik
    else:
        cik = cik
    ticker = return_data["ticker"].values[0]
    company_name = return_data["title"].values[0]
    print(return_data["cik_str"])
    print(return_data["ticker"])
    print(return_data["title"])
    return  cik,ticker,company_name


def get_asset_data_given_cik(cik):
    #get company concept data
    headers = {'User-Agent': "ghosh.pronay18071997@gmail.com"}
    companyConcept = requests.get(
        (
        f'https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}'
        f'/us-gaap/Assets.json'
        ),
        headers=headers
        )

    # review data
    companyConcept.json().keys()
    companyConcept.json()['units']
    companyConcept.json()['units'].keys()
    companyConcept.json()['units']['USD']
    companyConcept.json()['units']['USD'][0]

    # parse assets from single filing
    companyConcept.json()['units']['USD'][0]['val']

    # get all filings data 
    assetsData = pd.DataFrame.from_dict((
                companyConcept.json()['units']['USD']))
    return assetsData



def filter_accn(accn_data):
    accn_cleaned_list = []
    for accn in accn_data:
        print(accn)
        accn_cleaned = accn.split("-")[1]
        accn = accn.replace("-","")
        accn_cleaned_list.append(accn)
        return accn_cleaned_list[0]
    


# This method scrapes the latest_balance_sheet_data given accn
# This method scrapes the latest_balance_sheet_data given accn
def scrape_latest_balance_sheet_and_standerdize_columns(accn, balance_sheet_corpus,cik):
    """
    args:
    - accn : the accn of the company
    - balance sheet corpus: the overall data of the (select reject list) that has been defined above

    retrun:
    - the final balance sheet data in a dataframe --> var name : mydata
    """
    balance_sheet_cols_list = []
    html_report_list = []
    accn_folder_url = "https://www.sec.gov/Archives/edgar/data/"+cik+"/"+accn
    header = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest"
    }
    r = requests.get(accn_folder_url, headers=header)
    print("request successful for ", accn_folder_url)
    dfs = pd.read_html(r.text)
    mydata = dfs[0]
    for report in tqdm(mydata["Name"]):
        if report.startswith("R"):
            if report.endswith("htm"):
                print(report)
                html_report_list.append(report)
    flag = 0
    for report_index in tqdm(html_report_list):
        if flag == 1:
            break

        data_url = "https://www.sec.gov/Archives/edgar/data/"+ cik + "/" + accn + "/"+report_index
        # print()
        print("Trying for ", data_url)
        try :
            header = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"
            }
            time.sleep(2)
            r = requests.get(data_url, headers=header)
            if r.status_code != 200:
                continue
            else:
                print("request successful for ", data_url)
                dfs = pd.read_html(r.text)
                mydata = dfs[0]
                keywords_list = ["Balance Sheet","BALANCE SHEET","balance sheet"]
                for key in tqdm(keywords_list):
                    if key in mydata.columns:
                        print("Searching for {0} in Regular Index Level".format(key))
                        print(mydata.columns[0])
                        # if mydata.columns[0] in balance_sheet_corpus:
                        #     balance_sheet_cols_list.append(mydata.columns)
                        # mydata.to_csv(ticker+cik+accn+report_index+"_balance_sheeet_data.csv")
                        # print("Saving data...",mydata)
                        time.sleep(2)
                    elif key in mydata.columns[0]:
                        print("Searching for {0} in Multi Index Level".format(key))
                        balance_keyword = mydata.columns[0]
                        # Lowering the exact keyword
                        # balance_keyword= balance_keyword.lower()
                        # Removing starting and ending white spaces
                        balance_keyword=balance_keyword.rstrip()
                        print("Starting White space removed!!!!")
                        balance_keyword=balance_keyword.lstrip()
                        print("Leading White space removed!!!!")
                        print("The Exact Balance Keyword is :", balance_keyword)
                        print("Checking if the exact keyword is there in the balance_sheet_keyword_list or not")
                        # If any combination with the keyword "Parenthetical" found then don't download the data
                        # if ("Parenthetical" not in balance_keyword) or ("Parenthetical".lower() not in balance_keyword) or ("Parenthetical".upper() not in balance_keyword):
                        if balance_keyword in reject_list:
                            continue
                        if "Supplemental" in balance_keyword:
                            continue
                        if "supplemental" in balance_keyword:
                            continue
                        if "(Parenthetical)" in balance_keyword:
                            continue
                        if "Parenthetical" in balance_keyword:
                            continue
                        if "(parenthetical)" in balance_keyword:
                            continue
                        if "parenthetical" in balance_keyword:
                            continue
                        else:
                            if balance_keyword in balance_sheet_corpus:
                                print("Exact Match Found!!!!!!!!!!")
                                # mydata.to_csv(ticker+cik+accn+report_index+".csv")
                                print("Saving data...",mydata)
                                mydata = mydata.fillna(0)
                                mydata.to_csv("./original_data_folder/data.csv")
                                
                            elif "($)  " in balance_keyword:
                                print("[($)  ] found..." )
                                print("Checking for [  $]..." )
                                if "  $" in balance_keyword:
                                    """
                                    Return the mydata from here.

                                    """
                                    # print("[  $] found in data...")
                                    # balance_keyword = balance_keyword.replace("($)  ", "($) ")
                                    # mydata.to_csv(ticker+cik+accn+report_index+".csv")
                                    print("Saving data...",mydata)
                                    mydata = mydata.fillna(0)
                                    mydata.to_csv("./original_data_folder/data.csv")
                                    flag = 1
                                    break
                                    # filename = "/content/download_data/"+ticker+cik+accn+report_index+".csv" 
                            elif "  $" in balance_keyword:
                                """
                                Return the mydata from here.

                                """
                                # print("[  $] found in data...")
                                # print("removing extra white space...")
                                # balance_keyword = balance_keyword.replace("  $", " $")
                                # mydata.to_csv(ticker+cik+accn+report_index+".csv")
                                # # filename = "./downloaded_data/"+ticker+cik+accn+report_index+".csv"
                                print("Saving data...",mydata)
                                mydata = mydata.fillna(0)
                                mydata.to_csv("./original_data_folder/data.csv")
                                flag = 1
                                break
                            else:
                                print("No match found with ",balance_keyword)
                        # if mydata.columns[0][0] in balance_sheet_corpus:
                        #     balance_sheet_cols_list.append(mydata.columns[0])
                        # mydata.to_csv(ticker+cik+accn+report_index+".csv")
                        # print("Saving data...",mydata)
                        time.sleep(2)
        except Exception as e:
            print(e)
            pass
    
    return mydata


def get_stabnderdized_total_current_assets_data(database_data,data):
    """
    Arguments : database_data and data
    - database_data : Here the database data is the standerdized gsheet data
    - data: Here data refers to the downloaded balance sheet data where we will perform the overall standerdization operation
    """
    
    # Local variable declaration
    col1_cash_data = 0
    col2_cash_data = 0
    col1_market_data = 0
    col2_market_data = 0
    col1_restricted_cash_data = 0
    col2_restricted_cash_data = 0
    col1_rights_of_use_relating_leases_data = 1
    col2_rights_of_use_relating_leases_data = 0
    col1_inventories_data = 0
    col2_inventories_data = 0
    col1_account_data =0
    col2_account_data =0
    col1_other_receivables_data = 0
    col2_other_receivables_data = 0
    col1_tax_data = 0
    col2_tax_data = 0
    col1_other_current_assets_data = 0
    col2_other_current_assets_data = 0
    total_current_asset = 0
    total_current_idx = 0
    datatype_filtered_list = [int, float]
    
    
    
    # Preprocessing
    if 'Unnamed: 0' in data.columns:
        data.drop(['Unnamed: 0'], axis =1 , inplace = True)
    print(data.isnull().sum())
    print(database_data.isnull().sum())
    data.fillna(0, inplace= True)
    for col in database_data.columns:
        if database_data[col].dtypes == "object":
            database_data[col].fillna(database_data[col].mode()[0], inplace= True)

    """
    - After preprocessing we are starting the standerdization as per the given SOP
    - We are using exact search mechamnism to get and standerdize our data.
    - In this function we will standerdize for all the total current assets.
    """
    # Decalation of all Current assets line items.
    balance_col_values = data[data.columns.values[0]].values
    cash_list = database_data["Cash and cash equivalents"].values
    marketable_segments_list = database_data["Marketable securities"].values
    restricted_cash_list = database_data["Restricted cash"].values
    rights_of_use_relating_leases_list = database_data["Rights-of-use relating to leases"].values
    inventories_list = database_data["Inventories"].values
    accounts_list = database_data["Accounts receivable, net of allowances"].values
    other_receivables_list = database_data["Other receivables"].values
    tax_list = database_data["Tax"].values

    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    
    marketable_segments_list = [x.lower() for x in marketable_segments_list]
    set_marketable_segments_list = set(marketable_segments_list)
    
    marketable_segments_list = []
    for element in set_marketable_segments_list:
        marketable_segments_list.append(element)
        
    print("Unique marketable_segments_list :", marketable_segments_list)
    cash_list = [x.lower() for x in cash_list]
    restricted_cash_list = [x.lower() for x in restricted_cash_list]
    balance_col_values_total_current_assets  = [x.lower() for x in balance_col_values]
    print("balance_col_values",balance_col_values)
    
    # Searching for all possible combinations of total current assets from the database.
    for total_current_asset in database_data["Total current assets"]:
        # Turing it to lowercase for fextact filteration.
        print("total_current_asset key is :",total_current_asset.lower())
        total_current_asset_lower = total_current_asset.lower()
        if total_current_asset_lower in balance_col_values_total_current_assets:
            total_current_idx = balance_col_values_total_current_assets.index(total_current_asset_lower)
    # These are the baalnce col values for the current assets....
    balance_col_values_total_current_assets = balance_col_values_total_current_assets[:total_current_idx+1]
    
    
    for marketable_segments in balance_col_values_total_current_assets:
        if marketable_segments in marketable_segments_list:
            print("Marketable value found")
            index_of_market = balance_col_values_total_current_assets.index(marketable_segments)
            col1_market_data = data[data.columns[1]].values[index_of_market]
            print("Column 1 market Data is :",col1_market_data)
            col2_market_data = data[data.columns[2]].values[index_of_market]
            print("Column 2 market Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_market_data) in datatype_filtered_list:
                col1_market_data = col1_market_data
            if type(col2_market_data) in datatype_filtered_list:
                col2_market_data = col2_market_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_market_data:
                    col1_market_data = col1_market_data.replace("$", "")
                if ("$") in col2_market_data:
                    col2_market_data = col2_market_data.replace("$", "")
                if (",") in col1_market_data:
                    col1_market_data = col1_market_data.replace(",", "")
                if (",") in col2_market_data:
                    col2_market_data = col2_market_data.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_market_data:
                    col1_market_data = col1_market_data.replace("(", "")
                if (")") in col1_market_data:
                    col1_market_data = col1_market_data.replace(")", "")
                if ("(") in col2_market_data:
                    col2_market_data = col2_market_data.replace("(", "")
                if (")") in col2_market_data:
                    col2_market_data = col2_market_data.replace(")", "")
                if (" ") in col1_market_data:
                    col1_market_data = col1_market_data.replace(" ", "")
                if (" ") in col2_market_data:
                    col2_market_data = col2_market_data.replace(" ", "")
            print("Column 1 market Data after filteration is :",col1_market_data)
            print("Column 2 market Data after filteration is :",col2_market_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_market_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_market_data))
            
    

    # balance_col_values_total_current_assets
    market_df = pd.DataFrame({data.columns.values[0]:["Filtered marketable securities data"],
            data.columns[1]: col1_market_data,
            data.columns[2]:col2_market_data
            })
    
    print(market_df)
    
    # for restricted_cash in balance_col_values_total_current_assets:
    #     if restricted_cash in restricted_cash_list:
    #         print("restricted_cash Value Found")
    #         index_of_restricted_cash = balance_col_values_total_current_assets.index(restricted_cash)
    #         print("The index of {} is {} ".format(restricted_cash,index_of_restricted_cash))
    #         col1_restricted_cash_data = data[data.columns[1]].values[index_of_restricted_cash]
    #         print("Column 1 restricted_cash Data is :",col1_restricted_cash_data)
    #         col2_restricted_cash_data = data[data.columns[2]].values[index_of_restricted_cash]
    #         print("Column 1 restricted_cash Data is :",col2_restricted_cash_data)
    #          # Checking if the col1 and col2 datatypes are int, float or not
    #         if type(col1_restricted_cash_data) in datatype_filtered_list:
    #             col1_restricted_cash_data = col1_restricted_cash_data
    #         if type(col2_restricted_cash_data) in datatype_filtered_list:
    #             col2_restricted_cash_data = col2_restricted_cash_data
    #         else:
    #             # Cleaning ($ and ,)
    #             if ("$") in col1_restricted_cash_data:
    #                 col1_restricted_cash_data = col1_restricted_cash_data.replace("$", "")
    #             if ("$") in col2_restricted_cash_data:
    #                 col2_restricted_cash_data = col2_restricted_cash_data.replace("$", "")
    #             if (",") in col1_restricted_cash_data:
    #                 col1_restricted_cash_data = col1_restricted_cash_data.replace(",", "")
    #             if (",") in col2_restricted_cash_data:
    #                 col2_restricted_cash_data = col2_restricted_cash_data.replace(",", "")
    #             # Updated Add On (Bracket Filteration )  
    #             if ("(") in col1_restricted_cash_data:
    #                 col1_restricted_cash_data = col1_restricted_cash_data.replace("(", "")
    #             if (")") in col1_restricted_cash_data:
    #                 col1_restricted_cash_data = col1_restricted_cash_data.replace(")", "")
    #             if ("(") in col2_restricted_cash_data:
    #                 col2_restricted_cash_data = col2_restricted_cash_data.replace("(", "")
    #             if (")") in col2_restricted_cash_data:
    #                 col2_restricted_cash_data = col2_restricted_cash_data.replace(")", "")
    #         print("Column 1 restricted_cash Data after filteration is :",col1_restricted_cash_data)
    #         print("Column 2 restricted_cash Data after filteration is :",col2_restricted_cash_data)
    #         cash_eqv_and_marketable_segments_list_column1.append(int(col1_restricted_cash_data))
    #         cash_eqv_and_marketable_segments_list_column2.append(int(col2_restricted_cash_data))
    
    
    # # balance_col_values_total_current_assets
    # restricted_cash_df = pd.DataFrame({data.columns.values[0]:["Filtered restricted_cash data"],
    #         data.columns[1]: col1_restricted_cash_data,
    #         data.columns[2]:col2_restricted_cash_data
    #         })
    
    # print(restricted_cash_df)
    
    
    # getting all of the values out of the first index
    for cash in balance_col_values_total_current_assets:
        # print(cash)
        if cash in cash_list:
            print("Got it")
            print("Cash Value is :", cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            print("The index of {} is {} ".format(cash,index_of_cash))
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 Cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 1 Cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("(", "")
                if (")") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(")", "")
                if ("(") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("(", "")
                if (")") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(")", "")
                if (" ") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(" ", "")
                if (" ") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(" ", "")
            print("Column 1 Cash Data after filteration is :",col1_cash_data)
            print("Column 2 Cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    cash_df = pd.DataFrame({data.columns.values[0]:["Filtered Cash"],
            data.columns[1]: col1_cash_data,
            data.columns[2]:col2_cash_data
            })
    print(cash_df)    
    
    
    cash_and_market_df = pd.DataFrame({data.columns.values[0]:["Cash Equivalents & Marketable Sec"],
                                       data.columns[1]: [sum(cash_eqv_and_marketable_segments_list_column1)],
                                       data.columns[2]: [sum(cash_eqv_and_marketable_segments_list_column2)]
                                       })
    
    print("cash_and_market_df is.....", cash_and_market_df)
    
    """
    Rights-of-use relating to leases
    """
    balance_col_values  = data[data.columns[0]].tolist()
    balance_col_values = [x.lower() for x in balance_col_values]
    col1_rights_of_use_relating_leases_data = 0
    col2_rights_of_use_relating_leases_data = 0
    col1_rights_of_use_relating_leases_data_list = []
    col2_rights_of_use_relating_leases_data_list = []
    for rights_of_use_relating_leases in balance_col_values_total_current_assets:
        if rights_of_use_relating_leases in rights_of_use_relating_leases_list:
            print("Marketable value found")
            index_of_rights_of_use_relating_leases = balance_col_values_total_current_assets.index(rights_of_use_relating_leases)
            col1_rights_of_use_relating_leases_data = data[data.columns[1]].values[index_of_rights_of_use_relating_leases]
            print("Column 1 rights_of_use_relating_leases Data is :",col1_rights_of_use_relating_leases_data)
            col2_rights_of_use_relating_leases_data = data[data.columns[2]].values[index_of_rights_of_use_relating_leases]
            print("Column 2 rights_of_use_relating_leases Data is :",col2_rights_of_use_relating_leases_data)
            # Cleaning ($ and ,)
            if ("$") in col1_rights_of_use_relating_leases_data:
                col1_rights_of_use_relating_leases_data = col1_rights_of_use_relating_leases_data.replace("$", "")
            if ("$") in col2_rights_of_use_relating_leases_data:
                col2_rights_of_use_relating_leases_data = col2_rights_of_use_relating_leases_data.replace("$", "")
            if (",") in col1_rights_of_use_relating_leases_data:
                col1_rights_of_use_relating_leases_data = col1_rights_of_use_relating_leases_data.replace(",", "")
            if (",") in col2_rights_of_use_relating_leases_data:
                col2_rights_of_use_relating_leases_data = col2_rights_of_use_relating_leases_data.replace(",", "")
            # Updated Add On (Bracket Filteration )  
            if ("(") in col1_rights_of_use_relating_leases_data:
                col1_rights_of_use_relating_leases_data = col1_rights_of_use_relating_leases_data.replace("(", "")
            if (")") in col1_rights_of_use_relating_leases_data:
                col1_rights_of_use_relating_leases_data = col1_rights_of_use_relating_leases_data.replace(")", "")
            if ("(") in col2_rights_of_use_relating_leases_data:
                col2_rights_of_use_relating_leases_data = col2_rights_of_use_relating_leases_data.replace("(", "")
            if (")") in col2_rights_of_use_relating_leases_data:
                col2_rights_of_use_relating_leases_data = col2_rights_of_use_relating_leases_data.replace(")", "")
            col1_rights_of_use_relating_leases_data_list.append(float(col1_rights_of_use_relating_leases_data))
            col2_rights_of_use_relating_leases_data_list.append(float(col2_rights_of_use_relating_leases_data))
            print("Column 1 rights_of_use_relating_leases Data after filteration is :",col1_rights_of_use_relating_leases_data_list)
            print("Column 2 rights_of_use_relating_leases Data after filteration is :",col2_rights_of_use_relating_leases_data_list)

    if (len(col1_rights_of_use_relating_leases_data_list) > 1):
        col1_rights_of_use_relating_leases_data_list = sum(col1_rights_of_use_relating_leases_data_list)
    if (len(col2_rights_of_use_relating_leases_data_list) > 1):
        col2_rights_of_use_relating_leases_data_list = sum(col2_rights_of_use_relating_leases_data_list)
        
    if type(col2_rights_of_use_relating_leases_data_list) == list:
        pass
    if type(col2_rights_of_use_relating_leases_data_list) == list:
        pass
    if type(col1_rights_of_use_relating_leases_data_list) in  datatype_filtered_list:
        col1_rights_of_use_relating_leases_data_list = [col1_rights_of_use_relating_leases_data_list]
    if type(col2_rights_of_use_relating_leases_data_list) in  datatype_filtered_list:
        col2_rights_of_use_relating_leases_data_list = [col2_rights_of_use_relating_leases_data_list]
    
    # balance_col_values_total_current_assets
    try:
        rights_of_use_relating_leases_df = pd.DataFrame({data.columns.values[0]:["Rights-of-use relating to leases"],
                                                    data.columns[1]: [sum(col1_rights_of_use_relating_leases_data_list)],
                                                    data.columns[2]: [sum(col2_rights_of_use_relating_leases_data_list)]
                                                    })
    except Exception as e:
        pass
        rights_of_use_relating_leases_df = pd.DataFrame({data.columns.values[0]:["Rights-of-use relating to leases"],
                                                    data.columns[1]: col1_rights_of_use_relating_leases_data_list,
                                                    data.columns[2]: col2_rights_of_use_relating_leases_data_list
                                                    })
    """
    inventories Calculation
    """
    col1_inventories_data_list = []
    col2_inventories_data_list = []
    inventories_list = [x.lower() for x in inventories_list]
    for inventories in balance_col_values_total_current_assets:
        if inventories in inventories_list:
            print("inventories value is :", inventories)
            print("inventories value found")
            index_of_inventories = balance_col_values_total_current_assets.index(inventories)
            print("The index of {} is {} ".format(inventories,index_of_inventories))
            col1_inventories_data = data[data.columns[1]].values[index_of_inventories]
            col2_inventories_data = data[data.columns[2]].values[index_of_inventories]
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_inventories_data) in datatype_filtered_list:
                col1_inventories_data = col2_inventories_data
            if type(col2_inventories_data) in datatype_filtered_list:
                col2_inventories_data = col2_inventories_data
            else:            
                # Cleaning ($ and ,)
                if ("$") in col1_inventories_data:
                    col1_inventories_data = col1_inventories_data.replace("$", "")
                if ("$") in col2_inventories_data:
                    col2_inventories_data = col2_inventories_data.replace("$", "")
                if (",") in col1_inventories_data:
                    col1_inventories_data = col1_inventories_data.replace(",", "")
                if (",") in col2_inventories_data:
                    col2_inventories_data = col2_inventories_data.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_inventories_data:
                    col1_inventories_data = col1_inventories_data.replace("(", "")
                if (")") in col1_inventories_data:
                    col1_inventories_data = col1_inventories_data.replace(")", "")
                if ("(") in col2_inventories_data:
                    col2_inventories_data = col2_inventories_data.replace("(", "")
                if (")") in col2_inventories_data:
                    col2_inventories_data = col2_inventories_data.replace(")", "")
                if (" ") in col1_inventories_data:
                    col1_inventories_data = col1_inventories_data.replace(" ", "")
                if (" ") in col2_inventories_data:
                    col2_inventories_data = col2_inventories_data.replace(" ", "")
            col1_inventories_data_list.append(int(col1_inventories_data))
            col2_inventories_data_list.append(int(col2_inventories_data))
            print("Column 1 inventories Data after filteration is :",col1_inventories_data_list)
            print("Column 2 inventories Data after filteration is :",col2_inventories_data_list)
    
    if (len(col1_inventories_data_list) > 1):
        col1_inventories_data_list = sum(col1_inventories_data_list)
    if (len(col2_inventories_data_list) > 1):
        col2_inventories_data_list = sum(col2_inventories_data_list)
        
    if type(col1_inventories_data_list) == list:
        pass
    if type(col2_inventories_data_list) == list:
        pass
    if type(col1_inventories_data_list) in  datatype_filtered_list:
        col1_inventories_data_list = [col1_inventories_data_list]
    if type(col2_inventories_data_list) in  datatype_filtered_list:
        col2_inventories_data_list = [col2_inventories_data_list]
    
    try:
        inventories_df = pd.DataFrame({data.columns.values[0]:["Inventories"],
                                    data.columns[1]: [sum(col1_inventories_data_list)],
                                    data.columns[2]: [sum(col2_inventories_data_list)]
                                    })
    except Exception as e:
        pass
        inventories_df = pd.DataFrame({data.columns.values[0]:["Inventories"],
                                    data.columns[1]: col1_inventories_data_list,
                                    data.columns[2]: col1_inventories_data_list
                                    })
    """
    Accounts Data
    """
    col1_account_data_list = []
    col2_account_data_list = []
    accounts_list = [x.lower() for x in accounts_list]
    for account in balance_col_values_total_current_assets:
        if "accounts receivable" in account:
        # if account in accounts_list:
            print("Account value is :", account)
            print("account value found")
            index_of_account = balance_col_values_total_current_assets.index(account)
            print("The index of {} is {} ".format(account,index_of_account))
            col1_account_data = data[data.columns[1]].values[index_of_account]
            col2_account_data = data[data.columns[2]].values[index_of_account]
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_account_data) in datatype_filtered_list:
                col1_account_data = col1_account_data
            if type(col2_account_data) in datatype_filtered_list:
                col2_account_data = col2_account_data
            else:            
                # Cleaning ($ and ,)
                if ("$") in col1_account_data:
                    col1_account_data = col1_account_data.replace("$", "")
                if ("$") in col2_account_data:
                    col2_account_data = col2_account_data.replace("$", "")
                if (",") in col1_account_data:
                    col1_account_data = col1_account_data.replace(",", "")
                if (",") in col2_account_data:
                    col2_account_data = col2_account_data.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_account_data:
                    col1_account_data = col1_account_data.replace("(", "")
                if (")") in col1_account_data:
                    col1_account_data = col1_account_data.replace(")", "")
                if ("(") in col2_account_data:
                    col2_account_data = col2_account_data.replace("(", "")
                if (")") in col2_account_data:
                    col2_account_data = col2_account_data.replace(")", "")
                if (" ") in col1_account_data:
                    col1_account_data = col1_account_data.replace(" ", "")
                if (" ") in col2_account_data:
                    col2_account_data = col2_account_data.replace(" ", "")
            col1_account_data_list.append(int(col1_account_data))
            col2_account_data_list.append(int(col2_account_data))
            print("Column 1 account Data after filteration is :",col1_account_data_list)
            print("Column 2 account Data after filteration is :",col2_account_data_list)
        # Keyword based Filteration for Non-Trade-Recievable
        elif "non-trade receivable" not in account:
            if "trade receivable" in account:
            # if account in accounts_list:
                print("Account value is :", account)
                print("account value found")
                index_of_account = balance_col_values_total_current_assets.index(account)
                print("The index of {} is {} ".format(account,index_of_account))
                col1_account_data = data[data.columns[1]].values[index_of_account]
                col2_account_data = data[data.columns[2]].values[index_of_account]
                # Checking if the col1 and col2 datatypes are int, float or not
                if type(col1_account_data) in datatype_filtered_list:
                    col1_account_data = col1_account_data
                if type(col2_account_data) in datatype_filtered_list:
                    col2_account_data = col2_account_data
                else:            
                    # Cleaning ($ and ,)
                    if ("$") in col1_account_data:
                        col1_account_data = col1_account_data.replace("$", "")
                    if ("$") in col2_account_data:
                        col2_account_data = col2_account_data.replace("$", "")
                    if (",") in col1_account_data:
                        col1_account_data = col1_account_data.replace(",", "")
                    if (",") in col2_account_data:
                        col2_account_data = col2_account_data.replace(",", "")
                    # Updated Add On (Bracket Filteration )  
                    if ("(") in col1_account_data:
                        col1_account_data = col1_account_data.replace("(", "")
                    if (")") in col1_account_data:
                        col1_account_data = col1_account_data.replace(")", "")
                    if ("(") in col2_account_data:
                        col2_account_data = col2_account_data.replace("(", "")
                    if (")") in col2_account_data:
                        col2_account_data = col2_account_data.replace(")", "")
                    if (" ") in col1_account_data:
                        col1_account_data = col1_account_data.replace(" ", "")
                    if (" ") in col2_account_data:
                        col2_account_data = col2_account_data.replace(" ", "")
                col1_account_data_list.append(int(col1_account_data))
                col2_account_data_list.append(int(col2_account_data))
                print("Column 1 account Data after filteration is :",col1_account_data_list)
                print("Column 2 account Data after filteration is :",col2_account_data_list)
    
    if (len(col1_account_data_list) > 1):
        col1_account_data_list = sum(col1_account_data_list)
    if (len(col2_account_data_list) > 1):
        col2_account_data_list = sum(col2_account_data_list)
        
    if type(col1_account_data_list) == list:
        pass
    if type(col2_account_data_list) == list:
        pass
    if type(col1_account_data_list) in  datatype_filtered_list:
        col1_account_data_list = [col1_account_data_list]
    if type(col2_account_data_list) in  datatype_filtered_list:
        col2_account_data_list = [col2_account_data_list]
    
    try:
        account_df = pd.DataFrame({data.columns.values[0]:["Accounts/Trade Receivables"],
                                    data.columns[1]: [sum(col1_account_data_list)],
                                    data.columns[2]: [sum(col2_account_data_list)]
                                    })
    except Exception as e:
        pass
        account_df = pd.DataFrame({data.columns.values[0]:["Accounts/Trade Receivables"],
                                    data.columns[1]: col1_account_data_list,
                                    data.columns[2]: col2_account_data_list
                                    })
    
    """
    Other receivables Data
    """
    col1_other_receivables_data_list,col2_other_receivables_data_list = [],[]
    other_receivables_list = [x.lower() for x in other_receivables_list]
    print(other_receivables_list)
    for other_receivables in balance_col_values_total_current_assets:
        if other_receivables in other_receivables_list:
            print("other_receivables value is :", other_receivables)
            print("other_receivables value found")
            index_of_other_receivables = balance_col_values_total_current_assets.index(other_receivables)
            print("The index of {} is {} ".format(other_receivables,index_of_other_receivables))
            col1_other_receivables_data = data[data.columns[1]].values[index_of_other_receivables]
            col2_other_receivables_data = data[data.columns[2]].values[index_of_other_receivables]
            print("col1_other_receivables_data",col1_other_receivables_data)
            print("col2_other_receivables_data",col2_other_receivables_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_other_receivables_data) in datatype_filtered_list:
                col1_other_receivables_data = col2_other_receivables_data
            if type(col2_other_receivables_data) in datatype_filtered_list:
                col2_other_receivables_data = col2_other_receivables_data
            else:            
                # Cleaning ($ and ,)
                if ("$") in col1_other_receivables_data:
                    col1_other_receivables_data = col1_other_receivables_data.replace("$", "")
                if ("$") in col2_other_receivables_data:
                    col2_other_receivables_data = col2_other_receivables_data.replace("$", "")
                if (",") in col1_other_receivables_data:
                    col1_other_receivables_data = col1_other_receivables_data.replace(",", "")
                if (",") in col2_other_receivables_data:
                    col2_other_receivables_data = col2_other_receivables_data.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_other_receivables_data:
                    col1_other_receivables_data = col1_other_receivables_data.replace("(", "")
                if (")") in col1_other_receivables_data:
                    col1_other_receivables_data = col1_other_receivables_data.replace(")", "")
                if ("(") in col2_other_receivables_data:
                    col2_other_receivables_data = col2_other_receivables_data.replace("(", "")
                if (")") in col2_other_receivables_data:
                    col2_other_receivables_data = col2_other_receivables_data.replace(")", "")
                if (" ") in col1_other_receivables_data:
                    col1_other_receivables_data = col1_other_receivables_data.replace(" ", "")
                if (" ") in col2_other_receivables_data:
                    col2_other_receivables_data = col2_other_receivables_data.replace(" ", "")
            col1_other_receivables_data_list.append(float(col1_other_receivables_data))
            col2_other_receivables_data_list.append(float(col2_other_receivables_data))
            print("Column 1 aother_receivables Data after filteration is :",col1_other_receivables_data_list)
            print("Column 2 aother_receivables Data after filteration is :",col2_other_receivables_data_list)

    
    if (len(col1_other_receivables_data_list) > 1):
        col1_other_receivables_data_list = sum(col1_other_receivables_data_list)
    if (len(col2_other_receivables_data_list) > 1):
        col2_other_receivables_data_list = sum(col2_other_receivables_data_list)
        
    if type(col1_other_receivables_data_list) == list:
        pass
    if type(col2_other_receivables_data_list) == list:
        pass
    if type(col1_other_receivables_data_list) in  datatype_filtered_list:
        col1_other_receivables_data_list = [col1_other_receivables_data_list]
    if type(col2_other_receivables_data_list) in  datatype_filtered_list:
        col2_other_receivables_data_list = [col2_other_receivables_data_list]
    try:
        other_receivables_df = pd.DataFrame({data.columns.values[0]:["Other Receivables"],
                                    data.columns[1]: [sum(col1_other_receivables_data_list)],
                                    data.columns[2]: [sum(col2_other_receivables_data_list)]
                                    })
    except Exception as e:
        pass
        print("Exception occured : ", e)
        other_receivables_df = pd.DataFrame({data.columns.values[0]:["Other Receivables"],
                                        data.columns[1]: col1_other_receivables_data_list,
                                        data.columns[2]: col2_other_receivables_data_list
                                        })
    print("other_receivables_df",other_receivables_df)
    print("account_df[data.columns[1]]",account_df[data.columns[1]][0])
    print("other_receivables_df[data.columns[1]]",other_receivables_df[data.columns[1]][0])
    print("account_df[data.columns[2]]",account_df[data.columns[2]][0])
    print("other_receivables_df[data.columns[1]]",other_receivables_df[data.columns[2]][0])
    col1_total_accounts_and_trades_data = int(account_df[data.columns[1]][0]) + int(other_receivables_df[data.columns[1]][0])
    col2_total_accounts_and_trades_data = int(account_df[data.columns[2]][0]) + int(other_receivables_df[data.columns[2]][0])
    print("col1_total_accounts_and_trades_data :", col1_total_accounts_and_trades_data)
    print("col2_total_accounts_and_trades_data :", col2_total_accounts_and_trades_data)
    try:
        total_accounts_df = pd.DataFrame({data.columns.values[0]:["Accounts/Trade Receivables(Total)"],
                                    data.columns[1]: [sum(col1_total_accounts_and_trades_data)],
                                    data.columns[2]: [sum(col2_total_accounts_and_trades_data)]
                                    })
    except Exception as e:
        total_accounts_df = pd.DataFrame({data.columns.values[0]:["Accounts/Trade Receivables(Total)"],
                            data.columns[1]: col1_total_accounts_and_trades_data,
                            data.columns[2]: col2_total_accounts_and_trades_data
                            })
    

    """
    Tax Data
    """
    col1_tax_data_list = []
    col2_tax_data_list = []
    for tax in balance_col_values_total_current_assets:
        print(tax_list)
        if tax in tax_list:
            print("Tax value found")
            index_of_tax = balance_col_values_total_current_assets.index(tax)
            print("The index of {} is {} ".format(tax,index_of_tax))
            col1_tax_data = data[data.columns[1]].values[index_of_tax]
            print("Column 1 tax Data is :",col1_tax_data)
            col2_tax_data = data[data.columns[2]].values[index_of_tax]
            print("Column 1 tax Data is :",col2_tax_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_tax_data) in datatype_filtered_list:
                col1_tax_data = col2_tax_data
            if type(col2_tax_data) in datatype_filtered_list:
                col2_tax_data = col2_tax_data
            else:            
                # Cleaning ($ and ,)
                if ("$") in col1_tax_data:
                    col1_tax_data = col1_tax_data.replace("$", "")
                if ("$") in col2_tax_data:
                    col2_tax_data = col2_tax_data.replace("$", "")
                if (",") in col1_tax_data:
                    col1_tax_data = col1_tax_data.replace(",", "")
                if (",") in col2_tax_data:
                    col2_tax_data = col2_tax_data.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_tax_data:
                    col1_tax_data = col1_tax_data.replace("(", "")
                if (")") in col1_tax_data:
                    col1_tax_data = col1_tax_data.replace(")", "")
                if ("(") in col2_tax_data:
                    col2_tax_data = col2_tax_data.replace("(", "")
                if (")") in col2_tax_data:
                    col2_tax_data = col2_tax_data.replace(")", "")
                if (" ") in col1_tax_data:
                    col1_tax_data = col1_tax_data.replace(" ", "")
                if (" ") in col2_tax_data:
                    col2_tax_data = col2_tax_data.replace(" ", "")
            col1_tax_data_list.append(float(col1_tax_data))
            col2_tax_data_list.append(float(col2_tax_data))        
            print("Column 1 tax Data after filteration is :",col1_tax_data_list)
            print("Column 2 tax Data after filteration is :",col2_tax_data_list)

    if (len(col1_tax_data_list) > 1):
        col1_tax_data_list= sum(col1_tax_data_list)
    if (len(col1_tax_data_list) > 1):
        col2_tax_data_list = sum(col2_tax_data_list)
        
    if type(col1_tax_data_list) == list:
        pass
    if type(col2_tax_data_list) == list:
        pass
    if type(col1_tax_data_list) in  datatype_filtered_list:
        col1_tax_data_list = [col1_tax_data_list]
    if type(col2_tax_data_list) in  datatype_filtered_list:
        col2_tax_data_list = [col2_tax_data_list]
    # # balance_col_values_total_current_assets
    try:
        tax_df = pd.DataFrame({data.columns.values[0]:["Tax"],
                data.columns[1]: [sum(col1_tax_data_list)],
                data.columns[2]:[sum(col2_tax_data_list)]
                })
    except Exception as e:
        pass
        tax_df = pd.DataFrame({data.columns.values[0]:["Tax"],
                    data.columns[1]: col1_tax_data_list,
                    data.columns[2]:col2_tax_data_list
                    })
    
    """
    prepaid_expenses data
    """
    prepaid_expenses_list = database_data["Prepaid expenses and other current assets"].values
    col1_prepaid_expenses_data = 0
    col2_prepaid_expenses_data = 0
    col1_prepaid_expenses_data_list = []
    col2_prepaid_expenses_data_list = []
    for expense in balance_col_values_total_current_assets:
        if expense in prepaid_expenses_list:
            print("Prepaid expense value found")
            index_of_expense = balance_col_values_total_current_assets.index(expense)
            print("The index of {} is {} ".format(expense, index_of_expense))
            col1_prepaid_expenses_data = data[data.columns[1]].values[index_of_expense]
            print("Column 1 prepaid expenses data is:", col1_prepaid_expenses_data)
            col2_prepaid_expenses_data = data[data.columns[2]].values[index_of_expense]
            print("Column 2 prepaid expenses data is:", col2_prepaid_expenses_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_prepaid_expenses_data) in datatype_filtered_list:
                col1_prepaid_expenses_data = col1_prepaid_expenses_data
            if type(col2_prepaid_expenses_data) in datatype_filtered_list:
                col2_prepaid_expenses_data = col2_prepaid_expenses_data
            else:            
                # Cleaning ($ and ,)
                if "$" in col1_prepaid_expenses_data:
                    col1_prepaid_expenses_data = col1_prepaid_expenses_data.replace("$", "")
                if "$" in col2_prepaid_expenses_data:
                    col2_prepaid_expenses_data = col2_prepaid_expenses_data.replace("$", "")
                if "," in col1_prepaid_expenses_data:
                    col1_prepaid_expenses_data = col1_prepaid_expenses_data.replace(",", "")
                if "," in col2_prepaid_expenses_data:
                    col2_prepaid_expenses_data = col2_prepaid_expenses_data.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_prepaid_expenses_data:
                    col1_prepaid_expenses_data = col1_prepaid_expenses_data.replace("(", "")
                if (")") in col1_prepaid_expenses_data:
                    col1_prepaid_expenses_data = col1_prepaid_expenses_data.replace(")", "")
                if ("(") in col2_prepaid_expenses_data:
                    col2_prepaid_expenses_data = col2_prepaid_expenses_data.replace("(", "")
                if (")") in col2_prepaid_expenses_data:
                    col2_prepaid_expenses_data = col2_prepaid_expenses_data.replace(")", "")
                if (" ") in col1_prepaid_expenses_data:
                    col1_prepaid_expenses_data = col1_prepaid_expenses_data.replace(" ", "")
                if (" ") in col2_prepaid_expenses_data:
                    col2_prepaid_expenses_data = col2_prepaid_expenses_data.replace(" ", "")
            col1_prepaid_expenses_data_list.append(float(col1_prepaid_expenses_data))
            col2_prepaid_expenses_data_list.append(float(col2_prepaid_expenses_data))
            print("Column 1 prepaid expenses data after filtration is:", col1_prepaid_expenses_data_list)
            print("Column 2 prepaid expenses data after filtration is:", col2_prepaid_expenses_data_list)

    
    if (len(col1_prepaid_expenses_data_list) > 1):
        col1_prepaid_expenses_data_list= sum(col1_prepaid_expenses_data_list)
    if (len(col1_prepaid_expenses_data_list) > 1):
        col2_prepaid_expenses_data_list = sum(col2_prepaid_expenses_data_list)
        
    if type(col1_prepaid_expenses_data_list) == list:
        pass
    if type(col2_prepaid_expenses_data_list) == list:
        pass
    if type(col1_prepaid_expenses_data_list) in  datatype_filtered_list:
        col1_prepaid_expenses_data_list = [col1_prepaid_expenses_data_list]
    if type(col2_prepaid_expenses_data_list) in  datatype_filtered_list:
        col2_prepaid_expenses_data_list = [col2_prepaid_expenses_data_list]
    # prepaid_expenses_col_values
    try:
        prepaid_expenses_df = pd.DataFrame({
                                            data.columns.values[0]: ["Prepaid expenses and other current assets"],
                                            data.columns[1]: [sum(col1_prepaid_expenses_data_list)],
                                            data.columns[2]: [sum(col2_prepaid_expenses_data_list)]
                                        })
    except Exception as e:
        pass
        prepaid_expenses_df = pd.DataFrame({
                                            data.columns.values[0]: ["Prepaid expenses and other current assets"],
                                            data.columns[1]: col1_prepaid_expenses_data_list,
                                            data.columns[2]: col2_prepaid_expenses_data_list
                                        })
    """
    other_current_assets
    """
    col1_other_current_assets_list = []
    col2_other_current_assets_list = []
    other_current_assets_list = database_data["Other Current Assets"].values
    other_current_assets_list = [x.lower() for x in other_current_assets_list]
    col1_other_current_assets_data = 0
    col2_other_current_assets_data = 0
    for other_current_assets in balance_col_values_total_current_assets:
        if other_current_assets in other_current_assets_list:
            print("other_current_assets value found")
            index_of_other_current_assets = balance_col_values_total_current_assets.index(other_current_assets)
            print("The index of {} is {} ".format(other_current_assets, index_of_other_current_assets))
            col1_other_current_assets_data = data[data.columns[1]].values[index_of_other_current_assets]
            print("Column 1 other_current_assets data is:", col1_other_current_assets_data)
            col2_other_current_assets_data = data[data.columns[2]].values[index_of_other_current_assets]
            print("Column 2 other_current_assets data is:", col2_other_current_assets_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            print("Starting Debug after Column 2 other_current_assets......... 15082023 log")
            
            if type(col1_other_current_assets_data) in datatype_filtered_list:
                col1_other_current_assets_data = col1_other_current_assets_data
            if type(col2_other_current_assets_data) in datatype_filtered_list:
                col2_other_current_assets_data = col2_other_current_assets_data
            else:            
                # Cleaning ($ and ,)
                if "$" in col1_other_current_assets_data:
                    col1_other_current_assets_data = col1_other_current_assets_data.replace("$", "")
                if "$" in col2_other_current_assets_data:
                    col2_other_current_assets_data = col2_other_current_assets_data.replace("$", "")
                if "," in col1_other_current_assets_data:
                    col1_other_current_assets_data = col1_other_current_assets_data.replace(",", "")
                if "," in col2_other_current_assets_data:
                    col2_other_current_assets_data = col2_other_current_assets_data.replace(",", "")
                if (" ") in col1_other_current_assets_data:
                    col1_other_current_assets_data = col1_other_current_assets_data.replace(" ", "")
                if (" ") in col2_other_current_assets_data:
                    col2_other_current_assets_data = col2_other_current_assets_data.replace(" ", "")
                # # Updated Add On (Bracket Filteration )  
                # if ("(") in col1_other_current_assets_data:
                #     col1_other_current_assets_data = col1_other_current_assets_data.replace("(", "")
                # if (")") in col1_other_current_assets_data:
                #     col1_other_current_assets_data = col1_other_current_assets_data.replace(")", "")
                # if ("(") in col2_other_current_assets_data:
                #     col2_other_current_assets_data = col2_other_current_assets_data.replace("(", "")
                # if (")") in col2_other_current_assets_data:
                #     col2_other_current_assets_data = col2_other_current_assets_data.replace(")", "")
            col1_other_current_assets_list.append(float(col1_other_current_assets_data))
            col2_other_current_assets_list.append(float(col2_other_current_assets_data))
            print("Column 1 other_current_assets data after filtration is:", col1_other_current_assets_list)
            print("Column 2 other_current_assets data after filtration is:", col2_other_current_assets_list)

    if (len(col1_other_current_assets_list) > 1):
        col1_other_current_assets_list= sum(col1_other_current_assets_list)
        print("col1_other_current_assets_list after summtion is ", col1_other_current_assets_list)
    if (len(col2_other_current_assets_list) > 1):
        col2_other_current_assets_list = sum(col2_other_current_assets_list)
        print("col2_other_current_assets_list after summtion is ", col2_other_current_assets_list)
        
    if type(col1_other_current_assets_list) == list:
        pass
    if type(col2_other_current_assets_list) == list:
        pass
    if type(col1_other_current_assets_list) in  datatype_filtered_list:
        col1_other_current_assets_list = [col1_other_current_assets_list]
    if type(col2_other_current_assets_list) in  datatype_filtered_list:
        col2_other_current_assets_list = [col2_other_current_assets_list]
    # prepaid_expenses_col_values
    try:
        print("Trying for other_current_assets_df approach 1..........")
        print("Here col1_other_current_assets_list is :", col1_other_current_assets_list)
        print("Here col2_other_current_assets_list is :", col2_other_current_assets_list)
        other_current_assets_df = pd.DataFrame({
                                            data.columns.values[0]: ["Other Current Assets"],
                                            data.columns[1]: [sum(col1_other_current_assets_list)],
                                            data.columns[2]: [sum(col1_other_current_assets_list)]
                                        })
    except Exception as e:
        print("Trying for other_current_assets_df approach 2..........")
        print("Here col1_other_current_assets_list is :", col1_other_current_assets_list)
        print("Here col2_other_current_assets_list is :", col2_other_current_assets_list)
        other_current_assets_df = pd.DataFrame({
                                            data.columns.values[0]: ["Other Current Assets"],
                                            data.columns[1]: col1_other_current_assets_list,
                                            data.columns[2]: col2_other_current_assets_list
                                        })
    
    print("other_current_assets_df",other_current_assets_df)
    print("Debug Successful for Column 2 other_current_assets......... 15082023 log")
    
    
    
    
    
    current_assets_df = pd.concat([cash_and_market_df,
                                    rights_of_use_relating_leases_df,
                                    total_accounts_df,
                                    tax_df,
                                    inventories_df,
                                    prepaid_expenses_df,
                                    other_current_assets_df
                                    ], axis =0)
    current_assets_df = current_assets_df.reset_index()
    print(current_assets_df[current_assets_df.columns[2]].tolist())
    current_assets_col1_values = 1
    current_assets_col2_values = 0
    current_assets_col1_values_to_int = []
    current_assets_col2_values_to_int = []
    current_assets_col1_values = current_assets_df[current_assets_df.columns[2]].tolist()
    print("current_assets_col1_values",current_assets_col1_values)
    print("current_assets_col2_values",current_assets_col2_values)
    current_assets_col2_values = current_assets_df[current_assets_df.columns[3]].tolist()
    for i in current_assets_col1_values:
        current_assets_col1_values_to_int.append(float(i))
    
    for j in current_assets_col2_values:
        current_assets_col2_values_to_int.append(float(j))
    print("current_assets_col1_values_to_int",current_assets_col1_values_to_int)
    print("current_assets_col2_values_to_int",current_assets_col2_values_to_int)
    print("data.columns[0]:",data.columns[0])
    print("data.columns[1]:",data.columns[1])
    print("data.columns[2]:",data.columns[2])
    print("sum(current_assets_col1_values_to_int)",sum(current_assets_col1_values_to_int))
    print("sum(current_assets_col2_values_to_int)",sum(current_assets_col2_values_to_int))
    total_current_assets_df_calculated = pd.DataFrame({data.columns.values[0]: "Total Current Assets (Calculated)",
                                                       data.columns[1]: [sum(current_assets_col1_values_to_int)],
                                                       data.columns[2]: [sum(current_assets_col2_values_to_int)]
                                                       })
    print(total_current_assets_df_calculated)
    total_current_assets_df_calculated =total_current_assets_df_calculated.reset_index()
    
    col1_total_current_assets_list ,col2_total_current_assets_list = [], []
    total_current_assets_list = database_data["Total current assets"].values
    print("Total Current assets list is :", total_current_assets_list)
    col1_total_current_assets_data = 0
    col2_total_current_assets_data = 0
    total_current_assets_list = [x.lower() for x in total_current_assets_list]
    for asset in balance_col_values_total_current_assets:
        print("asset",asset)
        if asset in total_current_assets_list:
            
            print("Total current asset value found")
            index_of_asset = balance_col_values_total_current_assets.index(asset)
            print("The index of {} is {} ".format(asset, index_of_asset))
            col1_total_current_assets_data = data[data.columns[1]].values[index_of_asset]
            print("Column 1 total current assets data is:", col1_total_current_assets_data)
            col2_total_current_assets_data = data[data.columns[2]].values[index_of_asset]
            print("Column 2 total current assets data is:", col2_total_current_assets_data)
            if type(col1_total_current_assets_data) in datatype_filtered_list:
                col1_total_current_assets_data = col1_total_current_assets_data
            if type(col2_total_current_assets_data) in datatype_filtered_list:
                col2_total_current_assets_data = col2_total_current_assets_data
            else:            
                # Cleaning ($ and ,)
                if "$" in col1_total_current_assets_data:
                    col1_total_current_assets_data = col1_total_current_assets_data.replace("$", "")
                if "$" in col2_total_current_assets_data:
                    col2_total_current_assets_data = col2_total_current_assets_data.replace("$", "")
                if "," in col1_total_current_assets_data:
                    col1_total_current_assets_data = col1_total_current_assets_data.replace(",", "")
                if "," in col2_total_current_assets_data:
                    col2_total_current_assets_data = col2_total_current_assets_data.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_total_current_assets_data:
                    col1_total_current_assets_data = col1_total_current_assets_data.replace("(", "")
                if (")") in col1_total_current_assets_data:
                    col1_total_current_assets_data = col1_total_current_assets_data.replace(")", "")
                if ("(") in col2_total_current_assets_data:
                    col2_total_current_assets_data = col2_total_current_assets_data.replace("(", "")
                if (")") in col2_total_current_assets_data:
                    col2_total_current_assets_data = col2_total_current_assets_data.replace(")", "")
                if (" ") in col1_total_current_assets_data:
                    col1_total_current_assets_data = col1_total_current_assets_data.replace(" ", "")
                if (" ") in col2_total_current_assets_data:
                    col2_total_current_assets_data = col2_total_current_assets_data.replace(" ", "")
            
            col1_total_current_assets_list.append(float(col1_total_current_assets_data))
            col2_total_current_assets_list.append(float(col2_total_current_assets_data))
            print("Column 1 total current assets data after filtration is:", col1_total_current_assets_list)
            print("Column 2 total current assets data after filtration is:", col2_total_current_assets_list)

    if (len(col1_total_current_assets_list) > 1):
        col1_total_current_assets_list= sum(col1_total_current_assets_list)
    if (len(col2_total_current_assets_list) > 1):
        col2_total_current_assets_list = sum(col2_total_current_assets_list)
        
    if type(col1_total_current_assets_list) == list:
        pass
    if type(col2_total_current_assets_list) == list:
        pass
    if type(col1_total_current_assets_list) in  datatype_filtered_list:
        col1_total_current_assets_list = [col1_total_current_assets_list]
    if type(col2_total_current_assets_list) in  datatype_filtered_list:
        col2_total_current_assets_list = [col2_total_current_assets_list]
    
    
    
    # total_current_assets_col_values
    try:
        total_current_assets_df = pd.DataFrame({
                                                data.columns.values[0]: ["Total current assets"],
                                                data.columns[1]: [sum(col1_total_current_assets_list)],
                                                data.columns[2]: [sum(col2_total_current_assets_list)]
                                            })
    except Exception as e:
        total_current_assets_df = pd.DataFrame({
                                                data.columns.values[0]: ["Total current assets"],
                                                data.columns[1]: col1_total_current_assets_list,
                                                data.columns[2]: col2_total_current_assets_list
                                            })
    
    """
    int(col1_total_current_assets_data) is the actual data fetched from total current assets in first column. For inconsistancy of data the value has been type casted to integer.
    int(col2_total_current_assets_data) is the actual data fetched from total current assets in second column. For inconsistancy of data the value has been type casted to integer.
    sum(current_assets_col1_values_to_int) is the total sum of current asssets that has been calculated in column 1.
    sum(current_assets_col2_values_to_int) is the total sum of current asssets that has been calculated in column 2.
    """
    print(type(col1_total_current_assets_data))
    print(type(col2_total_current_assets_data))
    try:
        error_col1_total_current_assets_data = col1_total_current_assets_list - sum(current_assets_col1_values_to_int)
        error_col2_total_current_assets_data = col2_total_current_assets_list - sum(current_assets_col2_values_to_int)
        print(error_col1_total_current_assets_data,error_col2_total_current_assets_data)
    except Exception as e:
        pass
        error_col1_total_current_assets_data = sum(col1_total_current_assets_list) - sum(current_assets_col1_values_to_int)
        error_col2_total_current_assets_data = sum(col2_total_current_assets_list) - sum(current_assets_col2_values_to_int)
        print(error_col1_total_current_assets_data,error_col2_total_current_assets_data)
    
    # total_current_assets_col_values
    total_current_assets_error_df = pd.DataFrame({
                                                    data.columns.values[0]: ["Total current assets(Error)"],
                                                    data.columns[1]: [error_col1_total_current_assets_data],
                                                    data.columns[2]: [error_col2_total_current_assets_data]
                                                })
    

    """---------------------------Total Current Assets Done till here--------------------------------------"""
    
    final_data = pd.concat([cash_and_market_df,
                            rights_of_use_relating_leases_df,
                            inventories_df,
                            account_df,
                            total_accounts_df,
                            tax_df,
                            prepaid_expenses_df,
                            other_current_assets_df,
                            total_current_assets_df,
                            total_current_assets_df_calculated,
                            total_current_assets_error_df
                            ], axis =0)
    
    print(final_data)
    
    return final_data



def get_stabnderdized_total_long_term_assets_data(database_data,data):
    """
    Arguments : database_data and data
    - database_data : Here the database data is the standerdized gsheet data
    - data: Here data refers to the downloaded balance sheet data where we will perform the overall standerdization operation
    """
    
    # Local variable declaration
    col1_rights_data = 0                                        # Right-of-use assets for operating leases
    col2_rights_data = 0                                        # Right-of-use assets for operating leases
    col1_net_property_data = 0                                  # Net property and equipment
    col2_net_property_data = 0                                  # Net property and equipment
    col1_real_estate_assets = 0                                 # Real Estate Assets
    col2_real_estate_assets = 0                                 # Real Estate Assets
    col1_investments_and_other_assets = 0                       # Investments and other assets
    col2_investments_and_other_assets = 0                       # Investments and other assets    
    col1_investments_and_other_companies = 0                    # Investment in Other Companies
    col2_investments_and_other_companies = 0                    # Investment in Other Companies
    col1_pensions_assets = 0                                    # Pensions Assets	
    col2_pensions_assets = 0                                    # Pensions Assets
    col1_goodwill = 0                                           # Goodwill
    col2_goodwill = 0                                           # Goodwill
    col1_indefinite_lived_and_amortizable_intangible_assets = 0 # Indefinite-lived and amortizable intangible assets
    col2_indefinite_lived_and_amortizable_intangible_assets = 0 # Indefinite-lived and amortizable intangible assets
    col1_deferred_income_taxes = 0                              # Deferred income taxes
    col2_deferred_income_taxes = 0                              # Deferred income taxes
    col1_total_intangible_and_other_assets = 0                  # Total intangible and other assets
    col2_total_intangible_and_other_assets = 0                  # Total intangible and other assets
    col1_assets_for_discontinued_business = 0                   # Assets for Discontinued Business
    col2_assets_for_discontinued_business = 0                   # Assets for Discontinued Business
    col1_total_long_term_assets = 0                             # Total_long_term_assets
    col2_total_long_term_assets = 0                             # Total_long_term_assets
    datatype_filtered_list = [int, float]
    
    # Preprocessing
    if 'Unnamed: 0' in data.columns:
        data.drop(['Unnamed: 0'], axis =1 , inplace = True)
    print(data.isnull().sum())
    print(database_data.isnull().sum())
    data.fillna(0, inplace= True)
    for col in database_data.columns:
        if database_data[col].dtypes == "object":
            database_data[col].fillna(col, inplace= True)

    """
    - After preprocessing we are starting the standerdization as per the given SOP
    - We are using exact search mechamnism to get and standerdize our data.
    - In this function we will standerdize for all the total current assets.
    """
    # Decalation of all Current assets line items.
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values = [x.lower() for x in balance_col_values]
    rights_data_list = database_data["Right-of-use assets for operating leases"].values
    net_property_data_list = database_data["Net property and equipment"].values
    real_estate_assets_list = database_data["Real Estate Assets"].values
    investments_and_other_assets_list = database_data["Investments and other assets"].values
    investments_and_other_companies_list = database_data["Investment in Other Companies"].values
    pensions_assets_list = database_data["Pensions Assets"].values
    goodwill_list = database_data["Goodwill"].values
    indefinite_lived_and_amortizable_intangible_assets_list = database_data["Indefinite-lived and amortizable intangible assets"].values
    deferred_income_taxes_list = database_data["Deferred income taxes"].values
    total_intangible_and_other_assets_list = database_data["Total intangible and other assets"].values
    assets_for_discontinued_business_list = database_data["Assets for Discontinued Business"].values
    total_long_term_assets_list = database_data["Total Long Term assets"].values
    
    balance_col_values_total_long_term_assets  = [x.lower() for x in balance_col_values]
    
    
    
    # Searching for all possible combinations of total current assets from the database.
    for total_current_asset,total_asset in zip(database_data["Total current assets"],database_data["Total assets"]):
        # Tuning it to lowercase for fextact filteration.
        print("total_current_asset lower is :",total_current_asset.lower())
        print("total_asset lower is :",total_asset.lower())
        total_current_asset_lower = total_current_asset.lower()
        total_asset_lower = total_asset.lower()
        print("balance_col_values_total_long_term_assets:", balance_col_values_total_long_term_assets)
        if total_current_asset_lower in balance_col_values_total_long_term_assets:
            print("Just a Try.........")
        
         
        if total_current_asset_lower in balance_col_values_total_long_term_assets:
            print("Getting total_current_idx...............")
            total_current_idx = balance_col_values_total_long_term_assets.index(total_current_asset_lower)
            print("Executing this block...............")
            print("total_current_idx: ",total_current_idx)
        if total_asset_lower in balance_col_values_total_long_term_assets:
            print("Getting total_asset_idx...............")
            total_asset_idx = balance_col_values_total_long_term_assets.index(total_asset_lower)
            print("total_asset_idx: ",total_asset_idx)
    
    # These are the baalnce col values for the current assets....
    balance_col_values_total_long_term_assets = balance_col_values_total_long_term_assets[total_current_idx+1:total_asset_idx]
    print(balance_col_values_total_long_term_assets)
    
    
    
    """
    Rights-of-use relating to leases
    """
    balance_col_values = data[data.columns[0]].tolist()
    balance_col_values = [x.lower() for x in balance_col_values]
    rights_data_list = [x.lower()  for x in rights_data_list]
    balance_col_values_total_long_term_assets = [x.lower()  for x in balance_col_values_total_long_term_assets]
    rights_data_list_column1, rights_data_list_column2 = [], []
    for rights_data in balance_col_values_total_long_term_assets:
        if rights_data in rights_data_list:
            print("Marketable value found")
            index_of_rights_data = balance_col_values.index(rights_data)
            col1_rights_data = data[data.columns[1]].values[index_of_rights_data]
            print("Column 1 rights_data is :",col1_rights_data)
            col2_rights_data = data[data.columns[2]].values[index_of_rights_data]
            print("Column 2 rights_data is :",col2_rights_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_rights_data) in datatype_filtered_list:
                col1_rights_data = col1_rights_data
            if type(col2_rights_data) in datatype_filtered_list:
                col2_rights_data = col2_rights_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_rights_data:
                    col1_rights_data = col1_rights_data.replace("$", "")
                if ("$") in col2_rights_data:
                    col2_rights_data = col2_rights_data.replace("$", "")
                if (",") in col1_rights_data:
                    col1_rights_data = col1_rights_data.replace(",", "")
                if (",") in col2_rights_data:
                    col2_rights_data = col2_rights_data.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_rights_data:
                    col1_rights_data = col1_rights_data.replace("(", "")
                if (")") in col1_rights_data:
                    col1_rights_data = col1_rights_data.replace(")", "")
                if ("(") in col2_rights_data:
                    col2_rights_data = col2_rights_data.replace("(", "")
                if (")") in col2_rights_data:
                    col2_rights_data = col2_rights_data.replace(")", "")
                if (" ") in col1_rights_data:
                    col1_rights_data = col1_rights_data.replace(" ", "")
                if (" ") in col2_rights_data:
                    col2_rights_data = col2_rights_data.replace(" ", "")
            print("Column 1 rights_data after filteration is :",col1_rights_data)
            print("Column 2 rights_data after filteration is :",col2_rights_data)
            rights_data_list_column1.append(float(col1_rights_data))
            rights_data_list_column2.append(float(col2_rights_data))
        else:
            rights_data_list_column1.append(0)
            rights_data_list_column2.append(0)

    if (len(rights_data_list_column1) > 1):
        rights_data_list_column1 = sum(rights_data_list_column1)
    if (len(rights_data_list_column2) > 1):
        rights_data_list_column2 = sum(rights_data_list_column2)
        
    if type(rights_data_list_column1) == list:
        pass
    if type(rights_data_list_column2) == list:
        pass
    if type(rights_data_list_column1) in  datatype_filtered_list:
        rights_data_list_column1 = [rights_data_list_column1]
    if type(rights_data_list_column2) in  datatype_filtered_list:
        rights_data_list_column2 = [rights_data_list_column2]
    
    print("rights_data_list_column1",rights_data_list_column1)
    print("rights_data_list_column2",rights_data_list_column2)
    
    # rights_data_df
    rights_data_df = pd.DataFrame({data.columns.values[0]:["Rights-of-use relating to leases"],
                                    data.columns[1]: rights_data_list_column1,
                                    data.columns[2]: rights_data_list_column2
                                    })
        
    print(rights_data_df)
    
    
    
    """
    Net property and equipment
    """
    net_property_data_list = [x.lower()  for x in net_property_data_list]
    net_property_data_list_column1, net_property_data_list_column2 = [], []
    balance_col_values_total_long_term_assets = [x.lower()  for x in balance_col_values_total_long_term_assets]
    
    try:
        for net_property_data in balance_col_values_total_long_term_assets:
            if "equipment" in net_property_data:
                print("net_property_data value found")
                index_of_net_property_data = balance_col_values.index(net_property_data)
                col1_net_property_data = data[data.columns[1]].values[index_of_net_property_data]
                print("Column 1 net_property_data is :",col1_net_property_data)
                col2_net_property_data = data[data.columns[2]].values[index_of_net_property_data]
                print("Column 2 net_property_data is :",col2_net_property_data)
                # Checking if the col1 and col2 datatypes are int, float or not
                if type(col1_net_property_data) in datatype_filtered_list:
                    col1_net_property_data = col1_net_property_data
                if type(col2_net_property_data) in datatype_filtered_list:
                    col2_net_property_data = col2_net_property_data
                else:
                    # Cleaning ($ and ,)
                    if ("$") in col1_net_property_data:
                        col1_net_property_data = col1_net_property_data.replace("$", "")
                    if ("$") in col2_net_property_data:
                        col2_net_property_data = col2_net_property_data.replace("$", "")
                    if (",") in col1_net_property_data:
                        col1_net_property_data = col1_net_property_data.replace(",", "")
                    if (",") in col2_net_property_data:
                        col2_net_property_data = col2_net_property_data.replace(",", "")
                    # Updated Add On (Bracket Filteration )  
                    if ("(") in col1_net_property_data:
                        col1_net_property_data = col1_net_property_data.replace("(", "")
                    if (")") in col1_net_property_data:
                        col1_net_property_data = col1_net_property_data.replace(")", "")
                    if ("(") in col2_net_property_data:
                        col2_net_property_data = col2_net_property_data.replace("(", "")
                    if (")") in col2_net_property_data:
                        col2_net_property_data = col2_net_property_data.replace(")", "")
                    if (" ") in col1_net_property_data:
                        col1_net_property_data = col1_net_property_data.replace(" ", "")
                    if (" ") in col2_net_property_data:
                        col2_net_property_data = col2_net_property_data.replace(" ", "")
                print("Column 1 net_property_data after filteration is :",col1_net_property_data)
                print("Column 2 net_property_data after filteration is :",col2_net_property_data)
                net_property_data_list_column1.append(float(col1_net_property_data))
                net_property_data_list_column2.append(float(col2_net_property_data))
            else:
                net_property_data_list_column1.append(0)
                net_property_data_list_column2.append(0)
                
        if (len(net_property_data_list_column1) > 1):
            net_property_data_list_column1 = sum(net_property_data_list_column1)
        if (len(net_property_data_list_column2) > 1):
            net_property_data_list_column2 = sum(net_property_data_list_column2)
            
        if type(net_property_data_list_column1) == list:
            pass
        if type(net_property_data_list_column2) == list:
            pass
        if type(net_property_data_list_column1) in  datatype_filtered_list:
            net_property_data_list_column1 = [net_property_data_list_column1]
        if type(net_property_data_list_column2) in  datatype_filtered_list:
            net_property_data_list_column2 = [net_property_data_list_column2]

        
        try:
            net_property_data_df = pd.DataFrame(
                                                    {
                                                        data.columns.values[0]:["Plant, Property, and Equipment"],
                                                        data.columns[1]: net_property_data_list_column1,
                                                        data.columns[2]: net_property_data_list_column2
                                                    }
                                                )
        except Exception as e:
            pass
            print("net_property_data_df ran into errror.......", e)
            net_property_data_df = pd.DataFrame(
                                                    {
                                                        data.columns.values[0]: ["Income taxes payable"],
                                                        data.columns[1]: [sum(net_property_data_list_column1)],
                                                        data.columns[2]: [sum(net_property_data_list_column2)]
                                                    }
                                                )
    except Exception as e:
        pass
        print("Net property Calculation ran into an Exception......", e)

    
    # print(net_property_data_df)
    
    
    """
    Real Estate Assets
    """
    print("Starting Real Estate Calculation.......")
    try:
        real_estate_assets_list = [x.lower() for x in real_estate_assets_list]
        real_estate_assets_list_column1, real_estate_assets_list_column2 = [], []
        balance_col_values_total_long_term_assets = [x.lower() for x in balance_col_values_total_long_term_assets]
        for real_estate_assets in balance_col_values_total_long_term_assets:
            if real_estate_assets in real_estate_assets_list:
                print("real_estate_assets value found")
                index_of_real_estate_assets = balance_col_values.index(real_estate_assets)
                col1_real_estate_assets = data[data.columns[1]].values[index_of_real_estate_assets]
                print("Column 1 real_estate_assets is:", col1_real_estate_assets)
                col2_real_estate_assets = data[data.columns[2]].values[index_of_real_estate_assets]
                print("Column 2 real_estate_assets is:", col2_real_estate_assets)
                # Checking if the col1 and col2 datatypes are int, float or not
                if type(col1_real_estate_assets) in datatype_filtered_list:
                    col1_real_estate_assets = col1_net_property_data
                if type(col2_real_estate_assets) in datatype_filtered_list:
                    col2_real_estate_assets = col2_real_estate_assets
                else:
                    # Cleaning ($ and ,)
                    if "$" in col1_real_estate_assets:
                        col1_real_estate_assets = col1_real_estate_assets.replace("$", "")
                    if "$" in col2_real_estate_assets:
                        col2_real_estate_assets = col2_real_estate_assets.replace("$", "")
                    if "," in col1_real_estate_assets:
                        col1_real_estate_assets = col1_real_estate_assets.replace(",", "")
                    if "," in col2_real_estate_assets:
                        col2_real_estate_assets = col2_real_estate_assets.replace(",", "")
                    # Updated Add On (Bracket Filteration )  
                    if ("(") in col1_real_estate_assets:
                        col1_real_estate_assets = col1_real_estate_assets.replace("(", "")
                    if (")") in col1_real_estate_assets:
                        col1_real_estate_assets = col1_real_estate_assets.replace(")", "")
                    if ("(") in col2_real_estate_assets:
                        col2_real_estate_assets = col2_real_estate_assets.replace("(", "")
                    if (")") in col2_real_estate_assets:
                        col2_real_estate_assets = col2_real_estate_assets.replace(")", "")
                print("Column 1 real_estate_assets after filtration is:", col1_real_estate_assets)
                print("Column 2 real_estate_assets after filtration is:", col2_real_estate_assets)
                real_estate_assets_list_column1.append(float(col1_real_estate_assets))
                real_estate_assets_list_column2.append(float(col2_real_estate_assets))
            else:
                real_estate_assets_list_column1.append(0)
                real_estate_assets_list_column2.append(0)

        if (len(real_estate_assets_list_column1) > 1):
            real_estate_assets_list_column1 = sum(real_estate_assets_list_column1)
        if (len(real_estate_assets_list_column2) > 1):
            real_estate_assets_list_column2 = sum(real_estate_assets_list_column2)
            
        if type(real_estate_assets_list_column1) == list:
            pass
        if type(real_estate_assets_list_column2) == list:
            pass
        if type(real_estate_assets_list_column1) in  datatype_filtered_list:
            real_estate_assets_list_column1 = [real_estate_assets_list_column1]
        if type(real_estate_assets_list_column2) in  datatype_filtered_list:
            real_estate_assets_list_column2 = [real_estate_assets_list_column2]
        
        # real_estate_assets
        real_estate_assets_df = pd.DataFrame(
                                            {
                                                data.columns.values[0]: ["Real Estate Assets"],
                                                data.columns[1]: real_estate_assets_list_column1,
                                                data.columns[2]: real_estate_assets_list_column2
                                            }
                                            )

        print(real_estate_assets_df)
    except Exception as e:
        pass
        print("Real Estate Calculation ran into an Exception...", e)

    """
    investments_and_other_assets
    """
    
    investments_and_other_assets_list = [x.lower() for x in investments_and_other_assets_list]
    investments_and_other_assets_list_column1, investments_and_other_assets_list_column2 = [], []
    balance_col_values_total_long_term_assets = [x.lower() for x in balance_col_values_total_long_term_assets]
    for investments_and_other_assets in balance_col_values_total_long_term_assets:
        if investments_and_other_assets in investments_and_other_assets_list:
            print("investments_and_other_assets value found")
            index_of_investments_and_other_assets = balance_col_values.index(investments_and_other_assets)
            col1_investments_and_other_assets = data[data.columns[1]].values[index_of_investments_and_other_assets]
            print("Column 1 investments_and_other_assets is:", col1_investments_and_other_assets)
            col2_investments_and_other_assets = data[data.columns[2]].values[index_of_investments_and_other_assets]
            print("Column 2 investments_and_other_assets is:", col2_investments_and_other_assets)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_investments_and_other_assets) in datatype_filtered_list:
                col1_investments_and_other_assets = col1_investments_and_other_assets
            if type(col2_investments_and_other_assets) in datatype_filtered_list:
                col2_investments_and_other_assets = col2_investments_and_other_assets
            else:
                # Cleaning ($ and ,)
                if "$" in col1_investments_and_other_assets:
                    col1_investments_and_other_assets = col1_investments_and_other_assets.replace("$", "")
                if "$" in col2_investments_and_other_assets:
                    col2_investments_and_other_assets = col2_investments_and_other_assets.replace("$", "")
                if "," in col1_investments_and_other_assets:
                    col1_investments_and_other_assets = col1_investments_and_other_assets.replace(",", "")
                if "," in col2_investments_and_other_assets:
                    col2_investments_and_other_assets = col2_investments_and_other_assets.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_investments_and_other_assets:
                    col1_investments_and_other_assets = col1_investments_and_other_assets.replace("(", "")
                if (")") in col1_investments_and_other_assets:
                    col1_investments_and_other_assets = col1_investments_and_other_assets.replace(")", "")
                if ("(") in col2_investments_and_other_assets:
                    col2_investments_and_other_assets = col2_investments_and_other_assets.replace("(", "")
                if (")") in col2_investments_and_other_assets:
                    col2_investments_and_other_assets = col2_investments_and_other_assets.replace(")", "")
            print("Column 1 investments_and_other_assets after filtration is:", col1_investments_and_other_assets)
            print("Column 2 investments_and_other_assets after filtration is:", col2_investments_and_other_assets)
            investments_and_other_assets_list_column1.append(float(col1_investments_and_other_assets))
            investments_and_other_assets_list_column2.append(float(col2_investments_and_other_assets))
        else:
            investments_and_other_assets_list_column1.append(0)
            investments_and_other_assets_list_column2.append(0)
    
    if (len(investments_and_other_assets_list_column1) > 1):
        investments_and_other_assets_list_column1 = sum(investments_and_other_assets_list_column1)
    if (len(investments_and_other_assets_list_column2) > 1):
        investments_and_other_assets_list_column2 = sum(investments_and_other_assets_list_column2)
        
    if type(investments_and_other_assets_list_column1) == list:
        pass
    if type(investments_and_other_assets_list_column2) == list:
        pass
    if type(investments_and_other_assets_list_column1) in  datatype_filtered_list:
        investments_and_other_assets_list_column1 = [investments_and_other_assets_list_column1]
    if type(investments_and_other_assets_list_column2) in  datatype_filtered_list:
        investments_and_other_assets_list_column2 = [investments_and_other_assets_list_column2]
    
    
    
    # balance_col_values_investments_and_other_assets
    investments_and_other_assets_df = pd.DataFrame(
                                                    {
                                                        data.columns.values[0]: ["Investment Assets"],
                                                        data.columns[1]: investments_and_other_assets_list_column1,
                                                        data.columns[2]: investments_and_other_assets_list_column2
                                                    }
                                                )

    print(investments_and_other_assets_df)

    """
    investments_and_other_companies
    """
    
    investments_and_other_companies_list = [x.lower() for x in investments_and_other_companies_list]
    investments_and_other_companies_list_column1, investments_and_other_companies_list_column2 = [], []
    balance_col_values_total_long_term_assets = [x.lower() for x in balance_col_values_total_long_term_assets]
    for investments_and_other_companies in balance_col_values_total_long_term_assets:
        if investments_and_other_companies in investments_and_other_companies_list:
            print("investments_and_other_companies value found")
            index_of_investments_and_other_companies = balance_col_values.index(investments_and_other_companies)
            col1_investments_and_other_companies = data[data.columns[1]].values[index_of_investments_and_other_companies]
            print("Column 1 investments_and_other_companies is:", col1_investments_and_other_companies)
            col2_investments_and_other_companies = data[data.columns[2]].values[index_of_investments_and_other_companies]
            print("Column 2 investments_and_other_companies is:", col2_investments_and_other_companies)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_investments_and_other_companies) in datatype_filtered_list:
                col1_investments_and_other_companies = col1_investments_and_other_companies
            if type(col2_investments_and_other_companies) in datatype_filtered_list:
                col2_investments_and_other_companies = col2_investments_and_other_companies
            else:
                # Cleaning ($ and ,)
                if "$" in col1_investments_and_other_companies:
                    col1_investments_and_other_companies = col1_investments_and_other_companies.replace("$", "")
                if "$" in col2_investments_and_other_companies:
                    col2_investments_and_other_companies = col2_investments_and_other_companies.replace("$", "")
                if "," in col1_investments_and_other_companies:
                    col1_investments_and_other_companies = col1_investments_and_other_companies.replace(",", "")
                if "," in col2_investments_and_other_companies:
                    col2_investments_and_other_companies = col2_investments_and_other_companies.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_investments_and_other_companies:
                    col1_investments_and_other_companies = col1_investments_and_other_companies.replace("(", "")
                if (")") in col1_investments_and_other_companies:
                    col1_investments_and_other_companies = col1_investments_and_other_companies.replace(")", "")
                if ("(") in col2_investments_and_other_companies:
                    col2_investments_and_other_companies = col2_investments_and_other_companies.replace("(", "")
                if (")") in col2_investments_and_other_companies:
                    col2_investments_and_other_companies = col2_investments_and_other_companies.replace(")", "")
            print("Column 1 investments_and_other_companies after filtration is:", col1_investments_and_other_companies)
            print("Column 2 investments_and_other_companies after filtration is:", col2_investments_and_other_companies)
            investments_and_other_companies_list_column1.append(float(col1_investments_and_other_companies))
            investments_and_other_companies_list_column2.append(float(col2_investments_and_other_companies))
        else:
            investments_and_other_companies_list_column1.append(0)
            investments_and_other_companies_list_column2.append(0)
    
    if (len(investments_and_other_companies_list_column1) > 1):
        investments_and_other_companies_list_column1 = sum(investments_and_other_companies_list_column1)
    if (len(investments_and_other_companies_list_column2) > 1):
        investments_and_other_companies_list_column2 = sum(investments_and_other_companies_list_column2)
        
    if type(investments_and_other_companies_list_column1) == list:
        pass
    if type(investments_and_other_companies_list_column1) == list:
        pass
    if type(investments_and_other_companies_list_column1) in  datatype_filtered_list:
        investments_and_other_companies_list_column1 = [investments_and_other_companies_list_column1]
    if type(investments_and_other_companies_list_column2) in  datatype_filtered_list:
        investments_and_other_companies_list_column2 = [investments_and_other_companies_list_column2]

    
    # balance_col_values_investments_and_other_companies
    investments_and_other_companies_df = pd.DataFrame(
                                                        {
                                                            data.columns.values[0]: ["Investment in Other Companies"],
                                                            data.columns[1]: investments_and_other_companies_list_column1,
                                                            data.columns[2]: investments_and_other_companies_list_column2
                                                        }
                                                    )

    print(investments_and_other_companies_df)
    
    
    """
    pensions_assets
    """
    pensions_assets_list = [x.lower() for x in pensions_assets_list]
    pensions_assets_list_column1, pensions_assets_list_column2 = [], []
    balance_col_values_total_long_term_assets = [x.lower() for x in balance_col_values_total_long_term_assets]
    for pensions_assets in balance_col_values_total_long_term_assets:
        if pensions_assets in pensions_assets_list:
            print("pensions_assets value found")
            index_of_pensions_assets = balance_col_values.index(pensions_assets)
            col1_pensions_assets = data[data.columns[1]].values[index_of_pensions_assets]
            print("Column 1 pensions_assets is:", col1_pensions_assets)
            col2_pensions_assets = data[data.columns[2]].values[index_of_pensions_assets]
            print("Column 2 pensions_assets is:", col2_pensions_assets)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_pensions_assets) in datatype_filtered_list:
                col1_pensions_assets = col1_pensions_assets
            if type(col2_pensions_assets) in datatype_filtered_list:
                col2_pensions_assets = col2_pensions_assets
            else:
                # Cleaning ($ and ,)
                if "$" in col1_pensions_assets:
                    col1_pensions_assets = col1_pensions_assets.replace("$", "")
                if "$" in col2_pensions_assets:
                    col2_pensions_assets = col2_pensions_assets.replace("$", "")
                if "," in col1_pensions_assets:
                    col1_pensions_assets = col1_pensions_assets.replace(",", "")
                if "," in col2_pensions_assets:
                    col2_pensions_assets = col2_pensions_assets.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_pensions_assets:
                    col1_pensions_assets = col1_pensions_assets.replace("(", "")
                if (")") in col1_pensions_assets:
                    col1_pensions_assets = col1_pensions_assets.replace(")", "")
                if ("(") in col2_pensions_assets:
                    col2_pensions_assets = col2_pensions_assets.replace("(", "")
                if (")") in col2_pensions_assets:
                    col2_pensions_assets = col2_pensions_assets.replace(")", "")
            print("Column 1 pensions_assets after filtration is:", col1_pensions_assets)
            print("Column 2 pensions_assets after filtration is:", col2_pensions_assets)
            pensions_assets_list_column1.append(float(col1_pensions_assets))
            pensions_assets_list_column2.append(float(col2_pensions_assets))
        else:
            pensions_assets_list_column1.append(0)
            pensions_assets_list_column2.append(0)

    if (len(pensions_assets_list_column1) > 1):
        pensions_assets_list_column1 = sum(pensions_assets_list_column1)
    if (len(pensions_assets_list_column2) > 1):
        pensions_assets_list_column2 = sum(pensions_assets_list_column2)
        
    if type(pensions_assets_list_column1) == list:
        pass
    if type(pensions_assets_list_column2) == list:
        pass
    if type(pensions_assets_list_column1) in  datatype_filtered_list:
        pensions_assets_list_column1 = [pensions_assets_list_column1]
    if type(pensions_assets_list_column2) in  datatype_filtered_list:
        pensions_assets_list_column2 = [pensions_assets_list_column2]
    print("pensions_assets_list_column1 :",pensions_assets_list_column1)
    print("pensions_assets_list_column2 :",pensions_assets_list_column2)
    # balance_col_values_pensions_assets
    pensions_assets_df = pd.DataFrame(
                                        {
                                            data.columns.values[0]: ["Pensions Assets"],
                                            data.columns[1]: pensions_assets_list_column1,
                                            data.columns[2]: pensions_assets_list_column2
                                        }
                                    )

    print(pensions_assets_df)
    
    """
    Goodwill
    """
    goodwill_list = [x.lower() for x in goodwill_list]
    goodwill_list_column1, goodwill_list_column2 = [], []
    balance_col_values_total_long_term_assets = [x.lower() for x in balance_col_values_total_long_term_assets]
    for goodwill in balance_col_values_total_long_term_assets:
        if goodwill in goodwill_list:
            print("goodwill value found")
            index_of_goodwill = balance_col_values.index(goodwill)
            col1_goodwill = data[data.columns[1]].values[index_of_goodwill]
            print("Column 1 goodwill is:", col1_goodwill)
            col2_goodwill = data[data.columns[2]].values[index_of_goodwill]
            print("Column 2 goodwill is:", col2_goodwill)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_goodwill) in datatype_filtered_list:
                col1_goodwill = col1_pensions_assets
            if type(col2_goodwill) in datatype_filtered_list:
                col2_goodwill = col2_goodwill
            else:
                # Cleaning ($ and ,)
                if "$" in col1_goodwill:
                    col1_goodwill = col1_goodwill.replace("$", "")
                if "$" in col2_goodwill:
                    col2_goodwill = col2_goodwill.replace("$", "")
                if "," in col1_goodwill:
                    col1_goodwill = col1_goodwill.replace(",", "")
                if "," in col2_goodwill:
                    col2_goodwill = col2_goodwill.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_goodwill:
                    col1_goodwill = col1_goodwill.replace("(", "")
                if (")") in col1_goodwill:
                    col1_goodwill = col1_goodwill.replace(")", "")
                if ("(") in col2_goodwill:
                    col2_goodwill = col2_goodwill.replace("(", "")
                if (")") in col2_goodwill:
                    col2_goodwill = col2_goodwill.replace(")", "")
            print("Column 1 goodwill after filtration is:", col1_goodwill)
            print("Column 2 goodwill after filtration is:", col2_goodwill)
            goodwill_list_column1.append(float(col1_goodwill))
            goodwill_list_column2.append(float(col2_goodwill))
        else:
            goodwill_list_column1.append(0)
            goodwill_list_column2.append(0)
        
    if (len(goodwill_list_column1) > 1):
        goodwill_list_column1 = sum(goodwill_list_column1)
    if (len(goodwill_list_column2) > 1):
        goodwill_list_column2 = sum(goodwill_list_column2)
        
    if type(goodwill_list_column1) == list:
        pass
    if type(goodwill_list_column2) == list:
        pass
    if type(goodwill_list_column1) in  datatype_filtered_list:
        goodwill_list_column1 = [goodwill_list_column1]
    if type(goodwill_list_column2) in  datatype_filtered_list:
        goodwill_list_column2 = [goodwill_list_column2]
    # balance_col_values_goodwill
    goodwill_df = pd.DataFrame(
                                {
                                    data.columns.values[0]: ["Goodwill"],
                                    data.columns[1]: col1_goodwill,
                                    data.columns[2]: col2_goodwill
                                }
                            )

    print(goodwill_df)

    
    """
    Indefinite-lived and amortizable intangible assets
    """
    indefinite_lived_and_amortizable_intangible_assets_list = [x.lower() for x in indefinite_lived_and_amortizable_intangible_assets_list]
    indefinite_lived_and_amortizable_intangible_assets_list_column1, indefinite_lived_and_amortizable_intangible_assets_list_column2 = [], []
    balance_col_values_total_long_term_assets = [x.lower() for x in balance_col_values_total_long_term_assets]
    for indefinite_lived_and_amortizable_intangible_assets in balance_col_values_total_long_term_assets:
        if indefinite_lived_and_amortizable_intangible_assets in indefinite_lived_and_amortizable_intangible_assets_list:
            print("indefinite_lived_and_amortizable_intangible_assets value found")
            index_of_indefinite_lived_and_amortizable_intangible_assets = balance_col_values.index(indefinite_lived_and_amortizable_intangible_assets)
            col1_indefinite_lived_and_amortizable_intangible_assets = data[data.columns[1]].values[index_of_indefinite_lived_and_amortizable_intangible_assets]
            print("Column 1 indefinite_lived_and_amortizable_intangible_assets is:", col1_indefinite_lived_and_amortizable_intangible_assets)
            col2_indefinite_lived_and_amortizable_intangible_assets = data[data.columns[2]].values[index_of_indefinite_lived_and_amortizable_intangible_assets]
            print("Column 2 indefinite_lived_and_amortizable_intangible_assets is:", col2_indefinite_lived_and_amortizable_intangible_assets)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_indefinite_lived_and_amortizable_intangible_assets) in datatype_filtered_list:
                col1_indefinite_lived_and_amortizable_intangible_assets = col1_indefinite_lived_and_amortizable_intangible_assets
            if type(col2_indefinite_lived_and_amortizable_intangible_assets) in datatype_filtered_list:
                col2_indefinite_lived_and_amortizable_intangible_assets = col2_indefinite_lived_and_amortizable_intangible_assets
            else:
                # Cleaning ($ and ,)
                if "$" in col1_indefinite_lived_and_amortizable_intangible_assets:
                    col1_indefinite_lived_and_amortizable_intangible_assets = col1_indefinite_lived_and_amortizable_intangible_assets.replace("$", "")
                if "$" in col2_indefinite_lived_and_amortizable_intangible_assets:
                    col2_indefinite_lived_and_amortizable_intangible_assets = col2_indefinite_lived_and_amortizable_intangible_assets.replace("$", "")
                if "," in col1_indefinite_lived_and_amortizable_intangible_assets:
                    col1_indefinite_lived_and_amortizable_intangible_assets = col1_indefinite_lived_and_amortizable_intangible_assets.replace(",", "")
                if "," in col2_indefinite_lived_and_amortizable_intangible_assets:
                    col2_indefinite_lived_and_amortizable_intangible_assets = col2_indefinite_lived_and_amortizable_intangible_assets.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_goodwill:
                    col1_goodwill = col1_goodwill.replace("(", "")
                if (")") in col1_goodwill:
                    col1_goodwill = col1_goodwill.replace(")", "")
                if ("(") in col2_goodwill:
                    col2_goodwill = col2_goodwill.replace("(", "")
                if (")") in col2_goodwill:
                    col2_goodwill = col2_goodwill.replace(")", "")
            print("Column 1 indefinite_lived_and_amortizable_intangible_assets after filtration is:", col1_indefinite_lived_and_amortizable_intangible_assets)
            print("Column 2 indefinite_lived_and_amortizable_intangible_assets after filtration is:", col2_indefinite_lived_and_amortizable_intangible_assets)
            indefinite_lived_and_amortizable_intangible_assets_list_column1.append(float(col1_indefinite_lived_and_amortizable_intangible_assets))
            indefinite_lived_and_amortizable_intangible_assets_list_column2.append(float(col2_indefinite_lived_and_amortizable_intangible_assets))
        else:
            indefinite_lived_and_amortizable_intangible_assets_list_column1.append(0)
            indefinite_lived_and_amortizable_intangible_assets_list_column2.append(0)
    
    if (len(indefinite_lived_and_amortizable_intangible_assets_list_column1) > 1):
        indefinite_lived_and_amortizable_intangible_assets_list_column1 = sum(indefinite_lived_and_amortizable_intangible_assets_list_column1)
    if (len(indefinite_lived_and_amortizable_intangible_assets_list_column2) > 1):
        indefinite_lived_and_amortizable_intangible_assets_list_column2 = sum(indefinite_lived_and_amortizable_intangible_assets_list_column2)
        
    if type(indefinite_lived_and_amortizable_intangible_assets_list_column1) == list:
        pass
    if type(indefinite_lived_and_amortizable_intangible_assets_list_column2) == list:
        pass
    if type(indefinite_lived_and_amortizable_intangible_assets_list_column1) in  datatype_filtered_list:
        indefinite_lived_and_amortizable_intangible_assets_list_column1 = [indefinite_lived_and_amortizable_intangible_assets_list_column1]
    if type(indefinite_lived_and_amortizable_intangible_assets_list_column2) in  datatype_filtered_list:
        indefinite_lived_and_amortizable_intangible_assets_list_column2 = [indefinite_lived_and_amortizable_intangible_assets_list_column2]
    
    # balance_col_values_indefinite_lived_and_amortizable_intangible_assets
    indefinite_lived_and_amortizable_intangible_assets_df = pd.DataFrame(
                                                                            {
                                                                                data.columns.values[0]: ["Indefinite-lived and amortizable intangible assets"],
                                                                                data.columns[1]: indefinite_lived_and_amortizable_intangible_assets_list_column1,
                                                                                data.columns[2]: indefinite_lived_and_amortizable_intangible_assets_list_column2
                                                                            }
                                                                        )

    print(indefinite_lived_and_amortizable_intangible_assets_df)
    
    """
    Deferred income taxes
    """
    deferred_income_taxes_list = [x.lower() for x in deferred_income_taxes_list]
    deferred_income_taxes_list_column1, deferred_income_taxes_list_column2 = [], []
    balance_col_values_total_long_term_assets = [x.lower() for x in balance_col_values_total_long_term_assets]
    for deferred_income_taxes in balance_col_values_total_long_term_assets:
        if deferred_income_taxes in deferred_income_taxes_list:
            print("deferred_income_taxes value found")
            index_of_deferred_income_taxes = balance_col_values.index(deferred_income_taxes)
            col1_deferred_income_taxes = data[data.columns[1]].values[index_of_deferred_income_taxes]
            print("Column 1 deferred_income_taxes is:", col1_deferred_income_taxes)
            col2_deferred_income_taxes = data[data.columns[2]].values[index_of_deferred_income_taxes]
            print("Column 2 deferred_income_taxes is:", col2_deferred_income_taxes)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_deferred_income_taxes) in datatype_filtered_list:
                col1_deferred_income_taxes = col1_deferred_income_taxes
            if type(col2_deferred_income_taxes) in datatype_filtered_list:
                col2_deferred_income_taxes = col2_deferred_income_taxes
            else:
                # Cleaning ($ and ,)
                if "$" in col1_deferred_income_taxes:
                    col1_deferred_income_taxes = col1_deferred_income_taxes.replace("$", "")
                if "$" in col2_deferred_income_taxes:
                    col2_deferred_income_taxes = col2_deferred_income_taxes.replace("$", "")
                if "," in col1_deferred_income_taxes:
                    col1_deferred_income_taxes = col1_deferred_income_taxes.replace(",", "")
                if "," in col2_deferred_income_taxes:
                    col2_deferred_income_taxes = col2_deferred_income_taxes.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_deferred_income_taxes:
                    col1_deferred_income_taxes = col1_deferred_income_taxes.replace("(", "")
                if (")") in col1_deferred_income_taxes:
                    col1_deferred_income_taxes = col1_deferred_income_taxes.replace(")", "")
                if ("(") in col2_deferred_income_taxes:
                    col2_deferred_income_taxes = col2_deferred_income_taxes.replace("(", "")
                if (")") in col2_deferred_income_taxes:
                    col2_deferred_income_taxes = col2_deferred_income_taxes.replace(")", "")
            print("Column 1 deferred_income_taxes after filtration is:", col1_deferred_income_taxes)
            print("Column 2 deferred_income_taxes after filtration is:", col2_deferred_income_taxes)
            deferred_income_taxes_list_column1.append(float(col1_deferred_income_taxes))
            deferred_income_taxes_list_column2.append(float(col2_deferred_income_taxes))
        else:
            deferred_income_taxes_list_column1.append(0)
            deferred_income_taxes_list_column2.append(0)
    
    if (len(deferred_income_taxes_list_column1) > 1):
        deferred_income_taxes_list_column1 = sum(deferred_income_taxes_list_column1)
    if (len(deferred_income_taxes_list_column2) > 1):
        deferred_income_taxes_list_column2 = sum(deferred_income_taxes_list_column2)
        
    if type(deferred_income_taxes_list_column1) == list:
        pass
    if type(deferred_income_taxes_list_column2) == list:
        pass
    if type(deferred_income_taxes_list_column1) in  datatype_filtered_list:
        deferred_income_taxes_list_column1 = [deferred_income_taxes_list_column1]
    if type(deferred_income_taxes_list_column2) in  datatype_filtered_list:
        deferred_income_taxes_list_column2 = [deferred_income_taxes_list_column2]
    
    # balance_col_values_deferred_income_taxes
    deferred_income_taxes_df = pd.DataFrame(
                                                {
                                                    data.columns.values[0]: ["Deferred Income Taxes"],
                                                    data.columns[1]: deferred_income_taxes_list_column1,
                                                    data.columns[2]: deferred_income_taxes_list_column2
                                                }
                                            )

    print(deferred_income_taxes_df)
    
    """
    total_intangible_and_other_assets
    """
    total_intangible_and_other_assets_list = [x.lower() for x in total_intangible_and_other_assets_list]
    total_intangible_and_other_assets_list_column1, total_intangible_and_other_assets_list_column2 = [], []
    balance_col_values_total_long_term_assets = [x.lower() for x in balance_col_values_total_long_term_assets]
    for total_intangible_and_other_assets in balance_col_values_total_long_term_assets:
        if total_intangible_and_other_assets in total_intangible_and_other_assets_list:
            print("total_intangible_and_other_assets value found")
            index_of_total_intangible_and_other_assets = balance_col_values.index(total_intangible_and_other_assets)
            col1_total_intangible_and_other_assets = data[data.columns[1]].values[index_of_total_intangible_and_other_assets]
            print("Column 1 total_intangible_and_other_assets is:", col1_total_intangible_and_other_assets)
            col2_total_intangible_and_other_assets = data[data.columns[2]].values[index_of_total_intangible_and_other_assets]
            print("Column 2 total_intangible_and_other_assets is:", col2_total_intangible_and_other_assets)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_total_intangible_and_other_assets) in datatype_filtered_list:
                col1_total_intangible_and_other_assets = col1_total_intangible_and_other_assets
            if type(col2_total_intangible_and_other_assets) in datatype_filtered_list:
                col2_total_intangible_and_other_assets = col2_total_intangible_and_other_assets
            else:    
                # Cleaning ($ and ,)
                if "$" in col1_total_intangible_and_other_assets:
                    col1_total_intangible_and_other_assets = col1_total_intangible_and_other_assets.replace("$", "")
                if "$" in col2_total_intangible_and_other_assets:
                    col2_total_intangible_and_other_assets = col2_total_intangible_and_other_assets.replace("$", "")
                if "," in col1_total_intangible_and_other_assets:
                    col1_total_intangible_and_other_assets = col1_total_intangible_and_other_assets.replace(",", "")
                if "," in col2_total_intangible_and_other_assets:
                    col2_total_intangible_and_other_assets = col2_total_intangible_and_other_assets.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_total_intangible_and_other_assets:
                    col1_total_intangible_and_other_assets = col1_total_intangible_and_other_assets.replace("(", "")
                if (")") in col1_total_intangible_and_other_assets:
                    col1_total_intangible_and_other_assets = col1_total_intangible_and_other_assets.replace(")", "")
                if ("(") in col2_total_intangible_and_other_assets:
                    col2_total_intangible_and_other_assets = col2_total_intangible_and_other_assets.replace("(", "")
                if (")") in col2_total_intangible_and_other_assets:
                    col2_total_intangible_and_other_assets = col2_total_intangible_and_other_assets.replace(")", "")
            print("Column 1 total_intangible_and_other_assets after filtration is:", col1_total_intangible_and_other_assets)
            print("Column 2 total_intangible_and_other_assets after filtration is:", col2_total_intangible_and_other_assets)
            total_intangible_and_other_assets_list_column1.append(float(col1_total_intangible_and_other_assets))
            total_intangible_and_other_assets_list_column2.append(float(col2_total_intangible_and_other_assets))
        else:
            total_intangible_and_other_assets_list_column1.append(0)
            total_intangible_and_other_assets_list_column2.append(0)
    
    if (len(total_intangible_and_other_assets_list_column1) > 1):
        total_intangible_and_other_assets_list_column1 = sum(total_intangible_and_other_assets_list_column1)
    if (len(total_intangible_and_other_assets_list_column2) > 1):
        total_intangible_and_other_assets_list_column2 = sum(total_intangible_and_other_assets_list_column2)
        
    if type(total_intangible_and_other_assets_list_column1) == list:
        pass
    if type(total_intangible_and_other_assets_list_column2) == list:
        pass
    if type(total_intangible_and_other_assets_list_column1) in  datatype_filtered_list:
        total_intangible_and_other_assets_list_column1 = [total_intangible_and_other_assets_list_column1]
    if type(total_intangible_and_other_assets_list_column2) in  datatype_filtered_list:
        total_intangible_and_other_assets_list_column2 = [total_intangible_and_other_assets_list_column2]
    
    
    
    # balance_col_values_total_intangible_and_other_assets
    total_intangible_and_other_assets_df = pd.DataFrame(
                                                            {
                                                                data.columns.values[0]: ["Total Intangible and Other Assets"],
                                                                data.columns[1]: total_intangible_and_other_assets_list_column1,
                                                                data.columns[2]: total_intangible_and_other_assets_list_column2
                                                            }
                                                        )

    print(total_intangible_and_other_assets_df)
    
    """
    Assets for Discontinued Business
    """
    assets_for_discontinued_business_list = [x.lower() for x in assets_for_discontinued_business_list]
    assets_for_discontinued_business_list_column1, assets_for_discontinued_business_list_column2 = [], []
    balance_col_values_total_long_term_assets = [x.lower() for x in balance_col_values_total_long_term_assets]
    for assets_for_discontinued_business in balance_col_values_total_long_term_assets:
        if assets_for_discontinued_business in assets_for_discontinued_business_list:
            print("assets_for_discontinued_business value found")
            index_of_assets_for_discontinued_business = balance_col_values.index(assets_for_discontinued_business)
            col1_assets_for_discontinued_business = data[data.columns[1]].values[index_of_assets_for_discontinued_business]
            print("Column 1 assets_for_discontinued_business is:", col1_assets_for_discontinued_business)
            col2_assets_for_discontinued_business = data[data.columns[2]].values[index_of_assets_for_discontinued_business]
            print("Column 2 assets_for_discontinued_business is:", col2_assets_for_discontinued_business)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_assets_for_discontinued_business) in datatype_filtered_list:
                col1_assets_for_discontinued_business = col1_assets_for_discontinued_business
            if type(col2_assets_for_discontinued_business) in datatype_filtered_list:
                col2_assets_for_discontinued_business = col2_assets_for_discontinued_business
            else:    
                # Cleaning ($ and ,)
                if "$" in col1_assets_for_discontinued_business:
                    col1_assets_for_discontinued_business = col1_assets_for_discontinued_business.replace("$", "")
                if "$" in col2_assets_for_discontinued_business:
                    col2_assets_for_discontinued_business = col2_assets_for_discontinued_business.replace("$", "")
                if "," in col1_assets_for_discontinued_business:
                    col1_assets_for_discontinued_business = col1_assets_for_discontinued_business.replace(",", "")
                if "," in col2_assets_for_discontinued_business:
                    col2_assets_for_discontinued_business = col2_assets_for_discontinued_business.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_assets_for_discontinued_business:
                    col1_assets_for_discontinued_business = col1_assets_for_discontinued_business.replace("(", "")
                if (")") in col1_assets_for_discontinued_business:
                    col1_assets_for_discontinued_business = col1_assets_for_discontinued_business.replace(")", "")
                if ("(") in col2_assets_for_discontinued_business:
                    col2_assets_for_discontinued_business = col2_assets_for_discontinued_business.replace("(", "")
                if (")") in col2_assets_for_discontinued_business:
                    col2_assets_for_discontinued_business = col2_assets_for_discontinued_business.replace(")", "")
            print("Column 1 assets_for_discontinued_business after filtration is:", col1_assets_for_discontinued_business)
            print("Column 2 assets_for_discontinued_business after filtration is:", col2_assets_for_discontinued_business)
            assets_for_discontinued_business_list_column1.append(float(col1_assets_for_discontinued_business))
            assets_for_discontinued_business_list_column2.append(float(col2_assets_for_discontinued_business))
        else:
            assets_for_discontinued_business_list_column1.append(0)
            assets_for_discontinued_business_list_column2.append(0)
    
    if (len(assets_for_discontinued_business_list_column1) > 1):
        assets_for_discontinued_business_list_column1 = sum(assets_for_discontinued_business_list_column1)
    if (len(assets_for_discontinued_business_list_column2) > 1):
        assets_for_discontinued_business_list_column2 = sum(assets_for_discontinued_business_list_column2)
        
    if type(assets_for_discontinued_business_list_column1) == list:
        pass
    if type(assets_for_discontinued_business_list_column2) == list:
        pass
    if type(assets_for_discontinued_business_list_column1) in  datatype_filtered_list:
        assets_for_discontinued_business_list_column1 = [assets_for_discontinued_business_list_column1]
    if type(assets_for_discontinued_business_list_column2) in  datatype_filtered_list:
        assets_for_discontinued_business_list_column2 = [assets_for_discontinued_business_list_column2]
    
    
    
    
    # balance_col_values_assets_for_discontinued_business
    assets_for_discontinued_business_df = pd.DataFrame(
                                                            {
                                                                data.columns.values[0]: ["Assets for Discontinued Business"],
                                                                data.columns[1]: assets_for_discontinued_business_list_column1,
                                                                data.columns[2]: assets_for_discontinued_business_list_column2
                                                            }
                                                        )

    print(assets_for_discontinued_business_df)
    
    
    """
    Total Long Term Assets Calculation
    """
    total_long_term_assets_list = [x.lower() for x in total_long_term_assets_list]
    total_long_term_assets_list_column1, total_long_term_assets_list_column2 = [], []
    balance_col_values_total_long_term_assets = [x.lower() for x in balance_col_values_total_long_term_assets]
    for total_long_term_assets in balance_col_values_total_long_term_assets:
        if total_long_term_assets in total_long_term_assets_list:
            print("total_long_term_assets value found")
            index_of_total_long_term_assets = balance_col_values.index(total_long_term_assets)
            col1_total_long_term_assets = data[data.columns[1]].values[index_of_total_long_term_assets]
            print("Column 1 total_long_term_assets is:", col1_total_long_term_assets)
            col2_total_long_term_assets = data[data.columns[2]].values[index_of_total_long_term_assets]
            print("Column 2 total_long_term_assets is:", col2_total_long_term_assets)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_total_long_term_assets) in datatype_filtered_list:
                col1_total_long_term_assets = col1_total_long_term_assets
            if type(col2_total_long_term_assets) in datatype_filtered_list:
                col2_total_long_term_assets = col2_total_long_term_assets
            else:    
                # Cleaning ($ and ,)
                if "$" in col1_total_long_term_assets:
                    col1_total_long_term_assets = col1_total_long_term_assets.replace("$", "")
                if "$" in col2_total_long_term_assets:
                    col2_total_long_term_assets = col2_total_long_term_assets.replace("$", "")
                if "," in col1_total_long_term_assets:
                    col1_total_long_term_assets = col1_total_long_term_assets.replace(",", "")
                if "," in col2_total_long_term_assets:
                    col2_total_long_term_assets = col2_total_long_term_assets.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_total_long_term_assets:
                    col1_total_long_term_assets = col1_total_long_term_assets.replace("(", "")
                if (")") in col1_total_long_term_assets:
                    col1_total_long_term_assets = col1_total_long_term_assets.replace(")", "")
                if ("(") in col2_total_long_term_assets:
                    col2_total_long_term_assets = col2_total_long_term_assets.replace("(", "")
                if (")") in col2_total_long_term_assets:
                    col2_total_long_term_assets = col2_total_long_term_assets.replace(")", "")
            print("Column 1 total_long_term_assets after filtration is:", col1_total_long_term_assets)
            print("Column 2 total_long_term_assets after filtration is:", col2_total_long_term_assets)
            total_long_term_assets_list_column1.append(int(col1_total_long_term_assets))
            total_long_term_assets_list_column2.append(int(col2_total_long_term_assets))
            print("total_long_term_assets_list_column1: ",total_long_term_assets_list_column1)
            print("total_long_term_assets_list_column2: ",total_long_term_assets_list_column2)
        else:
            total_long_term_assets_list_column1.append(0)
            total_long_term_assets_list_column2.append(0)
    
    if (len(total_long_term_assets_list_column1) > 1):
        total_long_term_assets_list_column1 = sum(total_long_term_assets_list_column1)
    if (len(total_long_term_assets_list_column2) > 1):
        total_long_term_assets_list_column2 = sum(total_long_term_assets_list_column2)
        
    if type(total_long_term_assets_list_column1) == list:
        pass
    if type(total_long_term_assets_list_column2) == list:
        pass
    if type(total_long_term_assets_list_column1) in  datatype_filtered_list:
        total_long_term_assets_list_column1 = [total_long_term_assets_list_column1]
    if type(total_long_term_assets_list_column2) in  datatype_filtered_list:
        total_long_term_assets_list_column2 = [total_long_term_assets_list_column2]
    
    print("total_long_term_assets_list_column1:",total_long_term_assets_list_column1)
    print("total_long_term_assets_list_column2:",total_long_term_assets_list_column2)
    # balance_col_values_total_long_term_assets
    total_long_term_assets_df = pd.DataFrame(
                                                {
                                                    data.columns.values[0]: ["Total Long Term Assets"],
                                                    data.columns[1]: sum(total_long_term_assets_list_column1),
                                                    data.columns[2]: sum(total_long_term_assets_list_column2)
                                                }
                                            )

    print(total_long_term_assets_df)

    
    
    
    """
    Intangible assets   :   Goodwill + Indefinite-lived and amortizable intangible assets
    """
    intangible_assets_df = pd.concat([goodwill_df,
                                      indefinite_lived_and_amortizable_intangible_assets_df,], axis =0)
    intangible_assets_df = intangible_assets_df.reset_index()
    print(intangible_assets_df[intangible_assets_df.columns[2]].tolist())
    intangible_assets_col1_values = 1
    intangible_assets_col2_values = 0
    intangible_assets_col1_values_to_int = []
    intangible_assets_col2_values_to_int = []
    intangible_assets_col1_values = intangible_assets_df[intangible_assets_df.columns[2]].tolist()
    print("current_assets_col1_values",intangible_assets_col1_values)
    print("current_assets_col2_values",intangible_assets_col2_values)
    intangible_assets_col2_values = intangible_assets_df[intangible_assets_df.columns[3]].tolist()
    for i in intangible_assets_col1_values:
        intangible_assets_col1_values_to_int.append(float(i))

    for j in intangible_assets_col2_values:
        intangible_assets_col2_values_to_int.append(float(j))
    print("intangible_assets_col1_values_to_int", intangible_assets_col1_values_to_int)
    print("intangible_assets_col2_values_to_int", intangible_assets_col2_values_to_int)
    print("data.columns[0]:", data.columns[0])
    print("data.columns[1]:", data.columns[1])
    print("data.columns[2]:", data.columns[2])
    print("sum(intangible_assets_col1_values_to_int)", sum(intangible_assets_col1_values_to_int))
    print("sum(intangible_assets_col2_values_to_int)", sum(intangible_assets_col2_values_to_int))
    total_intangible_assets_df_calculated = pd.DataFrame({data.columns.values[0]: "Total Intangible Assets (Calculated)",
                                                        data.columns[1]: [sum(intangible_assets_col1_values_to_int)],
                                                        data.columns[2]: [sum(intangible_assets_col2_values_to_int)]
                                                        })
    print(total_intangible_assets_df_calculated)
    total_intangible_assets_df_calculated = total_intangible_assets_df_calculated.reset_index()
    
    
    """
    other_assets_calculation
    """    
    other_assets_df = pd.concat([deferred_income_taxes_df, total_intangible_and_other_assets_df], axis=0)
    other_assets_df = other_assets_df.reset_index()
    print(other_assets_df[other_assets_df.columns[2]].tolist())
    other_assets_col1_values = 0
    other_assets_col2_values = 0
    other_assets_col1_values_to_int = []
    other_assets_col2_values_to_int = []
    other_assets_col1_values = other_assets_df[other_assets_df.columns[2]].tolist()
    print("other_assets_col1_values", other_assets_col1_values)
    print("other_assets_col2_values", other_assets_col2_values)
    other_assets_col2_values = other_assets_df[other_assets_df.columns[3]].tolist()
    for i in other_assets_col1_values:
        other_assets_col1_values_to_int.append(float(i))

    for j in other_assets_col2_values:
        other_assets_col2_values_to_int.append(float(j))
    print("other_assets_col1_values_to_int", other_assets_col1_values_to_int)
    print("other_assets_col2_values_to_int", other_assets_col2_values_to_int)
    print("data.columns[0]:", data.columns[0])
    print("data.columns[1]:", data.columns[1])
    print("data.columns[2]:", data.columns[2])
    print("sum(other_assets_col1_values_to_int)", sum(other_assets_col1_values_to_int))
    print("sum(other_assets_col2_values_to_int)", sum(other_assets_col2_values_to_int))
    total_other_assets_df_calculated = pd.DataFrame({data.columns.values[0]: "Total Other Assets (Calculated)",
                                                    data.columns[1]: [sum(other_assets_col1_values_to_int)],
                                                    data.columns[2]: [sum(other_assets_col2_values_to_int)]
                                                    })
    print(total_other_assets_df_calculated)
    total_other_assets_df_calculated = total_other_assets_df_calculated.reset_index()

    
    
    
    final_data = pd.concat([rights_data_df,
                            net_property_data_df,
                            real_estate_assets_df,
                            investments_and_other_assets_df,
                            investments_and_other_companies_df,
                            pensions_assets_df,
                            # goodwill_df,
                            # indefinite_lived_and_amortizable_intangible_assets_df,
                            total_intangible_assets_df_calculated,
                            # deferred_income_taxes_df,
                            # total_intangible_and_other_assets_df,
                            total_other_assets_df_calculated,
                            assets_for_discontinued_business_df,
                            
                            ], axis =0)
    
    final_data_col1_int , final_data_col2_int = [], []
    final_data_col1 = final_data[data.columns[1]].values
    print("final_data_col1: ",final_data_col1)
    final_data_col2 = final_data[data.columns[2]].values
    print("final_data_col2: ",final_data_col2)
    for col1,col2 in zip(final_data_col1,final_data_col2):
        final_data_col1_int.append(int(col1))
        final_data_col2_int.append(int(col2))
    total_final_data_col1_int = sum(final_data_col1_int)
    total_final_data_col2_int = sum(final_data_col2_int)
    
    total_long_term_assets_df_calculated = pd.DataFrame({data.columns.values[0]: "Total Long Term Assets (Calculated)",
                                                    data.columns[1]: [total_final_data_col1_int],
                                                    data.columns[2]: [total_final_data_col2_int]
                                                    })
    
    print("total_long_term_assets_list_column1: ", sum(total_long_term_assets_list_column1))
    print("total_long_term_assets_list_column2: ", sum(total_long_term_assets_list_column2))
    print("total_final_data_col1_int:",total_final_data_col1_int)
    print("total_final_data_col2_int:",total_final_data_col2_int)
        
    error_col1_total_long_term_assets_data = sum(total_long_term_assets_list_column1) - total_final_data_col1_int
    error_col2_total_long_term_assets_data = sum(total_long_term_assets_list_column1) - total_final_data_col2_int
    
    error_total_other_assets_df_calculated = pd.DataFrame({data.columns.values[0]: "Total Long Term Assets (Error)",
                                                    data.columns[1]: [error_col1_total_long_term_assets_data],
                                                    data.columns[2]: [error_col2_total_long_term_assets_data]
                                                    })
    
    
    
    # print(error_col1_total_current_assets_data,error_col2_total_current_assets_data)
    final_data = pd.concat([rights_data_df,
                            net_property_data_df,
                            real_estate_assets_df,
                            investments_and_other_assets_df,
                            investments_and_other_companies_df,
                            pensions_assets_df,
                            # goodwill_df,
                            # indefinite_lived_and_amortizable_intangible_assets_df,
                            total_intangible_assets_df_calculated,
                            # deferred_income_taxes_df,
                            # total_intangible_and_other_assets_df,
                            total_other_assets_df_calculated,
                            assets_for_discontinued_business_df,
                            total_long_term_assets_df,
                            total_long_term_assets_df_calculated,
                            error_total_other_assets_df_calculated
                            ], axis =0)
    
    
    print(final_data)
    return final_data


def get_total_assets_data(current_assets_standerdised_data,long_term_assets_standerdized_data,data, database_data):
    total_asset_calculated_data = 0
    current_assets_standerdised_data = current_assets_standerdised_data.reset_index()
    current_assets_standerdised_data.drop(["level_0","index"], axis =1 , inplace = True)
    current_assets_standerdised_data_total_current_assets_standerdised_column1 =  current_assets_standerdised_data[current_assets_standerdised_data[current_assets_standerdised_data[current_assets_standerdised_data.columns[0]]=="Total Current Assets (Calculated)"].columns[1]][8]
    current_assets_standerdised_data_total_current_assets_standerdised_column2 = current_assets_standerdised_data[current_assets_standerdised_data[current_assets_standerdised_data[current_assets_standerdised_data.columns[0]]=="Total Current Assets (Calculated)"].columns[2]][8]
    long_term_assets_standerdized_data = long_term_assets_standerdized_data.reset_index()
    long_term_assets_standerdized_data.drop(["level_0","index"], axis =1 , inplace = True)
    long_term_assets_standerdized_data_column1 = long_term_assets_standerdized_data[long_term_assets_standerdized_data[long_term_assets_standerdized_data[long_term_assets_standerdized_data.columns[0]] == "Total Long Term Assets (Calculated)"].columns[1]][10]
    long_term_assets_standerdized_data_column2 = long_term_assets_standerdized_data[long_term_assets_standerdized_data[long_term_assets_standerdized_data[long_term_assets_standerdized_data.columns[0]] == "Total Long Term Assets (Calculated)"].columns[2]][10]
    total_current_assets_column1 = long_term_assets_standerdized_data_column1 + current_assets_standerdised_data_total_current_assets_standerdised_column1
    total_current_assets_column2 = long_term_assets_standerdized_data_column2 + current_assets_standerdised_data_total_current_assets_standerdised_column2
    total_asset_calculated_data = pd.DataFrame({"Total Assets (Calculated)": [total_current_assets_column1,total_current_assets_column2]}).T
    total_asset_calculated_data = total_asset_calculated_data.reset_index()
    total_asset_calculated_data.columns = long_term_assets_standerdized_data.columns
    
    
    
    ###### Extract actual total asset
    balance_col_values  = data[data.columns[0]].tolist()
    total_assets_list = database_data["Total assets"].values
    datatype_filtered_list = [int,float]
    total_assets_list = [x.lower() for x in total_assets_list]
    total_assets_list_column1, total_assets_list_column2 = [], []
    balance_col_values = [x.lower() for x in balance_col_values]
    for total_assets in balance_col_values:
        try:
            if total_assets in total_assets_list:
                print("total_assets value found")
            
                index_of_total_assets = balance_col_values.index(total_assets)
                print("index_of_total_assets :", index_of_total_assets)
                col1_total_assets = data[data.columns[1]].values[index_of_total_assets]
                print("Column 1 total_assets is:", col1_total_assets)
                col2_total_assets = data[data.columns[2]].values[index_of_total_assets]
                print("Column 2 total_assets is:", col2_total_assets)
                # Checking if the col1 and col2 datatypes are int, float or not
                if type(col1_total_assets) in datatype_filtered_list:
                    col1_total_assets = col1_total_assets
                if type(col2_total_assets) in datatype_filtered_list:
                    col2_total_assets = col2_total_assets
                else:    
                    # Cleaning ($ and ,)
                    if "$" in col1_total_assets:
                        col1_total_assets = col1_total_assets.replace("$", "")
                    if "$" in col2_total_assets:
                        col2_total_assets = col2_total_assets.replace("$", "")
                    if "," in col1_total_assets:
                        col1_total_assets = col1_total_assets.replace(",", "")
                    if "," in col2_total_assets:
                        col2_total_assets = col2_total_assets.replace(",", "")
                    # Updated Add On (Bracket Filteration )  
                    if ("(") in col1_total_assets:
                        col1_total_assets = col1_total_assets.replace("(", "")
                    if (")") in col1_total_assets:
                        col1_total_assets = col1_total_assets.replace(")", "")
                    if ("(") in col2_total_assets:
                        col2_total_assets = col2_total_assets.replace("(", "")
                    if (")") in col2_total_assets:
                        col2_total_assets = col2_total_assets.replace(")", "")
                print("Column 1 total_assets after filtration is:", col1_total_assets)
                print("Column 2 total_assets after filtration is:", col2_total_assets)
                total_assets_list_column1.append(float(col1_total_assets))
                total_assets_list_column2.append(float(col2_total_assets))
                print("total_assets_list_column1: ",total_assets_list_column1)
                print("total_assets_list_column2: ",total_assets_list_column2)
            else:
                total_assets_list_column1.append(0)
                total_assets_list_column2.append(0)
        except Exception as e:
            pass
            print("Exception for Total Assets:", e)
            

    if (len(total_assets_list_column1) > 1):
        total_assets_list_column1 = sum(total_assets_list_column1)
    if (len(total_assets_list_column2) > 1):
        total_assets_list_column2 = sum(total_assets_list_column2)

    if type(total_assets_list_column1) == list:
        pass
    if type(total_assets_list_column2) == list:
        pass
    if type(total_assets_list_column1) in datatype_filtered_list:
        total_assets_list_column1 = [total_assets_list_column1]
    if type(total_assets_list_column2) in datatype_filtered_list:
        total_assets_list_column2 = [total_assets_list_column2]

    print("total_assets_list_column1:", total_assets_list_column1)
    print("total_assets_list_column2:", total_assets_list_column2)
    # balance_col_values_total_assets
    try:
        total_assets_df = pd.DataFrame(
                                        {
                                            data.columns.values[0]: ["Total Assets"],
                                            data.columns[1]: total_assets_list_column1,
                                            data.columns[2]: total_assets_list_column2
                                        }
                                    )
    except Exception as e:
        pass
        total_assets_df = pd.DataFrame(
                                            {
                                                data.columns.values[0]: ["Total Assets"],
                                                data.columns[1]: sum(total_assets_list_column1),
                                                data.columns[2]: sum(total_assets_list_column2)
                                            }
                                        )

    print(total_assets_df)
    
    #### Perform Error Check
    if type(total_assets_list_column1) in  datatype_filtered_list:
        pass
    if type(total_assets_list_column2) in  datatype_filtered_list:
        pass
    if type(total_assets_list_column1) ==  list:
        total_assets_list_column1 = float(total_assets_list_column1[0])
    if type(total_assets_list_column2) ==  list:
        total_assets_list_column2 = float(total_assets_list_column2[0])
    


    if type(total_current_assets_column1) in datatype_filtered_list:
        pass
    if type(total_current_assets_column2) in datatype_filtered_list:
        pass
    
    
    if type(total_current_assets_column1) ==  list:
        total_current_assets_column1 = float(total_current_assets_column1[0])
    if type(total_current_assets_column2) ==  list:
        total_current_assets_column2 = float(total_current_assets_column2[0]) 
    ########################################### Monitor Logs here ########################
    
    
    error_total_asset_col1 = float(total_assets_list_column1)-float(total_current_assets_column1)
    error_total_asset_col2 = float(total_assets_list_column2)-float(total_current_assets_column2)                                                              
    
    print("*"*50)
    print("Total Asset Error Col1 :", error_total_asset_col1)
    print("Total Asset Error Col2 :", error_total_asset_col2)
    print("*"*50)
    try:
        total_assets_error_df = pd.DataFrame(
                                                {
                                                    data.columns.values[0]: ["Total Assets (Error)"],
                                                    data.columns[1]: [error_total_asset_col1],
                                                    data.columns[2]: [error_total_asset_col2]
                                                }
                                            )
    except Exception as e:
        pass
        print("total_assets_error:",e)
        total_assets_error_df = pd.DataFrame(
                                        {
                                            data.columns.values[0]: ["Total Assets (Error)"],
                                            data.columns[1]: sum([error_total_asset_col1]),
                                            data.columns[2]: sum([error_total_asset_col2])
                                        }
                                    )
    
    
    ### Render Final Data
    final_data = pd.concat([current_assets_standerdised_data,
                            long_term_assets_standerdized_data,
                            total_asset_calculated_data,
                            total_assets_df,
                            total_assets_error_df],axis =0)
    return final_data


def get_stabnderdized_total_current_liabilities_data(database_data,data):
    """
    Arguments : database_data and data
    - database_data : Here the database data is the standerdized gsheet data
    - data: Here data refers to the downloaded balance sheet data where we will perform the overall standerdization operation
    """ 
    
    # Local variable declaration    
    col1_short_term_debt = 0
    col2_short_term_debt = 0
    col1_leases = 0
    col2_leases = 0
    col1_accounts_payables = 0
    col2_accounts_payables = 0
    col1_deferred_tax_revenues = 0
    col2_deferred_tax_revenues = 0
    col1_other_current_liabilities = 0
    col2_other_current_liabilities = 0
    col1_total_current_liabilities = 0
    col2_total_current_liabilities = 0
    datatype_filtered_list = [int, float]
    
    # Preprocessing
    if 'Unnamed: 0' in data.columns:
        data.drop(['Unnamed: 0'], axis =1 , inplace = True)
    print(data.isnull().sum())
    print(database_data.isnull().sum())
    data.fillna(0, inplace= True)
    for col in database_data.columns:
        if database_data[col].dtypes == "object":
            database_data[col].fillna(col, inplace= True)

    """
    - After preprocessing we are starting the standerdization as per the given SOP
    - We are using exact search mechamnism to get and standerdize our data.
    - In this function we will standerdize for all the total current assets.
    """
    # Decalation of all Current assets line items.
    balance_col_values = data[data.columns.values[0]].values
    rights_data_list = database_data["Right-of-use assets for operating leases"].values
    current_portion_long_term_debt_list = database_data["Current portion of long-term debt"].values
    current_portion_operating_lease_liability_list = database_data["Current portion of operating lease liability"].values
    accounts_payable_list = database_data["Accounts payable"].values
    accrued_liabilities_list = database_data["Accrued liabilities"].values
    income_taxes_payable_list = database_data["Income taxes payable"].values
    deferred_income_list = database_data["Deferred income"].values
    contracts_payable_programming_rights_list = database_data["Contracts payable for programming rights"].values
    total_current_liabilities_list = database_data["Total current liabilities"].values


    
    balance_col_values_total_current_liabilities  = [x.lower() for x in balance_col_values]
    for total_asset,total_current_liabilities in zip(database_data["Total assets"],database_data["Total current liabilities"]):
        # Tuning it to lowercase for fextact filteration.
        print("Total assets lower is :",total_asset.lower())
        print("total_current_liabilities lower is :",total_asset.lower())
        total_asset_lower = total_asset.lower()
        total_current_liabilities_lower = total_current_liabilities.lower()
         
        if total_asset_lower in balance_col_values_total_current_liabilities:
            total_asset_idx = balance_col_values_total_current_liabilities.index(total_asset_lower)
            print("total_asset_idx: ",total_asset_idx)
        
        if total_current_liabilities_lower in balance_col_values_total_current_liabilities:
            total_current_liabilities_idx = balance_col_values_total_current_liabilities.index(total_current_liabilities_lower)
            print("total_current_liabilities_idx: ",total_current_liabilities_idx)
    
    # These are the baalnce col values for the current assets....
    balance_col_values_total_current_liabilities = balance_col_values_total_current_liabilities[total_asset_idx+1:total_current_liabilities_idx + 1]
    print(total_current_liabilities_idx)

    
    """
    Current portion of long-term debt
    """
    balance_col_values  = data[data.columns[0]].tolist()
    balance_col_values = [x.lower() for x in balance_col_values]
    current_portion_long_term_debt_list = database_data["Current portion of long-term debt"].values
    current_portion_long_term_debt_list = [x.lower() for x in current_portion_long_term_debt_list]
    current_portion_long_term_debt_col1, current_portion_long_term_debt_col2 = [], []

    balance_col_values_current_portion_long_term_debt = [x.lower() for x in balance_col_values_total_current_liabilities]

    for current_portion_long_term_debt in balance_col_values_current_portion_long_term_debt:
        if current_portion_long_term_debt in current_portion_long_term_debt_list:
            print("current_portion_long_term_debt value found")
            index_of_current_portion_long_term_debt = balance_col_values.index(current_portion_long_term_debt)
            col1_current_portion_long_term_debt = data[data.columns[1]].values[index_of_current_portion_long_term_debt]
            print("Column 1 current_portion_long_term_debt is:", col1_current_portion_long_term_debt)
            col2_current_portion_long_term_debt = data[data.columns[2]].values[index_of_current_portion_long_term_debt]
            print("Column 2 current_portion_long_term_debt is:", col2_current_portion_long_term_debt)
            
            # Checking if the col1 and col2 data types are int, float or not
            if type(col1_current_portion_long_term_debt) in datatype_filtered_list:
                col1_current_portion_long_term_debt = col1_current_portion_long_term_debt
            if type(col2_current_portion_long_term_debt) in datatype_filtered_list:
                col2_current_portion_long_term_debt = col2_current_portion_long_term_debt
            else:
                # Cleaning ($ and ,)
                if "$" in col1_current_portion_long_term_debt:
                    col1_current_portion_long_term_debt = col1_current_portion_long_term_debt.replace("$", "")
                if "$" in col2_current_portion_long_term_debt:
                    col2_current_portion_long_term_debt = col2_current_portion_long_term_debt.replace("$", "")
                if "," in col1_current_portion_long_term_debt:
                    col1_current_portion_long_term_debt = col1_current_portion_long_term_debt.replace(",", "")
                if "," in col2_current_portion_long_term_debt:
                    col2_current_portion_long_term_debt = col2_current_portion_long_term_debt.replace(",", "")
                    
            print("Column 1 current_portion_long_term_debt after filtration is:", col1_current_portion_long_term_debt)
            print("Column 2 current_portion_long_term_debt after filtration is:", col2_current_portion_long_term_debt)
            current_portion_long_term_debt_col1.append(float(col1_current_portion_long_term_debt))
            current_portion_long_term_debt_col2.append(float(col2_current_portion_long_term_debt))

    if (len(current_portion_long_term_debt_col1) > 1):
        current_portion_long_term_debt_col1 = sum(current_portion_long_term_debt_col1)
    if (len(current_portion_long_term_debt_col2) > 1):
        current_portion_long_term_debt_col2 = sum(current_portion_long_term_debt_col2)
        
    if type(current_portion_long_term_debt_col1) == list:
        pass
    if type(current_portion_long_term_debt_col2) == list:
        pass
    if type(current_portion_long_term_debt_col1) in  datatype_filtered_list:
        current_portion_long_term_debt_col1 = [current_portion_long_term_debt_col1]
    if type(current_portion_long_term_debt_col2) in  datatype_filtered_list:
        current_portion_long_term_debt_col2 = [current_portion_long_term_debt_col2]
    
    try:    
        current_portion_long_term_debt_df = pd.DataFrame(
                                                            {
                                                                data.columns.values[0]: ["Current portion of long-term debt"],
                                                                data.columns[1]: current_portion_long_term_debt_col1,
                                                                data.columns[2]: current_portion_long_term_debt_col2
                                                            }
                                                        )
    except Exception as e:
        pass
        current_portion_long_term_debt_df = pd.DataFrame(
                                                            {
                                                                data.columns.values[0]: ["Current portion of long-term debt"],
                                                                data.columns[1]: [sum(current_portion_long_term_debt_col1)],
                                                                data.columns[2]: [sum(current_portion_long_term_debt_col2)]
                                                            }
                                                        )

    print(current_portion_long_term_debt_df)

    
    """
    Current portion of operating lease liability
    """
    balance_col_values  = data[data.columns[0]].tolist()
    balance_col_values = [x.lower() for x in balance_col_values]
    current_portion_operating_lease_liability_list = database_data["Current portion of operating lease liability"].values
    current_portion_operating_lease_liability_list = [x.lower() for x in current_portion_operating_lease_liability_list]
    current_portion_operating_lease_liability_col1, current_portion_operating_lease_liability_col2 = [], []

    balance_col_values_current_portion_operating_lease_liability = [x.lower() for x in balance_col_values_total_current_liabilities]

    for current_portion_operating_lease_liability in balance_col_values_current_portion_operating_lease_liability:
        if current_portion_operating_lease_liability in current_portion_operating_lease_liability_list:
            print("current_portion_operating_lease_liability value found")
            index_of_current_portion_operating_lease_liability = balance_col_values.index(current_portion_operating_lease_liability)
            col1_current_portion_operating_lease_liability = data[data.columns[1]].values[index_of_current_portion_operating_lease_liability]
            print("Column 1 current_portion_operating_lease_liability is:", col1_current_portion_operating_lease_liability)
            col2_current_portion_operating_lease_liability = data[data.columns[2]].values[index_of_current_portion_operating_lease_liability]
            print("Column 2 current_portion_operating_lease_liability is:", col2_current_portion_operating_lease_liability)

            # Checking if the col1 and col2 data types are int, float or not
            if type(col1_current_portion_operating_lease_liability) in datatype_filtered_list:
                col1_current_portion_operating_lease_liability = col1_current_portion_operating_lease_liability
            if type(col2_current_portion_operating_lease_liability) in datatype_filtered_list:
                col2_current_portion_operating_lease_liability = col2_current_portion_operating_lease_liability
            else:
                # Cleaning ($ and ,)
                if "$" in col1_current_portion_operating_lease_liability:
                    col1_current_portion_operating_lease_liability = col1_current_portion_operating_lease_liability.replace("$", "")
                if "$" in col2_current_portion_operating_lease_liability:
                    col2_current_portion_operating_lease_liability = col2_current_portion_operating_lease_liability.replace("$", "")
                if "," in col1_current_portion_operating_lease_liability:
                    col1_current_portion_operating_lease_liability = col1_current_portion_operating_lease_liability.replace(",", "")
                if "," in col2_current_portion_operating_lease_liability:
                    col2_current_portion_operating_lease_liability = col2_current_portion_operating_lease_liability.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_current_portion_operating_lease_liability:
                    col1_current_portion_operating_lease_liability = col1_current_portion_operating_lease_liability.replace("(", "")
                if (")") in col1_current_portion_operating_lease_liability:
                    col1_current_portion_operating_lease_liability = col1_current_portion_operating_lease_liability.replace(")", "")
                if ("(") in col2_current_portion_operating_lease_liability:
                    col2_current_portion_operating_lease_liability = col2_current_portion_operating_lease_liability.replace("(", "")
                if (")") in col2_current_portion_operating_lease_liability:
                    col2_current_portion_operating_lease_liability = col2_current_portion_operating_lease_liability.replace(")", "")
            print("Column 1 current_portion_operating_lease_liability after filtration is:", col1_current_portion_operating_lease_liability)
            print("Column 2 current_portion_operating_lease_liability after filtration is:", col2_current_portion_operating_lease_liability)
            current_portion_operating_lease_liability_col1.append(float(col1_current_portion_operating_lease_liability))
            current_portion_operating_lease_liability_col2.append(float(col1_current_portion_operating_lease_liability))
            
    

    if len(current_portion_operating_lease_liability_col1) > 1:
        current_portion_operating_lease_liability_col1 = sum(current_portion_operating_lease_liability_col1)
    if len(current_portion_operating_lease_liability_col2) > 1:
        current_portion_operating_lease_liability_col2 = sum(current_portion_operating_lease_liability_col2)

    if type(current_portion_operating_lease_liability_col1) == list:
        pass
    if type(current_portion_operating_lease_liability_col2) == list:
        pass
    if type(current_portion_operating_lease_liability_col1) in datatype_filtered_list:
        current_portion_operating_lease_liability_col1 = [current_portion_operating_lease_liability_col1]
    if type(current_portion_operating_lease_liability_col2) in datatype_filtered_list:
        current_portion_operating_lease_liability_col2 = [current_portion_operating_lease_liability_col2]

    try:
        current_portion_operating_lease_liability_df = pd.DataFrame(
                                                                        {
                                                                            data.columns.values[0]: ["Current portion of operating lease liability"],
                                                                            data.columns[1]: current_portion_operating_lease_liability_col1,
                                                                            data.columns[2]: current_portion_operating_lease_liability_col2
                                                                        }
                                                                    )
    except Exception as e:
        pass
        current_portion_operating_lease_liability_df = pd.DataFrame(
                                                                        {
                                                                            data.columns.values[0]: ["Current portion of operating lease liability"],
                                                                            data.columns[1]: [sum(current_portion_operating_lease_liability_col1)],
                                                                            data.columns[2]: [sum(current_portion_operating_lease_liability_col2)]
                                                                        }
                                                                    )

    print(current_portion_operating_lease_liability_df)
    
    

    """
    Accounts payable
    """
    balance_col_values  = data[data.columns[0]].tolist()
    balance_col_values = [x.lower() for x in balance_col_values]
    accounts_payable_list = database_data["Accounts payable"].values
    accounts_payable_list = [x.lower() for x in accounts_payable_list]
    accounts_payable_col1, accounts_payable_col2 = [], []

    balance_col_values_accounts_payable = [x.lower() for x in balance_col_values_total_current_liabilities]

    for accounts_payable in balance_col_values_accounts_payable:
        if accounts_payable in accounts_payable_list:
            print("Accounts payable value found")
            index_of_accounts_payable = balance_col_values.index(accounts_payable)
            col1_accounts_payable = data[data.columns[1]].values[index_of_accounts_payable]
            print("Column 1 Accounts payable is:", col1_accounts_payable)
            col2_accounts_payable = data[data.columns[2]].values[index_of_accounts_payable]
            print("Column 2 Accounts payable is:", col2_accounts_payable)

            # Checking if the col1 and col2 data types are int, float or not
            if type(col1_accounts_payable) in datatype_filtered_list:
                col1_accounts_payable = col1_accounts_payable
            if type(col2_accounts_payable) in datatype_filtered_list:
                col2_accounts_payable = col2_accounts_payable
            else:
                # Cleaning ($ and ,)
                if "$" in col1_accounts_payable:
                    col1_accounts_payable = col1_accounts_payable.replace("$", "")
                if "$" in col2_accounts_payable:
                    col2_accounts_payable = col2_accounts_payable.replace("$", "")
                if "," in col1_accounts_payable:
                    col1_accounts_payable = col1_accounts_payable.replace(",", "")
                if "," in col2_accounts_payable:
                    col2_accounts_payable = col2_accounts_payable.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_accounts_payable:
                    col1_accounts_payable = col1_accounts_payable.replace("(", "")
                if (")") in col1_accounts_payable:
                    col1_accounts_payable = col1_accounts_payable.replace(")", "")
                if ("(") in col2_accounts_payable:
                    col2_accounts_payable = col2_accounts_payable.replace("(", "")
                if (")") in col2_accounts_payable:
                    col2_accounts_payable = col2_accounts_payable.replace(")", "")
            print("Column 1 Accounts payable after filtration is:", col1_accounts_payable)
            print("Column 2 Accounts payable after filtration is:", col2_accounts_payable)
            accounts_payable_col1.append(float(col1_accounts_payable))
            accounts_payable_col2.append(float(col2_accounts_payable))

    if len(accounts_payable_col1) > 1:
        accounts_payable_col1 = sum(accounts_payable_col1)
    if len(accounts_payable_col2) > 1:
        accounts_payable_col2 = sum(accounts_payable_col2)

    if type(accounts_payable_col1) == list:
        pass
    if type(accounts_payable_col2) == list:
        pass
    if type(accounts_payable_col1) in datatype_filtered_list:
        accounts_payable_col1 = [accounts_payable_col1]
    if type(accounts_payable_col2) in datatype_filtered_list:
        accounts_payable_col2 = [accounts_payable_col2]

    try:
        accounts_payable_df = pd.DataFrame(
                                                {
                                                    data.columns.values[0]: ["Accounts payable"],
                                                    data.columns[1]: accounts_payable_col1,
                                                    data.columns[2]: accounts_payable_col2
                                                }
                                            )
    except Exception as e:
        pass
        accounts_payable_df = pd.DataFrame(
                                                {
                                                    data.columns.values[0]: ["Accounts payable"],
                                                    data.columns[1]: [sum(accounts_payable_col1)],
                                                    data.columns[2]: [sum(accounts_payable_col2)]
                                                }
                                            )

    print(accounts_payable_df)
    
    
    """
    Accrued liabilities
    """
    balance_col_values  = data[data.columns[0]].tolist()
    balance_col_values = [x.lower() for x in balance_col_values]
    accrued_liabilities_list = database_data["Accrued liabilities"].values
    accrued_liabilities_list = [x.lower() for x in accrued_liabilities_list]
    accrued_liabilities_col1, accrued_liabilities_col2 = [], []

    balance_col_values_accrued_liabilities = [x.lower() for x in balance_col_values_total_current_liabilities]

    for accrued_liabilities in balance_col_values_accrued_liabilities:
        if accrued_liabilities in accrued_liabilities_list:
            print("Accrued liabilities value found")
            index_of_accrued_liabilities = balance_col_values.index(accrued_liabilities)
            col1_accrued_liabilities = data[data.columns[1]].values[index_of_accrued_liabilities]
            print("Column 1 Accrued liabilities is:", col1_accrued_liabilities)
            col2_accrued_liabilities = data[data.columns[2]].values[index_of_accrued_liabilities]
            print("Column 2 Accrued liabilities is:", col2_accrued_liabilities)

            # Checking if the col1 and col2 data types are int, float or not
            if type(col1_accrued_liabilities) in datatype_filtered_list:
                col1_accrued_liabilities = col1_accrued_liabilities
            if type(col2_accrued_liabilities) in datatype_filtered_list:
                col2_accrued_liabilities = col2_accrued_liabilities
            else:
                # Cleaning ($ and ,)
                if "$" in col1_accrued_liabilities:
                    col1_accrued_liabilities = col1_accrued_liabilities.replace("$", "")
                if "$" in col2_accrued_liabilities:
                    col2_accrued_liabilities = col2_accrued_liabilities.replace("$", "")
                if "," in col1_accrued_liabilities:
                    col1_accrued_liabilities = col1_accrued_liabilities.replace(",", "")
                if "," in col2_accrued_liabilities:
                    col2_accrued_liabilities = col2_accrued_liabilities.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_accrued_liabilities:
                    col1_accrued_liabilities = col1_accrued_liabilities.replace("(", "")
                if (")") in col1_accrued_liabilities:
                    col1_accrued_liabilities = col1_accrued_liabilities.replace(")", "")
                if ("(") in col2_accrued_liabilities:
                    col2_accrued_liabilities = col2_accrued_liabilities.replace("(", "")
                if (")") in col2_accounts_payable:
                    col2_accrued_liabilities = col2_accrued_liabilities.replace(")", "")

            print("Column 1 Accrued liabilities after filtration is:", col1_accrued_liabilities)
            print("Column 2 Accrued liabilities after filtration is:", col2_accrued_liabilities)
            accrued_liabilities_col1.append(float(col1_accrued_liabilities))
            accrued_liabilities_col2.append(float(col2_accrued_liabilities))

    if len(accrued_liabilities_col1) > 1:
        accrued_liabilities_col1 = sum(accrued_liabilities_col1)
    if len(accrued_liabilities_col2) > 1:
        accrued_liabilities_col2 = sum(accrued_liabilities_col2)

    if type(accrued_liabilities_col1) == list:
        pass
    if type(accrued_liabilities_col2) == list:
        pass
    if type(accrued_liabilities_col1) in datatype_filtered_list:
        accrued_liabilities_col1 = [accrued_liabilities_col1]
    if type(accrued_liabilities_col2) in datatype_filtered_list:
        accrued_liabilities_col2 = [accrued_liabilities_col2]

    try:
        accrued_liabilities_df = pd.DataFrame(
            {
                data.columns.values[0]: ["Accrued liabilities"],
                data.columns[1]: accrued_liabilities_col1,
                data.columns[2]: accrued_liabilities_col2
            }
        )
    except Exception as e:
        pass
        accrued_liabilities_df = pd.DataFrame(
                                                {
                                                    data.columns.values[0]: ["Accrued liabilities"],
                                                    data.columns[1]: [sum(accrued_liabilities_col1)],
                                                    data.columns[2]: [sum(accrued_liabilities_col2)]
                                                }
                                            )
    """
    Income taxes payable
    """
    balance_col_values  = data[data.columns[0]].tolist()
    balance_col_values = [x.lower() for x in balance_col_values]
    income_taxes_payable_list = database_data["Income taxes payable"].values
    income_taxes_payable_list = [x.lower() for x in income_taxes_payable_list]
    income_taxes_payable_col1, income_taxes_payable_col2 = [], []

    balance_col_values_income_taxes_payable = [x.lower() for x in balance_col_values_total_current_liabilities]

    for income_taxes_payable in balance_col_values_income_taxes_payable:
        if income_taxes_payable in income_taxes_payable_list:
            print("Income taxes payable value found")
            index_of_income_taxes_payable = balance_col_values.index(income_taxes_payable)
            col1_income_taxes_payable = data[data.columns[1]].values[index_of_income_taxes_payable]
            print("Column 1 Income taxes payable is:", col1_income_taxes_payable)
            col2_income_taxes_payable = data[data.columns[2]].values[index_of_income_taxes_payable]
            print("Column 2 Income taxes payable is:", col2_income_taxes_payable)

            # Checking if the col1 and col2 data types are int, float or not
            if type(col1_income_taxes_payable) in datatype_filtered_list:
                col1_income_taxes_payable = col1_income_taxes_payable
            if type(col2_income_taxes_payable) in datatype_filtered_list:
                col2_income_taxes_payable = col2_income_taxes_payable
            else:
                # Cleaning ($ and ,)
                if "$" in col1_income_taxes_payable:
                    col1_income_taxes_payable = col1_income_taxes_payable.replace("$", "")
                if "$" in col2_income_taxes_payable:
                    col2_income_taxes_payable = col2_income_taxes_payable.replace("$", "")
                if "," in col1_income_taxes_payable:
                    col1_income_taxes_payable = col1_income_taxes_payable.replace(",", "")
                if "," in col2_income_taxes_payable:
                    col2_income_taxes_payable = col2_income_taxes_payable.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_income_taxes_payable:
                    col1_income_taxes_payable = col1_income_taxes_payable.replace("(", "")
                if (")") in col1_income_taxes_payable:
                    col1_income_taxes_payable = col1_income_taxes_payable.replace(")", "")
                if ("(") in col1_income_taxes_payable:
                    col1_income_taxes_payable = col1_income_taxes_payable.replace("(", "")
                if (")") in col1_income_taxes_payable:
                    col1_income_taxes_payable = col1_income_taxes_payable.replace(")", "")

            print("Column 1 Income taxes payable after filtration is:", col1_income_taxes_payable)
            print("Column 2 Income taxes payable after filtration is:", col2_income_taxes_payable)
            income_taxes_payable_col1.append(float(col1_income_taxes_payable))
            income_taxes_payable_col2.append(float(col2_income_taxes_payable))

    if len(income_taxes_payable_col1) > 1:
        income_taxes_payable_col1 = sum(income_taxes_payable_col1)
    if len(income_taxes_payable_col2) > 1:
        income_taxes_payable_col2 = sum(income_taxes_payable_col2)

    if type(income_taxes_payable_col1) == list:
        pass
    if type(income_taxes_payable_col2) == list:
        pass
    if type(income_taxes_payable_col1) in datatype_filtered_list:
        income_taxes_payable_col1 = [income_taxes_payable_col1]
    if type(income_taxes_payable_col2) in datatype_filtered_list:
        income_taxes_payable_col2 = [income_taxes_payable_col2]

    try:
        income_taxes_payable_df = pd.DataFrame(
                                                    {
                                                        data.columns.values[0]: ["Income taxes payable"],
                                                        data.columns[1]: income_taxes_payable_col1,
                                                        data.columns[2]: income_taxes_payable_col2
                                                    }
                                                )
    except Exception as e:
        pass
        income_taxes_payable_df = pd.DataFrame(
                                                        {
                                                            data.columns.values[0]: ["Income taxes payable"],
                                                            data.columns[1]: [sum(income_taxes_payable_col1)],
                                                            data.columns[2]: [sum(income_taxes_payable_col2)]
                                                        }
                                                )
    
    """
    Deferred income
    """
    balance_col_values  = data[data.columns[0]].tolist()
    balance_col_values = [x.lower() for x in balance_col_values]
    deferred_income_list = database_data["Deferred income"].values
    deferred_income_list = [x.lower() for x in deferred_income_list]
    deferred_income_col1, deferred_income_col2 = [], []

    balance_col_values_deferred_income = [x.lower() for x in balance_col_values_total_current_liabilities]

    for deferred_income in balance_col_values_deferred_income:
        if deferred_income in deferred_income_list:
            print("Deferred income value found")
            index_of_deferred_income = balance_col_values.index(deferred_income)
            col1_deferred_income = data[data.columns[1]].values[index_of_deferred_income]
            print("Column 1 deferred_income is:", col1_deferred_income)
            col2_deferred_income = data[data.columns[2]].values[index_of_deferred_income]
            print("Column 2 deferred_income is:", col2_deferred_income)

            # Checking if the col1 and col2 data types are int, float or not
            if type(col1_deferred_income) in datatype_filtered_list:
                col1_deferred_income = col1_deferred_income
            if type(col2_deferred_income) in datatype_filtered_list:
                col2_deferred_income = col2_deferred_income
            else:
                # Cleaning ($ and ,)
                if "$" in col1_deferred_income:
                    col1_deferred_income = col1_deferred_income.replace("$", "")
                if "$" in col2_deferred_income:
                    col2_deferred_income = col2_deferred_income.replace("$", "")
                if "," in col1_deferred_income:
                    col1_deferred_income = col1_deferred_income.replace(",", "")
                if "," in col2_deferred_income:
                    col2_deferred_income = col2_deferred_income.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_deferred_income:
                    col1_deferred_income = col1_deferred_income.replace("(", "")
                if (")") in col1_deferred_income:
                    col1_deferred_income = col1_deferred_income.replace(")", "")
                if ("(") in col2_deferred_income:
                    col2_deferred_income = col2_deferred_income.replace("(", "")
                if (")") in col2_deferred_income:
                    col2_deferred_income = col2_deferred_income.replace(")", "")

            print("Column 1 deferred_income after filtration is:", col1_deferred_income)
            print("Column 2 deferred_income after filtration is:", col2_deferred_income)
            deferred_income_col1.append(float(col1_deferred_income))
            deferred_income_col2.append(float(col2_deferred_income))

    if len(deferred_income_col1) > 1:
        deferred_income_col1 = sum(deferred_income_col1)
    if len(deferred_income_col2) > 1:
        deferred_income_col2 = sum(deferred_income_col2)

    if type(deferred_income_col1) == list:
        pass
    if type(deferred_income_col2) == list:
        pass
    if type(deferred_income_col1) in datatype_filtered_list:
        deferred_income_col1 = [deferred_income_col1]
    if type(deferred_income_col2) in datatype_filtered_list:
        deferred_income_col2 = [deferred_income_col2]

    try:
        deferred_income_df = pd.DataFrame(
            {
                data.columns.values[0]: ["Deferred income"],
                data.columns[1]: deferred_income_col1,
                data.columns[2]: deferred_income_col2
            }
        )
    except Exception as e:
        pass
        deferred_income_df = pd.DataFrame(
            {
                data.columns.values[0]: ["Deferred income"],
                data.columns[1]: [sum(deferred_income_col1)],
                data.columns[2]: [sum(deferred_income_col2)]
            }
        )

    print(deferred_income_df)

    """
    Contracts payable for programming rights
    """
    balance_col_values  = data[data.columns[0]].tolist()
    balance_col_values = [x.lower() for x in balance_col_values]
    contracts_payable_list = database_data["Contracts payable for programming rights"].values
    contracts_payable_list = [x.lower() for x in contracts_payable_list]
    contracts_payable_col1, contracts_payable_col2 = [], []

    balance_col_values_contracts_payable = [x.lower() for x in balance_col_values_total_current_liabilities]

    for contracts_payable in balance_col_values_contracts_payable:
        if contracts_payable in contracts_payable_list:
            print("Contracts payable for programming rights value found")
            index_of_contracts_payable = balance_col_values.index(contracts_payable)
            col1_contracts_payable = data[data.columns[1]].values[index_of_contracts_payable]
            print("Column 1 Contracts payable for programming rights is:", col1_contracts_payable)
            col2_contracts_payable = data[data.columns[2]].values[index_of_contracts_payable]
            print("Column 2 Contracts payable for programming rights is:", col2_contracts_payable)

            # Checking if the col1 and col2 data types are int, float or not
            if type(col1_contracts_payable) in datatype_filtered_list:
                col1_contracts_payable = col1_contracts_payable
            if type(col2_contracts_payable) in datatype_filtered_list:
                col2_contracts_payable = col2_contracts_payable
            else:
                # Cleaning ($ and ,)
                if "$" in col1_contracts_payable:
                    col1_contracts_payable = col1_contracts_payable.replace("$", "")
                if "$" in col2_contracts_payable:
                    col2_contracts_payable = col2_contracts_payable.replace("$", "")
                if "," in col1_contracts_payable:
                    col1_contracts_payable = col1_contracts_payable.replace(",", "")
                if "," in col2_contracts_payable:
                    col2_contracts_payable = col2_contracts_payable.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_contracts_payable:
                    col1_contracts_payable = col1_contracts_payable.replace("(", "")
                if (")") in col1_contracts_payable:
                    col1_contracts_payable = col1_contracts_payable.replace(")", "")
                if ("(") in col2_contracts_payable:
                    col2_contracts_payable = col2_contracts_payable.replace("(", "")
                if (")") in col2_contracts_payable:
                    col2_contracts_payable = col2_contracts_payable.replace(")", "")

            print("Column 1 Contracts payable for programming rights after filtration is:", col1_contracts_payable)
            print("Column 2 Contracts payable for programming rights after filtration is:", col2_contracts_payable)
            contracts_payable_col1.append(float(col1_contracts_payable))
            contracts_payable_col2.append(float(col2_contracts_payable))

    if len(contracts_payable_col1) > 1:
        contracts_payable_col1 = sum(contracts_payable_col1)
    if len(contracts_payable_col2) > 1:
        contracts_payable_col2 = sum(contracts_payable_col2)

    if type(contracts_payable_col1) == list:
        pass
    if type(contracts_payable_col2) == list:
        pass
    if type(contracts_payable_col1) in datatype_filtered_list:
        contracts_payable_col1 = [contracts_payable_col1]
    if type(contracts_payable_col2) in datatype_filtered_list:
        contracts_payable_col2 = [contracts_payable_col2]

    try:
        contracts_payable_df = pd.DataFrame(
                                                {
                                                    data.columns.values[0]: ["Contracts payable for programming rights"],
                                                    data.columns[1]: contracts_payable_col1,
                                                    data.columns[2]: contracts_payable_col2
                                                }
                                            )
    except Exception as e:
        pass
        contracts_payable_df = pd.DataFrame(
                                                {
                                                    data.columns.values[0]: ["Contracts payable for programming rights"],
                                                    data.columns[1]: [sum(contracts_payable_col1)],
                                                    data.columns[2]: [sum(contracts_payable_col2)]
                                                }
                                            )
    """
    Total current liabilities
    """
    balance_col_values  = data[data.columns[0]].tolist()
    balance_col_values = [x.lower() for x in balance_col_values]
    total_current_liabilities_list = database_data["Total current liabilities"].values
    total_current_liabilities_list = [x.lower() for x in total_current_liabilities_list]
    total_current_liabilities_col1, total_current_liabilities_col2 = [], []

    balance_col_values_total_current_liabilities = [x.lower() for x in balance_col_values_total_current_liabilities]

    for total_current_liabilities in balance_col_values_total_current_liabilities:
        if total_current_liabilities in total_current_liabilities_list:
            print("Total current liabilities value found")
            index_of_total_current_liabilities = balance_col_values.index(total_current_liabilities)
            col1_total_current_liabilities = data[data.columns[1]].values[index_of_total_current_liabilities]
            print("Column 1 Total current liabilities is:", col1_total_current_liabilities)
            col2_total_current_liabilities = data[data.columns[2]].values[index_of_total_current_liabilities]
            print("Column 2 Total current liabilities is:", col2_total_current_liabilities)

            # Checking if the col1 and col2 data types are int, float or not
            if type(col1_total_current_liabilities) in datatype_filtered_list:
                col1_total_current_liabilities = col1_total_current_liabilities
            if type(col2_total_current_liabilities) in datatype_filtered_list:
                col2_total_current_liabilities = col2_total_current_liabilities
            else:
                # Cleaning ($ and ,)
                if "$" in col1_total_current_liabilities:
                    col1_total_current_liabilities = col1_total_current_liabilities.replace("$", "")
                if "$" in col2_total_current_liabilities:
                    col2_total_current_liabilities = col2_total_current_liabilities.replace("$", "")
                if "," in col1_total_current_liabilities:
                    col1_total_current_liabilities = col1_total_current_liabilities.replace(",", "")
                if "," in col2_total_current_liabilities:
                    col2_total_current_liabilities = col2_total_current_liabilities.replace(",", "")
                # Updated Add On (Bracket Filteration )  
                if ("(") in col1_total_current_liabilities:
                    col1_total_current_liabilities = col1_total_current_liabilities.replace("(", "")
                if (")") in col1_total_current_liabilities:
                    col1_total_current_liabilities = col1_total_current_liabilities.replace(")", "")
                if ("(") in col2_total_current_liabilities:
                    col2_total_current_liabilities = col2_total_current_liabilities.replace("(", "")
                if (")") in col2_total_current_liabilities:
                    col2_total_current_liabilities = col2_total_current_liabilities.replace(")", "")
            print("Column 1 Total current liabilities after filtration is:", col1_total_current_liabilities)
            print("Column 2 Total current liabilities after filtration is:", col2_total_current_liabilities)
            total_current_liabilities_col1.append(float(col1_total_current_liabilities))
            total_current_liabilities_col2.append(float(col2_total_current_liabilities))

    if len(total_current_liabilities_col1) > 1:
        total_current_liabilities_col1 = sum(total_current_liabilities_col1)
    if len(total_current_liabilities_col2) > 1:
        total_current_liabilities_col2 = sum(total_current_liabilities_col2)

    if type(total_current_liabilities_col1) == list:
        pass
    if type(total_current_liabilities_col2) == list:
        pass
    if type(total_current_liabilities_col1) in datatype_filtered_list:
        total_current_liabilities_col1 = [total_current_liabilities_col1]
    if type(total_current_liabilities_col2) in datatype_filtered_list:
        total_current_liabilities_col2 = [total_current_liabilities_col2]

    try:
        total_current_liabilities_df = pd.DataFrame(
                                                        {
                                                            data.columns.values[0]: ["Total current liabilities"],
                                                            data.columns[1]: total_current_liabilities_col1,
                                                            data.columns[2]: total_current_liabilities_col2
                                                        }
                                                    )
    except Exception as e:
        pass
        total_current_liabilities_df = pd.DataFrame(
                                                        {
                                                            data.columns.values[0]: ["Total current liabilities"],
                                                            data.columns[1]: [sum(total_current_liabilities_col1)],
                                                            data.columns[2]: [sum(total_current_liabilities_col2)]
                                                        }
                                                    )
    final_data = pd.concat([current_portion_long_term_debt_df,
                            current_portion_operating_lease_liability_df,
                            accounts_payable_df,
                            accrued_liabilities_df,
                            income_taxes_payable_df,
                            deferred_income_df,
                            contracts_payable_df,
                            total_current_liabilities_df], axis =0)
    return final_data


def get_stabnderdized_total_long_term_liabilities_data(database_data,data):
    datatype_filtered_list = [int,float]
    balance_col_values_total_current_liabilities = data[data.columns[0]].values
    balance_col_values = data[data.columns[0]].values
  
    
    """
    Lease Liabilities
    """
    balance_col_values  = data[data.columns[0]].tolist()
    balance_col_values = [x.lower() for x in balance_col_values]
    lease_liabilities_list = database_data["Lease Liabilities"].values
    lease_liabilities_list = [x.lower() for x in lease_liabilities_list]
    lease_liabilities_col1, lease_liabilities_col2 = [], []

    balance_col_values_lease_liabilities = [x.lower() for x in balance_col_values_total_current_liabilities]

    for lease_liabilities in balance_col_values_lease_liabilities:
        if lease_liabilities in lease_liabilities_list:
            print("Lease Liabilities value found")
            index_of_lease_liabilities = balance_col_values.index(lease_liabilities)
            col1_lease_liabilities = data[data.columns[1]].values[index_of_lease_liabilities]
            print("Column 1 Lease Liabilities is:", col1_lease_liabilities)
            col2_lease_liabilities = data[data.columns[2]].values[index_of_lease_liabilities]
            print("Column 2 Lease Liabilities is:", col2_lease_liabilities)

            # Checking if the col1 and col2 data types are int, float or not
            if type(col1_lease_liabilities) in datatype_filtered_list:
                col1_lease_liabilities = col1_lease_liabilities
            if type(col2_lease_liabilities) in datatype_filtered_list:
                col2_lease_liabilities = col2_lease_liabilities
            else:
                # Cleaning ($ and ,)
                if "$" in col1_lease_liabilities:
                    col1_lease_liabilities = col1_lease_liabilities.replace("$", "")
                if "$" in col2_lease_liabilities:
                    col2_lease_liabilities = col2_lease_liabilities.replace("$", "")
                if "," in col1_lease_liabilities:
                    col1_lease_liabilities = col1_lease_liabilities.replace(",", "")
                if "," in col2_lease_liabilities:
                    col2_lease_liabilities = col2_lease_liabilities.replace(",", "")
                if ("(") in col1_lease_liabilities:
                    col1_lease_liabilities = col1_lease_liabilities.replace("(", "")
                if (")") in col1_lease_liabilities:
                    col1_lease_liabilities = col1_lease_liabilities.replace(")", "")
                if ("(") in col2_lease_liabilities:
                    col2_lease_liabilities = col2_lease_liabilities.replace("(", "")
                if (")") in col2_lease_liabilities:
                    col2_lease_liabilities = col2_lease_liabilities.replace(")", "")

            print("Column 1 Lease Liabilities after filtration is:", col1_lease_liabilities)
            print("Column 2 Lease Liabilities after filtration is:", col2_lease_liabilities)
            lease_liabilities_col1.append(float(col1_lease_liabilities))
            lease_liabilities_col2.append(float(col2_lease_liabilities))

    if len(lease_liabilities_col1) > 1:
        lease_liabilities_col1 = sum(lease_liabilities_col1)
    if len(lease_liabilities_col2) > 1:
        lease_liabilities_col2 = sum(lease_liabilities_col2)

    if type(lease_liabilities_col1) == list:
        pass
    if type(lease_liabilities_col2) == list:
        pass
    if type(lease_liabilities_col1) in datatype_filtered_list:
        lease_liabilities_col1 = [lease_liabilities_col1]
    if type(lease_liabilities_col2) in datatype_filtered_list:
        lease_liabilities_col2 = [lease_liabilities_col2]

    try:
        lease_liabilities_df = pd.DataFrame(
            {
                data.columns.values[0]: ["Lease Liabilities"],
                data.columns[1]: lease_liabilities_col1,
                data.columns[2]: lease_liabilities_col2
            }
        )
    except Exception as e:
        pass
        lease_liabilities_df = pd.DataFrame(
            {
                data.columns.values[0]: ["Lease Liabilities"],
                data.columns[1]: [sum(lease_liabilities_col1)],
                data.columns[2]: [sum(lease_liabilities_col2)]
            }
        )

    print(lease_liabilities_df)
    
    """
    Long Term Debt
    """
    balance_col_values  = data[data.columns[0]].tolist()
    balance_col_values = [x.lower() for x in balance_col_values]
    long_term_debt_list = database_data["Long-term debt"].values
    long_term_debt_list = [x.lower() for x in long_term_debt_list]
    long_term_debt_col1, long_term_debt_col2 = [], []

    balance_col_values_long_term_debt = [x.lower() for x in balance_col_values_total_current_liabilities]

    for long_term_debt in balance_col_values_long_term_debt:
        if long_term_debt in long_term_debt_list:
            print("Long Term Debt value found")
            index_of_long_term_debt = balance_col_values.index(long_term_debt)
            col1_long_term_debt = data[data.columns[1]].values[index_of_long_term_debt]
            print("Column 1 Long Term Debt is:", col1_long_term_debt)
            col2_long_term_debt = data[data.columns[2]].values[index_of_long_term_debt]
            print("Column 2 Long Term Debt is:", col2_long_term_debt)

            # Checking if the col1 and col2 data types are int, float or not
            if type(col1_long_term_debt) in datatype_filtered_list:
                col1_long_term_debt = col1_long_term_debt
            if type(col2_long_term_debt) in datatype_filtered_list:
                col2_long_term_debt = col2_long_term_debt
            else:
                # Cleaning ($ and ,)
                if "$" in col1_long_term_debt:
                    col1_long_term_debt = col1_long_term_debt.replace("$", "")
                if "$" in col2_long_term_debt:
                    col2_long_term_debt = col2_long_term_debt.replace("$", "")
                if "," in col1_long_term_debt:
                    col1_long_term_debt = col1_long_term_debt.replace(",", "")
                if "," in col2_long_term_debt:
                    col2_long_term_debt = col2_long_term_debt.replace(",", "")
                if ("(") in col1_long_term_debt:
                    col1_long_term_debt = col1_long_term_debt.replace("(", "")
                if (")") in col1_long_term_debt:
                    col1_long_term_debt = col1_long_term_debt.replace(")", "")
                if ("(") in col2_long_term_debt:
                    col2_long_term_debt = col2_long_term_debt.replace("(", "")
                if (")") in col2_long_term_debt:
                    col2_long_term_debt = col2_long_term_debt.replace(")", "")

            print("Column 1 Long Term Debt after filtration is:", col1_long_term_debt)
            print("Column 2 Long Term Debt after filtration is:", col2_long_term_debt)
            long_term_debt_col1.append(float(col1_long_term_debt))
            long_term_debt_col2.append(float(col2_long_term_debt))

    if len(long_term_debt_col1) > 1:
        long_term_debt_col1 = sum(long_term_debt_col1)
    if len(long_term_debt_col2) > 1:
        long_term_debt_col2 = sum(long_term_debt_col2)

    if type(long_term_debt_col1) == list:
        pass
    if type(long_term_debt_col2) == list:
        pass
    if type(long_term_debt_col1) in datatype_filtered_list:
        long_term_debt_col1 = [long_term_debt_col1]
    if type(long_term_debt_col2) in datatype_filtered_list:
        long_term_debt_col2 = [long_term_debt_col2]

    try:
        long_term_debt_df = pd.DataFrame(
            {
                data.columns.values[0]: ["Long Term Debt"],
                data.columns[1]: long_term_debt_col1,
                data.columns[2]: long_term_debt_col2
            }
        )
    except Exception as e:
        pass
        long_term_debt_df = pd.DataFrame(
            {
                data.columns.values[0]: ["Long Term Debt"],
                data.columns[1]: [sum(long_term_debt_col1)],
                data.columns[2]: [sum(long_term_debt_col2)]
            }
        )
    
    
    """
    Pension liabilities
    """
    balance_col_values  = data[data.columns[0]].tolist()
    balance_col_values = [x.lower() for x in balance_col_values]
    pension_liabilities_list = database_data["Pension liabilities"].values
    pension_liabilities_list = [x.lower() for x in pension_liabilities_list]
    pension_liabilities_col1, pension_liabilities_col2 = [], []

    balance_col_values_pension_liabilities = [x.lower() for x in balance_col_values_total_current_liabilities]

    for pension_liabilities in balance_col_values_pension_liabilities:
        if pension_liabilities in pension_liabilities_list:
            print("Pension Liabilities value found")
            index_of_pension_liabilities = balance_col_values.index(pension_liabilities)
            col1_pension_liabilities = data[data.columns[1]].values[index_of_pension_liabilities]
            print("Column 1 Pension Liabilities is:", col1_pension_liabilities)
            col2_pension_liabilities = data[data.columns[2]].values[index_of_pension_liabilities]
            print("Column 2 Pension Liabilities is:", col2_pension_liabilities)

            # Checking if the col1 and col2 data types are int, float or not
            if type(col1_pension_liabilities) in datatype_filtered_list:
                col1_pension_liabilities = col1_pension_liabilities
            if type(col2_pension_liabilities) in datatype_filtered_list:
                col2_pension_liabilities = col2_pension_liabilities
            else:
                # Cleaning ($ and ,)
                if "$" in col1_pension_liabilities:
                    col1_pension_liabilities = col1_pension_liabilities.replace("$", "")
                if "$" in col2_pension_liabilities:
                    col2_pension_liabilities = col2_pension_liabilities.replace("$", "")
                if "," in col1_pension_liabilities:
                    col1_pension_liabilities = col1_pension_liabilities.replace(",", "")
                if "," in col2_pension_liabilities:
                    col2_pension_liabilities = col2_pension_liabilities.replace(",", "")
                if ("(") in col1_pension_liabilities:
                    col1_pension_liabilities = col1_pension_liabilities.replace("(", "")
                if (")") in col1_pension_liabilities:
                    col1_pension_liabilities = col1_pension_liabilities.replace(")", "")
                if ("(") in col2_pension_liabilities:
                    col2_pension_liabilities = col2_pension_liabilities.replace("(", "")
                if (")") in col2_pension_liabilities:
                    col2_pension_liabilities = col2_pension_liabilities.replace(")", "")

            print("Column 1 Pension Liabilities after filtration is:", col1_pension_liabilities)
            print("Column 2 Pension Liabilities after filtration is:", col2_pension_liabilities)
            pension_liabilities_col1.append(float(col1_pension_liabilities))
            pension_liabilities_col2.append(float(col2_pension_liabilities))

    if len(pension_liabilities_col1) > 1:
        pension_liabilities_col1 = sum(pension_liabilities_col1)
    if len(pension_liabilities_col2) > 1:
        pension_liabilities_col2 = sum(pension_liabilities_col2)

    if type(pension_liabilities_col1) == list:
        pass
    if type(pension_liabilities_col2) == list:
        pass
    if type(pension_liabilities_col1) in datatype_filtered_list:
        pension_liabilities_col1 = [pension_liabilities_col1]
    if type(pension_liabilities_col2) in datatype_filtered_list:
        pension_liabilities_col2 = [pension_liabilities_col2]

    try:
        pension_liabilities_df = pd.DataFrame(
            {
                data.columns.values[0]: ["Pension Liabilities"],
                data.columns[1]: pension_liabilities_col1,
                data.columns[2]: pension_liabilities_col2
            }
        )
    except Exception as e:
        pass
        pension_liabilities_df = pd.DataFrame(
            {
                data.columns.values[0]: ["Pension Liabilities"],
                data.columns[1]: [sum(pension_liabilities_col1)],
                data.columns[2]: [sum(pension_liabilities_col2)]
            }
        )

    print(pension_liabilities)

    
    """
    Deferred income tax liability
    """
    balance_col_values  = data[data.columns[0]].tolist()
    balance_col_values = [x.lower() for x in balance_col_values]
    deferred_income_tax_liability_list = database_data["Deferred income tax liability"].values
    deferred_income_tax_liability_list = [x.lower() for x in deferred_income_tax_liability_list]
    deferred_income_tax_liability_col1, deferred_income_tax_liability_col2 = [], []

    balance_col_values_deferred_income_tax_liability = [x.lower() for x in balance_col_values_total_current_liabilities]

    for deferred_income_tax_liability in balance_col_values_deferred_income_tax_liability:
        if deferred_income_tax_liability in deferred_income_tax_liability_list:
            print("Deferred Income Tax Liability value found")
            index_of_deferred_income_tax_liability = balance_col_values.index(deferred_income_tax_liability)
            col1_deferred_income_tax_liability = data[data.columns[1]].values[index_of_deferred_income_tax_liability]
            print("Column 1 Deferred Income Tax Liability is:", col1_deferred_income_tax_liability)
            col2_deferred_income_tax_liability = data[data.columns[2]].values[index_of_deferred_income_tax_liability]
            print("Column 2 Deferred Income Tax Liability is:", col2_deferred_income_tax_liability)

            # Checking if the col1 and col2 data types are int, float or not
            if type(col1_deferred_income_tax_liability) in datatype_filtered_list:
                col1_deferred_income_tax_liability = col1_deferred_income_tax_liability
            if type(col2_deferred_income_tax_liability) in datatype_filtered_list:
                col2_deferred_income_tax_liability = col2_deferred_income_tax_liability
            else:
                # Cleaning ($ and ,)
                if "$" in col1_deferred_income_tax_liability:
                    col1_deferred_income_tax_liability = col1_deferred_income_tax_liability.replace("$", "")
                if "$" in col2_deferred_income_tax_liability:
                    col2_deferred_income_tax_liability = col2_deferred_income_tax_liability.replace("$", "")
                if "," in col1_deferred_income_tax_liability:
                    col1_deferred_income_tax_liability = col1_deferred_income_tax_liability.replace(",", "")
                if "," in col2_deferred_income_tax_liability:
                    col2_deferred_income_tax_liability = col2_deferred_income_tax_liability.replace(",", "")
                if ("(") in col1_deferred_income_tax_liability:
                    col1_deferred_income_tax_liability = col1_deferred_income_tax_liability.replace("(", "")
                if (")") in col1_deferred_income_tax_liability:
                    col1_deferred_income_tax_liability = col1_deferred_income_tax_liability.replace(")", "")
                if ("(") in col2_deferred_income_tax_liability:
                    col2_deferred_income_tax_liability = col2_deferred_income_tax_liability.replace("(", "")
                if (")") in col2_deferred_income_tax_liability:
                    col2_deferred_income_tax_liability = col2_deferred_income_tax_liability.replace(")", "")
            print("Column 1 Deferred Income Tax Liability after filtration is:", col1_deferred_income_tax_liability)
            print("Column 2 Deferred Income Tax Liability after filtration is:", col2_deferred_income_tax_liability)
            deferred_income_tax_liability_col1.append(float(col1_deferred_income_tax_liability))
            deferred_income_tax_liability_col2.append(float(col2_deferred_income_tax_liability))

    if len(deferred_income_tax_liability_col1) > 1:
        deferred_income_tax_liability_col1 = sum(deferred_income_tax_liability_col1)
    if len(deferred_income_tax_liability_col2) > 1:
        deferred_income_tax_liability_col2 = sum(deferred_income_tax_liability_col2)

    if type(deferred_income_tax_liability_col1) == list:
        pass
    if type(deferred_income_tax_liability_col2) == list:
        pass
    if type(deferred_income_tax_liability_col1) in datatype_filtered_list:
        deferred_income_tax_liability_col1 = [deferred_income_tax_liability_col1]
    if type(deferred_income_tax_liability_col2) in datatype_filtered_list:
        deferred_income_tax_liability_col2 = [deferred_income_tax_liability_col2]

    try:
        deferred_income_tax_liability_df = pd.DataFrame(
            {
                data.columns.values[0]: ["Deferred Income Tax Liability"],
                data.columns[1]: deferred_income_tax_liability_col1,
                data.columns[2]: deferred_income_tax_liability_col2
            }
        )
    except Exception as e:
        pass
        deferred_income_tax_liability_df = pd.DataFrame(
            {
                data.columns.values[0]: ["Deferred Income Tax Liability"],
                data.columns[1]: [sum(deferred_income_tax_liability_col1)],
                data.columns[2]: [sum(deferred_income_tax_liability_col2)]
            }
        )

    print(deferred_income_tax_liability_df)
    
    """
    Long-term tax liabilities
    """
    balance_col_values  = data[data.columns[0]].tolist()
    balance_col_values = [x.lower() for x in balance_col_values]
    long_term_tax_liabilities_list = database_data["Long-term tax liabilities"].values
    long_term_tax_liabilities_list = [x.lower() for x in long_term_tax_liabilities_list]
    long_term_tax_liabilities_col1, long_term_tax_liabilities_col2 = [], []

    balance_col_values_long_term_tax_liabilities = [x.lower() for x in balance_col_values_total_current_liabilities]

    for long_term_tax_liabilities in balance_col_values_long_term_tax_liabilities:
        if long_term_tax_liabilities in long_term_tax_liabilities_list:
            print("Long-term tax liabilities value found")
            index_of_long_term_tax_liabilities = balance_col_values.index(long_term_tax_liabilities)
            col1_long_term_tax_liabilities = data[data.columns[1]].values[index_of_long_term_tax_liabilities]
            print("Column 1 long_term_tax_liabilities is:", col1_long_term_tax_liabilities)
            col2_long_term_tax_liabilities = data[data.columns[2]].values[index_of_long_term_tax_liabilities]
            print("Column 2 long_term_tax_liabilities is:", col2_long_term_tax_liabilities)

            # Checking if the col1 and col2 data types are int, float or not
            if type(col1_long_term_tax_liabilities) in datatype_filtered_list:
                col1_long_term_tax_liabilities = col1_long_term_tax_liabilities
            if type(col2_long_term_tax_liabilities) in datatype_filtered_list:
                col2_long_term_tax_liabilities = col2_long_term_tax_liabilities
            else:
                # Cleaning ($ and ,)
                if "$" in col1_long_term_tax_liabilities:
                    col1_long_term_tax_liabilities = col1_long_term_tax_liabilities.replace("$", "")
                if "$" in col2_long_term_tax_liabilities:
                    col2_long_term_tax_liabilities = col2_long_term_tax_liabilities.replace("$", "")
                if "," in col1_long_term_tax_liabilities:
                    col1_long_term_tax_liabilities = col1_long_term_tax_liabilities.replace(",", "")
                if "," in col2_long_term_tax_liabilities:
                    col2_long_term_tax_liabilities = col2_long_term_tax_liabilities.replace(",", "")
                if ("(") in col1_long_term_tax_liabilities:
                    col1_long_term_tax_liabilities = col1_long_term_tax_liabilities.replace("(", "")
                if (")") in col1_long_term_tax_liabilities:
                    col1_long_term_tax_liabilities = col1_long_term_tax_liabilities.replace(")", "")
                if ("(") in col2_long_term_tax_liabilities:
                    col2_long_term_tax_liabilities = col2_long_term_tax_liabilities.replace("(", "")
                if (")") in col2_long_term_tax_liabilities:
                    col2_long_term_tax_liabilities = col2_long_term_tax_liabilities.replace(")", "")
            print("Column 1 long_term_tax_liabilities after filtration is:", col1_long_term_tax_liabilities)
            print("Column 2 long_term_tax_liabilities after filtration is:", col2_long_term_tax_liabilities)
            long_term_tax_liabilities_col1.append(float(col1_long_term_tax_liabilities))
            long_term_tax_liabilities_col2.append(float(col2_long_term_tax_liabilities))

    if len(long_term_tax_liabilities_col1) > 1:
        long_term_tax_liabilities_col1 = sum(long_term_tax_liabilities_col1)
    if len(long_term_tax_liabilities_col2) > 1:
        long_term_tax_liabilities_col2 = sum(long_term_tax_liabilities_col2)

    if type(long_term_tax_liabilities_col1) == list:
        pass
    if type(long_term_tax_liabilities_col2) == list:
        pass
    if type(long_term_tax_liabilities_col1) in datatype_filtered_list:
        long_term_tax_liabilities_col1 = [long_term_tax_liabilities_col1]
    if type(long_term_tax_liabilities_col2) in datatype_filtered_list:
        long_term_tax_liabilities_col2 = [long_term_tax_liabilities_col2]

    try:
        long_term_tax_liabilities_df = pd.DataFrame(
                                                        {
                                                            data.columns.values[0]: ["Long-term Tax Liabilities"],
                                                            data.columns[1]: long_term_tax_liabilities_col1,
                                                            data.columns[2]: long_term_tax_liabilities_col2
                                                        }
                                                    )
    except Exception as e:
        pass
        long_term_tax_liabilities_df = pd.DataFrame(
                                                        {
                                                            data.columns.values[0]: ["Long-term Tax Liabilities"],
                                                            data.columns[1]: [sum(long_term_tax_liabilities_col1)],
                                                            data.columns[2]: [sum(long_term_tax_liabilities_col2)]
                                                        }
                                                    )

    print(long_term_tax_liabilities_df)
    
    """
    Other noncurrent liabilities
    """
    balance_col_values  = data[data.columns[0]].tolist()
    balance_col_values = [x.lower() for x in balance_col_values]
    other_noncurrent_liabilities_list = database_data["Other noncurrent liabilities"].values
    other_noncurrent_liabilities_list = [x.lower() for x in other_noncurrent_liabilities_list]
    other_noncurrent_liabilities_col1, other_noncurrent_liabilities_col2 = [], []

    balance_col_values_other_noncurrent_liabilities = [x.lower() for x in balance_col_values_total_current_liabilities]

    for other_noncurrent_liability in balance_col_values_other_noncurrent_liabilities:
        if other_noncurrent_liability in other_noncurrent_liabilities_list:
            print("Other noncurrent liability value found")
            index_of_other_noncurrent_liability = balance_col_values.index(other_noncurrent_liability)
            col1_other_noncurrent_liability = data[data.columns[1]].values[index_of_other_noncurrent_liability]
            print("Column 1 Other noncurrent liability is:", col1_other_noncurrent_liability)
            col2_other_noncurrent_liability = data[data.columns[2]].values[index_of_other_noncurrent_liability]
            print("Column 2 Other noncurrent liability is:", col2_other_noncurrent_liability)
            
            # Checking if the col1 and col2 data types are int, float or not
            if type(col1_other_noncurrent_liability) in datatype_filtered_list:
                col1_other_noncurrent_liability = col1_other_noncurrent_liability
            if type(col2_other_noncurrent_liability) in datatype_filtered_list:
                col2_other_noncurrent_liability = col2_other_noncurrent_liability
            else:
                # Cleaning ($ and ,)
                if "$" in col1_other_noncurrent_liability:
                    col1_other_noncurrent_liability = col1_other_noncurrent_liability.replace("$", "")
                if "$" in col2_other_noncurrent_liability:
                    col2_other_noncurrent_liability = col2_other_noncurrent_liability.replace("$", "")
                if "," in col1_other_noncurrent_liability:
                    col1_other_noncurrent_liability = col1_other_noncurrent_liability.replace(",", "")
                if "," in col2_other_noncurrent_liability:
                    col2_other_noncurrent_liability = col2_other_noncurrent_liability.replace(",", "")
                if ("(") in col1_other_noncurrent_liability:
                    col1_other_noncurrent_liability = col1_other_noncurrent_liability.replace("(", "")
                if (")") in col1_other_noncurrent_liability:
                    col1_other_noncurrent_liability = col1_other_noncurrent_liability.replace(")", "")
                if ("(") in col2_other_noncurrent_liability:
                    col2_other_noncurrent_liability = col2_other_noncurrent_liability.replace("(", "")
                if (")") in col2_other_noncurrent_liability:
                    col2_other_noncurrent_liability = col2_other_noncurrent_liability.replace(")", "")
            print("Column 1 Other noncurrent liability after filtration is:", col1_other_noncurrent_liability)
            print("Column 2 Other noncurrent liability after filtration is:", col2_other_noncurrent_liability)
            other_noncurrent_liabilities_col1.append(float(col1_other_noncurrent_liability))
            other_noncurrent_liabilities_col2.append(float(col2_other_noncurrent_liability))

    if len(other_noncurrent_liabilities_col1) > 1:
        other_noncurrent_liabilities_col1 = sum(other_noncurrent_liabilities_col1)
    if len(other_noncurrent_liabilities_col2) > 1:
        other_noncurrent_liabilities_col2 = sum(other_noncurrent_liabilities_col2)

    if type(other_noncurrent_liabilities_col1) == list:
        pass
    if type(other_noncurrent_liabilities_col2) == list:
        pass
    if type(other_noncurrent_liabilities_col1) in datatype_filtered_list:
        other_noncurrent_liabilities_col1 = [other_noncurrent_liabilities_col1]
    if type(other_noncurrent_liabilities_col2) in datatype_filtered_list:
        other_noncurrent_liabilities_col2 = [other_noncurrent_liabilities_col2]

    try:
        other_noncurrent_liabilities_df = pd.DataFrame(
                                                            {
                                                                data.columns.values[0]: ["Other Noncurrent Liabilities"],
                                                                data.columns[1]: other_noncurrent_liabilities_col1,
                                                                data.columns[2]: other_noncurrent_liabilities_col2
                                                            }
                                                        )
    except Exception as e:
        pass
        other_noncurrent_liabilities_df = pd.DataFrame(
                                                            {
                                                                data.columns.values[0]: ["Other Noncurrent Liabilities"],
                                                                data.columns[1]: [sum(other_noncurrent_liabilities_col1)],
                                                                data.columns[2]: [sum(other_noncurrent_liabilities_col2)]
                                                            }
                                                        )

    print(other_noncurrent_liabilities_df)
    
    """
    Total liabilities
    """
    balance_col_values  = data[data.columns[0]].tolist()
    balance_col_values = [x.lower() for x in balance_col_values]
    total_liabilities_list = database_data["Total liabilities"].values
    total_liabilities_list = [x.lower() for x in total_liabilities_list]
    total_liabilities_col1, total_liabilities_col2 = [], []

    balance_col_values_total_liabilities = [x.lower() for x in balance_col_values_total_current_liabilities]

    for total_liability in balance_col_values_total_liabilities:
        if total_liability in total_liabilities_list:
            print("Total liabilities value found")
            index_of_total_liability = balance_col_values.index(total_liability)
            col1_total_liability = data[data.columns[1]].values[index_of_total_liability]
            print("Column 1 total_liability is:", col1_total_liability)
            col2_total_liability = data[data.columns[2]].values[index_of_total_liability]
            print("Column 2 total_liability is:", col2_total_liability)

            # Checking if the col1 and col2 data types are int, float or not
            if type(col1_total_liability) in datatype_filtered_list:
                col1_total_liability = col1_total_liability
            if type(col2_total_liability) in datatype_filtered_list:
                col2_total_liability = col2_total_liability
            else:
                # Cleaning ($ and ,)
                if "$" in col1_total_liability:
                    col1_total_liability = col1_total_liability.replace("$", "")
                if "$" in col2_total_liability:
                    col2_total_liability = col2_total_liability.replace("$", "")
                if "," in col1_total_liability:
                    col1_total_liability = col1_total_liability.replace(",", "")
                if "," in col2_total_liability:
                    col2_total_liability = col2_total_liability.replace(",", "")
                if ("(") in col1_total_liability:
                    col1_total_liability = col1_total_liability.replace("(", "")
                if (")") in col1_total_liability:
                    col1_total_liability = col1_total_liability.replace(")", "")
                if ("(") in col2_total_liability:
                    col2_total_liability = col2_total_liability.replace("(", "")
                if (")") in col2_total_liability:
                    col2_total_liability = col2_total_liability.replace(")", "")
            print("Column 1 total_liability after filtration is:", col1_total_liability)
            print("Column 2 total_liability after filtration is:", col2_total_liability)
            total_liabilities_col1.append(float(col1_total_liability))
            total_liabilities_col2.append(float(col2_total_liability))

    if len(total_liabilities_col1) > 1:
        total_liabilities_col1 = sum(total_liabilities_col1)
    if len(total_liabilities_col2) > 1:
        total_liabilities_col2 = sum(total_liabilities_col2)

    if type(total_liabilities_col1) == list:
        pass
    if type(total_liabilities_col2) == list:
        pass
    if type(total_liabilities_col1) in datatype_filtered_list:
        total_liabilities_col1 = [total_liabilities_col1]
    if type(total_liabilities_col2) in datatype_filtered_list:
        total_liabilities_col2 = [total_liabilities_col2]

    try:
        total_liabilities_df = pd.DataFrame(
            {
                data.columns.values[0]: ["Total Liabilities"],
                data.columns[1]: total_liabilities_col1,
                data.columns[2]: total_liabilities_col2
            }
        )
    except Exception as e:
        pass
        total_liabilities_df = pd.DataFrame(
            {
                data.columns.values[0]: ["Total Liabilities"],
                data.columns[1]: [sum(total_liabilities_col1)],
                data.columns[2]: [sum(total_liabilities_col2)]
            }
        )

    print(total_liabilities_df)
    
    final_data = pd.concat([lease_liabilities_df,
                            long_term_debt_df,
                            pension_liabilities_df,
                            deferred_income_tax_liability_df,
                            long_term_tax_liabilities_df,
                            other_noncurrent_liabilities_df,
                            total_liabilities_df],
                            axis =0)
    return final_data





def get_account_trade_payable_current_liabilitites(current_liabilities_data):
    current_liabilities_data[current_liabilities_data.columns[0]][0] = "Short-term debt"
    current_liabilities_data[current_liabilities_data.columns[0]][1] = "Leases"
    current_liabilities_data[current_liabilities_data.columns[0]][5] = "Deferred Tax or Revenues"
    current_liabilities_data[current_liabilities_data.columns[0]][6] = "Other Current Liabilities"
    current_liabilities_data_accounts_payable = current_liabilities_data[current_liabilities_data[current_liabilities_data.columns[0]]=="Accounts payable"]
    current_liabilities_data_accrued_liabilities = current_liabilities_data[current_liabilities_data[current_liabilities_data.columns[0]]=="Accrued liabilities"]
    current_liabilities_data_income_tax_payable = current_liabilities_data[current_liabilities_data[current_liabilities_data.columns[0]]=="Income taxes payable"]
    current_liabilities_data_accounts_payable_column1 = current_liabilities_data_accounts_payable[current_liabilities_data_accounts_payable.columns[1]].values.tolist()[0]
    current_liabilities_data_accounts_payable_column2 = current_liabilities_data_accounts_payable[current_liabilities_data_accounts_payable.columns[2]].values.tolist()[0]
    current_liabilities_data_accrued_liabilities_column1 = current_liabilities_data_accrued_liabilities[current_liabilities_data_accrued_liabilities.columns[1]].values.tolist()[0]
    current_liabilities_data_accrued_liabilities_column2 = current_liabilities_data_accrued_liabilities[current_liabilities_data_accrued_liabilities.columns[2]].values.tolist()[0]
    current_liabilities_data_income_tax_payable_column1 = current_liabilities_data_income_tax_payable[current_liabilities_data_income_tax_payable.columns[1]].values.tolist()[0]
    current_liabilities_data_income_tax_payable_column2 = current_liabilities_data_income_tax_payable[current_liabilities_data_income_tax_payable.columns[2]].values.tolist()[0]
    accounts_trade_payables_column1 = current_liabilities_data_accounts_payable_column1 + current_liabilities_data_accrued_liabilities_column1 + current_liabilities_data_income_tax_payable_column1
    accounts_trade_payables_column2 = current_liabilities_data_accounts_payable_column2 + current_liabilities_data_accrued_liabilities_column2 + current_liabilities_data_income_tax_payable_column2
    accounts_trade_payables = pd.DataFrame({"Accounts/Trade Payables":[accounts_trade_payables_column1,accounts_trade_payables_column2]}).T
    accounts_trade_payables = accounts_trade_payables.reset_index()
    accounts_trade_payables.columns = current_liabilities_data_accounts_payable.columns
    return accounts_trade_payables



def modify_current_liabilities_data(current_liabilities_data,account_trade_payable_current_liabilitites_data):
    modified_current_liabilities_data = pd.concat([current_liabilities_data,account_trade_payable_current_liabilitites_data], axis = 0)
    modified_current_liabilities_data = modified_current_liabilities_data.drop([2,3,4])
    modified_current_liabilities_data["idx"] = [0,1,2,3,4,5]
    modified_current_liabilities_data = modified_current_liabilities_data.set_index(["idx"])
    total_current_liabilities = modified_current_liabilities_data.loc[4]
    account_trade = modified_current_liabilities_data.loc[5]
    modified_current_liabilities_data.loc[4] = account_trade
    modified_current_liabilities_data.loc[5] =   total_current_liabilities
    return modified_current_liabilities_data





def get_final_standerdised_current_liabilities(modified_current_liabilities_data):
    """_summary_

    Args:
        modified_current_liabilities_data: This is the modified data after calculating the current liabilities as per pavaki standerds
    Returns:
        _final_total_current_liability_data: This is the error check and total current liabilities in code level calculation
    """
    total_current_liabilitites_column1 = sum([modified_current_liabilities_data.iloc[0][1],
                                                modified_current_liabilities_data.iloc[1][1],
                                                modified_current_liabilities_data.iloc[2][1],
                                                modified_current_liabilities_data.iloc[3][1],
                                                modified_current_liabilities_data.iloc[4][1]])
    total_current_liabilitites_column2 = sum([modified_current_liabilities_data.iloc[0][2],
                                                modified_current_liabilities_data.iloc[1][2],
                                                modified_current_liabilities_data.iloc[2][2],
                                                modified_current_liabilities_data.iloc[3][2],
                                                modified_current_liabilities_data.iloc[4][2]])

    error_total_current_liabilitites_column1 =  total_current_liabilitites_column1 - modified_current_liabilities_data.iloc[5][1]
    error_total_current_liabilitites_column2 = total_current_liabilitites_column2 - modified_current_liabilities_data.iloc[5][2]

    calulated_total_current_liability = pd.DataFrame({"Total Current Liabilities (Calculated)":[total_current_liabilitites_column1,total_current_liabilitites_column2],
                                                    "Total Current Liabilities (Error)":[error_total_current_liabilitites_column1 ,error_total_current_liabilitites_column2]}).T


    calulated_total_current_liability = calulated_total_current_liability.reset_index()


    calulated_total_current_liability.columns  = modified_current_liabilities_data.columns
    final_total_current_liability_data = pd.concat([modified_current_liabilities_data,calulated_total_current_liability], axis = 0)

    print(final_total_current_liability_data)
    return final_total_current_liability_data


def get_redeemable_noncontrolling_interest(database_data,data):
    balance_col_values  = data[data.columns[0]].tolist()
    balance_col_values = [x.lower() for x in balance_col_values]
    database_data["Redeemable noncontrolling interest"] = database_data["Redeemable noncontrolling interest"].fillna(database_data["Redeemable noncontrolling interest"].mode()[0])
    redeemable_noncontrolling_interest_list = database_data["Redeemable noncontrolling interest"].values
    print("redeemable_noncontrolling_interest_list", redeemable_noncontrolling_interest_list)
    redeemable_noncontrolling_interest_list = [x.lower() for x in redeemable_noncontrolling_interest_list]
    redeemable_noncontrolling_interest_col1, redeemable_noncontrolling_interest_col2 = [], []

    balance_col_values_total_liabilities = [x.lower() for x in balance_col_values]
    datatype_filtered_list = [int,float]
    for redeemable_noncontrolling_interest in balance_col_values_total_liabilities:
        if redeemable_noncontrolling_interest in redeemable_noncontrolling_interest_list:
            print("redeemable_noncontrolling_interest value found")
            index_of_redeemable_noncontrolling_interest = balance_col_values.index(redeemable_noncontrolling_interest)
            col1_redeemable_noncontrolling_interest = data[data.columns[1]].values[redeemable_noncontrolling_interest]
            print("Column 1 redeemable_noncontrolling_interest is:", col1_redeemable_noncontrolling_interest)
            col2_redeemable_noncontrolling_interest = data[data.columns[2]].values[index_of_redeemable_noncontrolling_interest]
            print("Column 2 redeemable_noncontrolling_interest is:", col2_redeemable_noncontrolling_interest)

            # Checking if the col1 and col2 data types are int, float or not
            if type(col1_redeemable_noncontrolling_interest) in datatype_filtered_list:
                col1_redeemable_noncontrolling_interest = col1_redeemable_noncontrolling_interest
            if type(col2_redeemable_noncontrolling_interest) in datatype_filtered_list:
                col2_redeemable_noncontrolling_interest = col2_redeemable_noncontrolling_interest
            else:
                # Cleaning ($ and ,)
                if "$" in col1_redeemable_noncontrolling_interest:
                    col1_redeemable_noncontrolling_interest = col1_redeemable_noncontrolling_interest.replace("$", "")
                if "$" in col2_redeemable_noncontrolling_interest:
                    col2_redeemable_noncontrolling_interest = col2_redeemable_noncontrolling_interest.replace("$", "")
                if "," in col1_redeemable_noncontrolling_interest:
                    col1_redeemable_noncontrolling_interest = col1_redeemable_noncontrolling_interest.replace(",", "")
                if "," in col2_redeemable_noncontrolling_interest:
                    col2_redeemable_noncontrolling_interest = col2_redeemable_noncontrolling_interest.replace(",", "")
                if ("(") in col1_redeemable_noncontrolling_interest:
                    col1_redeemable_noncontrolling_interest = col1_redeemable_noncontrolling_interest.replace("(", "")
                if (")") in col1_redeemable_noncontrolling_interest:
                    col1_redeemable_noncontrolling_interest = col1_redeemable_noncontrolling_interest.replace(")", "")
                if ("(") in col2_redeemable_noncontrolling_interest:
                    col2_redeemable_noncontrolling_interest = col1_redeemable_noncontrolling_interest.replace("(", "")
                if (")") in col2_redeemable_noncontrolling_interest:
                    col2_redeemable_noncontrolling_interest = col2_redeemable_noncontrolling_interest.replace(")", "")
            print("Column 1 redeemable_noncontrolling_interest after filtration is:", col1_redeemable_noncontrolling_interest)
            print("Column 2 redeemable_noncontrolling_interest after filtration is:", col2_redeemable_noncontrolling_interest)
            redeemable_noncontrolling_interest_col1.append(float(col1_redeemable_noncontrolling_interest))
            redeemable_noncontrolling_interest_col2.append(float(col2_redeemable_noncontrolling_interest))

    if len(redeemable_noncontrolling_interest_col1) > 1:
        redeemable_noncontrolling_interest_col1 = sum(redeemable_noncontrolling_interest_col1)
    if len(redeemable_noncontrolling_interest_col2) > 1:
        redeemable_noncontrolling_interest_col2 = sum(redeemable_noncontrolling_interest_col2)

    if type(redeemable_noncontrolling_interest_col1) == list:
        pass
    if type(redeemable_noncontrolling_interest_col2) == list:
        pass
    if type(redeemable_noncontrolling_interest_col1) in datatype_filtered_list:
        redeemable_noncontrolling_interest_col1 = [redeemable_noncontrolling_interest_col1]
    if type(redeemable_noncontrolling_interest_col2) in datatype_filtered_list:
        redeemable_noncontrolling_interest_col2 = [redeemable_noncontrolling_interest_col2]

    try:
        redeemable_noncontrolling_interest_df = pd.DataFrame(
            {
                data.columns.values[0]: ["Redeemable noncontrolling interest"],
                data.columns[1]: redeemable_noncontrolling_interest_col1,
                data.columns[2]: redeemable_noncontrolling_interest_col2
            }
        )
    except Exception as e:
        pass
        redeemable_noncontrolling_interest_df = pd.DataFrame(
                                                                {
                                                                    data.columns.values[0]: ["Redeemable noncontrolling interest"],
                                                                    data.columns[1]: [sum(redeemable_noncontrolling_interest_col1)],
                                                                    data.columns[2]: [sum(redeemable_noncontrolling_interest_col1)]
                                                                }
                                                            )

    print(redeemable_noncontrolling_interest_df)
    
    return redeemable_noncontrolling_interest_df 


def get_total_equity(database_data,data):
    balance_col_values  = data[data.columns[0]].tolist()
    balance_col_values = [x.lower() for x in balance_col_values]
    # Filling Null values
    database_data["Total equity"].fillna(database_data["Total equity"].mode()[0], inplace = True)
    total_equity_list = database_data["Total equity"].values
    print("total_equity_list is :", total_equity_list)
    total_equity_list = [x.lower() for x in total_equity_list]
    total_equity_col1, total_equity_col2 = [], []

    balance_col_values_total_equity = [x.lower() for x in balance_col_values]
    datatype_filtered_list = [int,float]
    for total_equity in balance_col_values_total_equity:
        if total_equity in total_equity_list:
            print("total_equity value found")
            index_of_total_equity = balance_col_values.index(total_equity)
            col1_total_equity = data[data.columns[1]].values[index_of_total_equity]
            print("Column 1 total_equity is:", col1_total_equity)
            col2_total_equity = data[data.columns[2]].values[index_of_total_equity]
            print("Column 2 total_equity is:", col2_total_equity)

            # Checking if the col1 and col2 data types are int, float or not
            if type(col1_total_equity) in datatype_filtered_list:
                col1_total_equity = col1_total_equity
            if type(col2_total_equity) in datatype_filtered_list:
                col2_total_equity = col2_total_equity
            else:
                # Cleaning ($ and ,)
                if "$" in col1_total_equity:
                    col1_total_equity = col1_total_equity.replace("$", "")
                if "$" in col2_total_equity:
                    col2_total_equity = col2_total_equity.replace("$", "")
                if "," in col1_total_equity:
                    col1_total_equity = col1_total_equity.replace(",", "")
                if "," in col2_total_equity:
                    col2_total_equity = col2_total_equity.replace(",", "")
                if ("(") in col1_total_equity:
                    col1_total_equity = col1_total_equity.replace("(", "")
                if (")") in col1_total_equity:
                    col1_total_equity = col1_total_equity.replace(")", "")
                if ("(") in col2_total_equity:
                    col2_total_equity = col2_total_equity.replace("(", "")
                if (")") in col2_total_equity:
                    col2_total_equity = col2_total_equity.replace(")", "")

            print("Column 1 total_equity after filtration is:", col1_total_equity)
            print("Column 2 total_equity after filtration is:", col2_total_equity)
            total_equity_col1.append(float(col1_total_equity))
            total_equity_col2.append(float(col2_total_equity))

    if len(total_equity_col1) > 1:
        total_equity_col1 = sum(total_equity_col1)
    if len(total_equity_col2) > 1:
        total_equity_col2 = sum(total_equity_col2)

    if type(total_equity_col1) == list:
        pass
    if type(total_equity_col2) == list:
        pass
    if type(total_equity_col1) in datatype_filtered_list:
        total_equity_col1 = [total_equity_col1]
    if type(total_equity_col2) in datatype_filtered_list:
        total_equity_col2 = [total_equity_col2]

    try:
        total_equity_df = pd.DataFrame(
            {
                data.columns.values[0]: ["Total equity"],
                data.columns[1]: total_equity_col1,
                data.columns[2]: total_equity_col2
            }
        )
    except Exception as e:
        pass
        total_equity_df = pd.DataFrame(
                                            {
                                                data.columns.values[0]: ["Total equity"],
                                                data.columns[1]: [sum(total_equity_col1)],
                                                data.columns[2]: [sum(total_equity_col1)]
                                            }
                                        )

    print(total_equity_df)
    
    return total_equity_df 


def get_total_liabilities_redeemable_noncontrolling_interest_and_equity(database_data,data):
    balance_col_values  = data[data.columns[0]].tolist()
    balance_col_values = [x.lower() for x in balance_col_values]
    database_data["Total liabilities, redeemable noncontrolling interest and equity"].fillna(database_data["Total liabilities, redeemable noncontrolling interest and equity"].mode()[0], inplace = True)
    total_liabilities_redeemable_noncontrolling_interest_and_equity_list = database_data["Total liabilities, redeemable noncontrolling interest and equity"].values
    total_liabilities_redeemable_noncontrolling_interest_and_equity_list = [x.lower() for x in total_liabilities_redeemable_noncontrolling_interest_and_equity_list]
    total_liabilities_redeemable_noncontrolling_interest_and_equity_col1, total_liabilities_redeemable_noncontrolling_interest_and_equity_col2 = [], []

    balance_col_values_total_liabilities_redeemable_noncontrolling_interest_and_equity = [x.lower() for x in balance_col_values]
    datatype_filtered_list = [int,float]
    for total_liabilities_redeemable_noncontrolling_interest_and_equity in balance_col_values_total_liabilities_redeemable_noncontrolling_interest_and_equity:
        if total_liabilities_redeemable_noncontrolling_interest_and_equity in total_liabilities_redeemable_noncontrolling_interest_and_equity_list:
            print("total_liabilities_redeemable_noncontrolling_interest_and_equity value found")
            index_of_total_liabilities_redeemable_noncontrolling_interest_and_equity = balance_col_values.index(total_liabilities_redeemable_noncontrolling_interest_and_equity)
            col1_total_liabilities_redeemable_noncontrolling_interest_and_equity = data[data.columns[1]].values[index_of_total_liabilities_redeemable_noncontrolling_interest_and_equity]
            print("Column 1 total_liabilities_redeemable_noncontrolling_interest_and_equity is:", col1_total_liabilities_redeemable_noncontrolling_interest_and_equity)
            col2_total_liabilities_redeemable_noncontrolling_interest_and_equity = data[data.columns[2]].values[index_of_total_liabilities_redeemable_noncontrolling_interest_and_equity]
            print("Column 2 total_liabilities_redeemable_noncontrolling_interest_and_equity is:", col2_total_liabilities_redeemable_noncontrolling_interest_and_equity)

            # Checking if the col1 and col2 data types are int, float or not
            if type(col1_total_liabilities_redeemable_noncontrolling_interest_and_equity) in datatype_filtered_list:
                col1_total_liabilities_redeemable_noncontrolling_interest_and_equity = col1_total_liabilities_redeemable_noncontrolling_interest_and_equity
            if type(col2_total_liabilities_redeemable_noncontrolling_interest_and_equity) in datatype_filtered_list:
                col2_total_liabilities_redeemable_noncontrolling_interest_and_equity = col2_total_liabilities_redeemable_noncontrolling_interest_and_equity
            else:
                # Cleaning ($ and ,)
                if "$" in col1_total_liabilities_redeemable_noncontrolling_interest_and_equity:
                    col1_total_liabilities_redeemable_noncontrolling_interest_and_equity = col1_total_liabilities_redeemable_noncontrolling_interest_and_equity.replace("$", "")
                if "$" in col2_total_liabilities_redeemable_noncontrolling_interest_and_equity:
                    col2_total_liabilities_redeemable_noncontrolling_interest_and_equity = col2_total_liabilities_redeemable_noncontrolling_interest_and_equity.replace("$", "")
                if "," in col1_total_liabilities_redeemable_noncontrolling_interest_and_equity:
                    col1_total_liabilities_redeemable_noncontrolling_interest_and_equity = col1_total_liabilities_redeemable_noncontrolling_interest_and_equity.replace(",", "")
                if "," in col2_total_liabilities_redeemable_noncontrolling_interest_and_equity:
                    col2_total_liabilities_redeemable_noncontrolling_interest_and_equity = col2_total_liabilities_redeemable_noncontrolling_interest_and_equity.replace(",", "")
                if ("(") in col1_total_liabilities_redeemable_noncontrolling_interest_and_equity:
                    col1_total_liabilities_redeemable_noncontrolling_interest_and_equity = col1_total_liabilities_redeemable_noncontrolling_interest_and_equity.replace("(", "")
                if (")") in col1_total_liabilities_redeemable_noncontrolling_interest_and_equity:
                    col1_total_liabilities_redeemable_noncontrolling_interest_and_equity = col1_total_liabilities_redeemable_noncontrolling_interest_and_equity.replace(")", "")
                if ("(") in col2_total_liabilities_redeemable_noncontrolling_interest_and_equity:
                    col2_total_liabilities_redeemable_noncontrolling_interest_and_equity = col2_total_liabilities_redeemable_noncontrolling_interest_and_equity.replace("(", "")
                if (")") in col2_total_liabilities_redeemable_noncontrolling_interest_and_equity:
                    col2_total_liabilities_redeemable_noncontrolling_interest_and_equity = col2_total_liabilities_redeemable_noncontrolling_interest_and_equity.replace(")", "")
                if (" ") in col1_total_liabilities_redeemable_noncontrolling_interest_and_equity:
                    col1_total_liabilities_redeemable_noncontrolling_interest_and_equity = col1_total_liabilities_redeemable_noncontrolling_interest_and_equity.replace(" ", "")
                if (" ") in col2_total_liabilities_redeemable_noncontrolling_interest_and_equity:
                    col2_total_liabilities_redeemable_noncontrolling_interest_and_equity = col2_total_liabilities_redeemable_noncontrolling_interest_and_equity.replace(" ", "")
            print("Column 1 total_liabilities_redeemable_noncontrolling_interest_and_equity after filtration is:", col1_total_liabilities_redeemable_noncontrolling_interest_and_equity)
            print("Column 2 total_liabilities_redeemable_noncontrolling_interest_and_equity after filtration is:", col2_total_liabilities_redeemable_noncontrolling_interest_and_equity)
            total_liabilities_redeemable_noncontrolling_interest_and_equity_col1.append(float(col1_total_liabilities_redeemable_noncontrolling_interest_and_equity))
            total_liabilities_redeemable_noncontrolling_interest_and_equity_col2.append(float(col2_total_liabilities_redeemable_noncontrolling_interest_and_equity))

    if len(total_liabilities_redeemable_noncontrolling_interest_and_equity_col1) > 1:
        total_liabilities_redeemable_noncontrolling_interest_and_equity_col1 = sum(total_liabilities_redeemable_noncontrolling_interest_and_equity_col1)
    if len(total_liabilities_redeemable_noncontrolling_interest_and_equity_col2) > 1:
        total_liabilities_redeemable_noncontrolling_interest_and_equity_col2 = sum(total_liabilities_redeemable_noncontrolling_interest_and_equity_col2)

    if type(total_liabilities_redeemable_noncontrolling_interest_and_equity_col1) == list:
        pass
    if type(total_liabilities_redeemable_noncontrolling_interest_and_equity_col2) == list:
        pass
    if type(total_liabilities_redeemable_noncontrolling_interest_and_equity_col1) in datatype_filtered_list:
        total_liabilities_redeemable_noncontrolling_interest_and_equity_col1 = [total_liabilities_redeemable_noncontrolling_interest_and_equity_col1]
    if type(total_liabilities_redeemable_noncontrolling_interest_and_equity_col2) in datatype_filtered_list:
        total_liabilities_redeemable_noncontrolling_interest_and_equity_col2 = [total_liabilities_redeemable_noncontrolling_interest_and_equity_col2]

    try:
        total_liabilities_redeemable_noncontrolling_interest_and_equity_df = pd.DataFrame(
                                                                                            {
                                                                                                data.columns.values[0]: ["Total liabilities, redeemable noncontrolling interest and equity"],
                                                                                                data.columns[1]: total_liabilities_redeemable_noncontrolling_interest_and_equity_col1,
                                                                                                data.columns[2]: total_liabilities_redeemable_noncontrolling_interest_and_equity_col2
                                                                                            }
                                                                                        )
    except Exception as e:
        pass
        total_liabilities_redeemable_noncontrolling_interest_and_equity_df = pd.DataFrame(
                                                                                            {
                                                                                                data.columns.values[0]: ["Total liabilities, redeemable noncontrolling interest and equity"],
                                                                                                data.columns[1]: [sum(total_liabilities_redeemable_noncontrolling_interest_and_equity_col1)],
                                                                                                data.columns[2]: [sum(total_liabilities_redeemable_noncontrolling_interest_and_equity_col1)]
                                                                                            }
                                                                                        )

    print(total_liabilities_redeemable_noncontrolling_interest_and_equity_df)
    
    return total_liabilities_redeemable_noncontrolling_interest_and_equity_df



def get_total_liabilities_calculated_and_error_from_std_data(data):
    col1_total_liabilities_calculated = data.iloc[32][1] + data.iloc[33][1] + data.iloc[34][1]+data.iloc[35][1] + data.iloc[36][1] + data.iloc[37][1] + data.iloc[38][1] + data.iloc[39][1]
    col2_total_liabilities_calculated = data.iloc[32][2] + data.iloc[33][2] + data.iloc[34][2]+data.iloc[35][2] + data.iloc[36][2] + data.iloc[37][2] + data.iloc[38][2] + data.iloc[39][2]
    error_total_liabilities_col1 = data.iloc[40][1] - col1_total_liabilities_calculated
    error_total_liabilities_col2 = data.iloc[40][2] - col2_total_liabilities_calculated
    total_liabilities_redeemable_noncontrolling_interest_and_equity_col1_calculated = col1_total_liabilities_calculated + data.iloc[41][1] + data.iloc[42][1]
    total_liabilities_redeemable_noncontrolling_interest_and_equity_col2_calculated = col2_total_liabilities_calculated + data.iloc[41][2] + data.iloc[42][2]
    error_final_col1  = total_liabilities_redeemable_noncontrolling_interest_and_equity_col1_calculated - data.iloc[43][1]
    error_final_col2 = total_liabilities_redeemable_noncontrolling_interest_and_equity_col2_calculated - data.iloc[43][2]
    total_liabilities_calculated_df = pd.DataFrame(
                                                {
                                                    data.columns.values[0]: ["Total Liabilities (Calculated)"],
                                                    data.columns[1]: [col1_total_liabilities_calculated],
                                                    data.columns[2]: [col2_total_liabilities_calculated]
                                                }
                                            )


    total_liabilities_calculated_error_df = pd.DataFrame(
                                                    {
                                                        data.columns.values[0]: ["Total Liabilities (Error)"],
                                                        data.columns[1]: [error_total_liabilities_col1],
                                                        data.columns[2]: [error_total_liabilities_col2]
                                                    }
                                                )
    total_liabilities_redeemable_noncontrolling_interest_and_equity_col1_calculated_error_df = pd.DataFrame(
                                                                                                        {
                                                                                                            data.columns.values[0]: ["Total liabilities, redeemable noncontrolling interest and equity(Error)"],
                                                                                                            data.columns[1]: [error_final_col1],
                                                                                                            data.columns[2]: [error_final_col2]
                                                                                                        }
                                                                                                    )
    return total_liabilities_calculated_df, total_liabilities_calculated_error_df,total_liabilities_redeemable_noncontrolling_interest_and_equity_col1_calculated_error_df



def get_complete_standerdized_data_given_ticker_name(ticker_name):
    standerdised_ticker_data_full_and_final = 0
    balance_sheet_data_to_return = 0
    tickers_list = [ticker_name]
    cik = 0
    for ticker in tickers_list:
        cik,ticker,company_name = return_ticker_data(ticker)
        # If the length of cik is less than 10 then filter the number of zeros that needs to be added and form the new cik number
        if len(cik) < 10:
            cik_buffer = 10 - len(cik)
            cik = "0"*cik_buffer +cik
        else:
            cik = cik
        print(cik)
        try:
            asset_data = get_asset_data_given_cik(cik)
            asset_data["filed"].max()
            balance_sheet_data_10q = asset_data[asset_data["form"] == "10-Q"] 
            balance_sheet_data_10k = asset_data[asset_data["form"] == "10-K"]
            balance_sheet_data = pd.concat([balance_sheet_data_10q,balance_sheet_data_10k], axis =0)
            latest_datapoint = balance_sheet_data[balance_sheet_data["filed"] == balance_sheet_data["filed"].max()]
            filtered_accn = filter_accn(latest_datapoint["accn"])
            # balance_sheet_col = scrape_latest_balance_sheet_and_standerdize_columns(filtered_accn)
            balance_sheet_data = scrape_latest_balance_sheet_and_standerdize_columns(filtered_accn,balance_sheet_corpus,cik)
            # balance_sheet_data.to_csv("AMD_actual_bs_data.csv")
            balance_sheet_data_to_return = balance_sheet_data
            current_assets_standerdised_data = get_stabnderdized_total_current_assets_data(database_data=database_data,
                                                                                            data=balance_sheet_data)
            
            
            long_term_assets_standerdized_data = get_stabnderdized_total_long_term_assets_data(database_data=database_data,
                                                                                            data = balance_sheet_data)
            print("Current Assets Standerdization Successful....................\n")
            print("Current Assets data is........")
            print(current_assets_standerdised_data)
            print("Total Long Term Standerdization Successful....................\n")
            print("Long Term Assets data is........")
            print(long_term_assets_standerdized_data)
            try:
                total_assets_data = get_total_assets_data(current_assets_standerdised_data=current_assets_standerdised_data,
                                                        long_term_assets_standerdized_data=long_term_assets_standerdized_data,
                                                        data=balance_sheet_data,
                                                        database_data=database_data)
                print("total_assets_data is........")
                print(total_assets_data)
            except Exception as  e:
                print("Total Asset Calculation ran into exception :", e)
                pass
            
            
            
            current_liabilities_data = get_stabnderdized_total_current_liabilities_data(database_data=database_data,data=balance_sheet_data)
            print("current_liabilities_data is........")
            print(current_liabilities_data)
            
            try:
                current_liabilities_data = current_liabilities_data.reset_index()
                current_liabilities_data.drop(["index"], axis =1 , inplace = True)
                if "level_0" in current_liabilities_data.columns:
                    current_liabilities_data.drop(["level_0"], axis =1 , inplace = True)


                account_trade_payable_current_liabilitites_data = get_account_trade_payable_current_liabilitites(current_liabilities_data)
                modified_current_liabilities_data =  modify_current_liabilities_data(current_liabilities_data,account_trade_payable_current_liabilitites_data)
                # Overwriting the current_liabilities_data to save memory.
                current_liabilities_data = get_final_standerdised_current_liabilities(modified_current_liabilities_data)
                print("current_liabilities_data:", current_liabilities_data)
            except Exception as e:
                print("Standerdization Error from Current Liabilities Calculation..")
                print("Exception is :",e)
            
            try:
                long_term_liabilities_data = get_stabnderdized_total_long_term_liabilities_data(database_data=database_data,data=balance_sheet_data)
                print("Long Term Liabilities......")
                print(long_term_liabilities_data)
            except Exception as e:
                pass
                print("Long Term Liabilities Standerdization Error", e)
            try:    
                redeemable_noncontrolling_interest = get_redeemable_noncontrolling_interest(database_data=database_data,data=balance_sheet_data)
                print("redeemable_noncontrolling_interest",redeemable_noncontrolling_interest)
            except Exception as e:
                pass
                print("redeemable_noncontrolling_interest Error", e)
            try:
                total_equity = get_total_equity(database_data=database_data,
                                                data=balance_sheet_data)
                print("total_equity : ", total_equity)
            except Exception as e:
                pass
                print("total_equity Exception: ", e)
            
            try:
                total_liabilities_redeemable_noncontrolling_interest_and_equity = get_total_liabilities_redeemable_noncontrolling_interest_and_equity(database_data=database_data,data=balance_sheet_data)
                print("total_liabilities_redeemable_noncontrolling_interest_and_equity:", total_liabilities_redeemable_noncontrolling_interest_and_equity)
            except Exception as e:
                pass
                print("total_liabilities_redeemable_noncontrolling_interest_and_equity Exception..",e)
            
            # Exception Handling Before Standerdization.......    
            try:
                print("Total Assets :", total_assets_data)
            except Exception as e:
                print("Total Assets Ran into an Exception ", e)
                total_assets_data = pd.DataFrame(
                                        {
                                            balance_sheet_data.columns.values[0]: ["Total Assets (Ran into Error)"],
                                            balance_sheet_data.columns[1]: [0],
                                            balance_sheet_data.columns[2]: [0]
                                        }
                                    )
            
            # Exception Handling Before Standerdization.......    
            try:
                print("current_liabilities_data :", current_liabilities_data)
            except Exception as e:
                print("current_liabilities_data Ran into an Exception ", e)
                current_liabilities_data = pd.DataFrame(
                                        {
                                            balance_sheet_data.columns.values[0]: ["Current Liabilities (Ran into Error)"],
                                            balance_sheet_data.columns[1]: [0],
                                            balance_sheet_data.columns[2]: [0]
                                        }
                                    )
                
                
            # Exception Handling Before Standerdization.......    
            try:
                print("long_term_liabilities_data :", long_term_liabilities_data)
            except Exception as e:
                print("long_term_liabilities_data Ran into an Exception ", e)
                long_term_liabilities_data = pd.DataFrame(
                                        {
                                            balance_sheet_data.columns.values[0]: ["Long Term Liabilities (Ran into Error)"],
                                            balance_sheet_data.columns[1]: [0],
                                            balance_sheet_data.columns[2]: [0]
                                        }
                                    )
            
            # Exception Handling Before Standerdization.......    
            try:
                print("redeemable_noncontrolling_interest :", redeemable_noncontrolling_interest)
            except Exception as e:
                print("redeemable_noncontrolling_interest Ran into an Exception ", e)
                redeemable_noncontrolling_interest = pd.DataFrame(
                                        {
                                            balance_sheet_data.columns.values[0]: ["redeemable_noncontrolling_interest (Ran into Error)"],
                                            balance_sheet_data.columns[1]: [0],
                                            balance_sheet_data.columns[2]: [0]
                                        }
                                    )
            
            
             # Exception Handling Before Standerdization.......    
            try:
                print("total_equity :", total_equity)
            except Exception as e:
                print("total_equity Ran into an Exception ", e)
                total_equity = pd.DataFrame(
                                        {
                                            balance_sheet_data.columns.values[0]: ["Total Equity (Ran into Error)"],
                                            balance_sheet_data.columns[1]: [0],
                                            balance_sheet_data.columns[2]: [0]
                                        }
                                    )
                
            # Exception Handling Before Standerdization.......    
            try:
                print("total_liabilities_redeemable_noncontrolling_interest_and_equity :", total_liabilities_redeemable_noncontrolling_interest_and_equity)
            except Exception as e:
                print("total_liabilities_redeemable_noncontrolling_interest_and_equity Ran into an Exception ", e)
                total_liabilities_redeemable_noncontrolling_interest_and_equity = pd.DataFrame(
                                        {
                                            balance_sheet_data.columns.values[0]: ["total_liabilities_redeemable_noncontrolling_interest_and_equity (Ran into Error)"],
                                            balance_sheet_data.columns[1]: [0],
                                            balance_sheet_data.columns[2]: [0]
                                        }
                                    )
            standerdised_ticker_data = pd.concat([total_assets_data,
                                                current_liabilities_data,
                                                long_term_liabilities_data,
                                                redeemable_noncontrolling_interest,
                                                total_equity,
                                                total_liabilities_redeemable_noncontrolling_interest_and_equity],
                                                axis =0)
            print("standerdised_ticker_data : ", standerdised_ticker_data)
            # try:
            #     standerdised_ticker_data = standerdised_ticker_data.reset_index()
            #     standerdised_ticker_data.drop(["index"], axis = 1, inplace = True)
            # except:
            #     pass
            try:
                total_liabilities_calculated_df, total_liabilities_calculated_error_df,total_liabilities_redeemable_noncontrolling_interest_and_equity_calculated_error_df = get_total_liabilities_calculated_and_error_from_std_data(standerdised_ticker_data)
            except Exception as e:
                print("Error happened at Total Liabilities Error Calculation......")
                pass
                
            
            
            standerdised_ticker_data_full_and_final = pd.concat([total_assets_data,
                                                                current_liabilities_data,
                                                                long_term_liabilities_data,
                                                                total_liabilities_calculated_df,
                                                                total_liabilities_calculated_error_df,
                                                                redeemable_noncontrolling_interest,
                                                                total_equity,
                                                                total_liabilities_redeemable_noncontrolling_interest_and_equity,
                                                                total_liabilities_redeemable_noncontrolling_interest_and_equity_calculated_error_df],
                                                                axis =0)
            
            print("standerdised_ticker_data.......")
            print(standerdised_ticker_data_full_and_final)
            
            
            
            # standerdised_ticker_data_full_and_final.to_csv(ticker+"_latest_standerdised_full_and_final.csv",index = False)
                
            
            """
            Genarating a CSV File for testing.....
            """
            # standerdised_ticker_data.to_csv(ticker+"_latest_standerdised.csv",index = False)
        
        except Exception as e:
            pass
        
    return standerdised_ticker_data_full_and_final,balance_sheet_data_to_return







footer="""<style>
a:link , a:visited{
color: blue;
background-color: transparent;
text-decoration: underline;
}

a:hover,  a:active {
color: red;
background-color: transparent;
text-decoration: underline;
}

.footer {
position: fixed;
left: 0;
bottom: 0;
width: 100%;
background-color: white;
color: black;
text-align: center;
}
</style>
<div class="footer">
<p>©℗Pavaki Capital. All rights reserved <a style='display: block; color: #00008B ;text-align: center;' href="https://www.insaid.co/" target="_blank">INSAID</a></p>
</div>
"""
col1,col2,col3,col4,col5,col6 = st.columns(6)
with col6:
    st.image('./logo.jpg', width = 200)
# with col2:
with col1:
    st.title('Valuations')
ticker_name = st.text_input('Enter Ticker Name: ', key="name")
# st.write("The Entered Ticker name is :",ticker_name)
# print(st.session_state.name)
# ticker_name = input("Enter Ticker Name :")
# standerdised_ticker_data_full_and_final,balance_sheet_data_to_return = get_complete_standerdized_data_given_ticker_name(ticker_name)

if "button_clicked" not in st.session_state:
    st.session_state.button_clicked = False

def callback():
    st.session_state.button_clicked = True


def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')

standerdised_ticker_data_full_and_final = pd.DataFrame({"Key1":[0]})
balance_sheet_data_to_return_original = pd.DataFrame({"Key2":[1]})
extraction_flag = 0
# flag= 0
if (st.button(label = f'Download {ticker_name} Data from SEC and Perform Standerdization',
            on_click=callback) or st.session_state.button_clicked):
    
    with st.spinner('Downlaoading In Progress ......'):
        standerdised_ticker_data_full_and_final,balance_sheet_data_to_return_original = get_complete_standerdized_data_given_ticker_name(ticker_name)
        st.success('Done!')
        extraction_flag = 1

if extraction_flag == 1:
    st.header("Balance Sheet Data:")
    st.dataframe(balance_sheet_data_to_return_original,hide_index = True)
    st.header("Standerdized Balance Sheet Data:")
    try:
        st.dataframe(standerdised_ticker_data_full_and_final,hide_index = True)
    except Exception as e:
        print(f"St.DataFrame Ran into an Exceotion................. {e}")
        print("*"*50)
        print("Logging.................")
        print(standerdised_ticker_data_full_and_final)
        print("*"*50)
        try:
            st.table(standerdised_ticker_data_full_and_final)
        except Exception as e:
            print("Streamlit Table Ran into an Exception.............")
            print(e)
            print("*"*50)
            print("Logging.................")
            print(standerdised_ticker_data_full_and_final)
            print("*"*50)        
    csv_original = convert_df(balance_sheet_data_to_return_original)
    csv_standerdized = convert_df(standerdised_ticker_data_full_and_final)

    st.download_button(
                            label="Download Original Balance Sheet Data",
                            data=csv_original,
                            file_name=f'{ticker_name}_latest_original.csv',
                            mime='text/csv',
                        )

    st.download_button(
                            label="Download Standerdized Balance Sheet Data",
                            data=csv_standerdized,
                            file_name=f'{ticker_name}_latest_standerdised.csv',
                            mime='text/csv',
                        )
        #time.sleep(100)
    
    # if st.button(label =f'Get {ticker_name} Actual Data', on_click=callback):
    #     print("Buttomn is Active")
    #     st.write('Fetching Original Balance Sheet ......')
    #     print("Spinner Time...")
    #     with st.spinner('Wait for it...'):
    #         time.sleep(5)
    #         st.success('Done!')
    #         print("Original Balance Sheet Data ........")
    #         print(balance_sheet_data_to_return)
    #         st.dataframe(balance_sheet_data_to_return)

    # if st.button(label =f'Get {ticker_name} Standerdized Data', on_click=callback):
    #     st.write('Fetching Standerdized Balance Sheet....')
    #     with st.spinner('Wait for it...'):
    #         time.sleep(5)
    #         st.success('Done!')
    #         print("Standerdized Balance Sheet Data ........")
    #         print(standerdised_ticker_data_full_and_final)
    #         st.dataframe(standerdised_ticker_data_full_and_final)
    
    



################################# BREAKDOWN BEGINS HERE ###############################

    

# balance_sheet_data_to_return ---> original BS Data



sheet_id_database_data = '1O04GWbcZztoIadx1KK2CQ8X58ZqPQcRqpv4ck4Yxz54'
sheet_name_database_data = 'sheet2'
url_database_data = f'https://docs.google.com/spreadsheets/d/{sheet_id_database_data}/gviz/tq?tqx=out:csv&sheet={sheet_name_database_data}'
database_data = pd.read_csv(url_database_data)

sheet_id_select_reject = '1eripQyKChiD0co-xrl22mFV6MCwlSJzm7m7DscIubOI'
sheet_name_select_reject = 'data_columns'
url_select_reject = f'https://docs.google.com/spreadsheets/d/{sheet_id_select_reject}/gviz/tq?tqx=out:csv&sheet={sheet_name_select_reject}'
data = pd.read_csv(url_select_reject)


selected_data = data[data["Select/Reject"] == "select"]
selected_data = data[data["Select/Reject"] == "select"]
balance_sheet_corpus = selected_data["data_columns"]
reject_list = data[data["Select/Reject"] == "reject"]

# st.text("BREAKDOWN LINE ITEMS DEMO VIEW :")


# reference_bs_data = balance_sheet_data_to_return_original
reference_bs_data = pd.read_csv("./original_data_folder/data.csv")

# reference_bs_data =  balance_sheet_data
# if "Unnamed: 0" in reference_bs_data.columns:
reference_bs_data.drop(["Unnamed: 0"],axis =1 , inplace = True)


# from data_standerdization_dropdown.frontend_finalized_view import *
def get_cash_and_cash_equivalents_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values  = data[data.columns[0]].tolist()
    balance_col_values = [x.lower() for x in balance_col_values]
    
    
    # balance_col_values = reference_bs_data[reference_bs_data.columns.values[0]].values
    # print("balance_col_values: ", balance_col_values)
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]
    
    
    # Getting all of the cash list
    database_data["Cash and cash equivalents"].fillna("Cash and cash equivalents", inplace= True)
    cash_list = database_data["Cash and cash equivalents"].values
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))
    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data) 
    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_marketable_secrities_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    # balance_col_values = data[data.columns.values[0]].values
    balance_col_values  = data[data.columns[0]].tolist()
    balance_col_values = [x.lower() for x in balance_col_values]
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]
    
    
    # Getting all of the cash list
    database_data["Marketable securities"].fillna("Marketable securities", inplace= True)
    cash_list = database_data["Marketable securities"].values
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))
    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = int(col1_cash_data[0])/2
    #     tmp2 = int(col1_cash_data[0])/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)
    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_rights_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    database_data["Rights-of-use relating to leases"].fillna("Rights-of-use relating to leases", inplace= True)
    cash_list = database_data["Rights-of-use relating to leases"].values
    
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))
    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = int(col1_cash_data[0])/2
    #     tmp2 = int(col1_cash_data[0])/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
        print("investments_and_other_assets_list_column1 :", col1_cash_data)
        print("investments_and_other_assets_list_column2 :", col1_cash_data)
    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_inventories_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Inventories"].values
    database_data["Inventories"].fillna("Inventories", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))
    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)
    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_accounts_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Accounts receivable, net of allowances"].values
    database_data["Accounts receivable, net of allowances"].fillna("Accounts receivable, net of allowances", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))
    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)
    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_other_recievables_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Other receivables"].values
    database_data["Other receivables"].fillna("Other receivables", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))
    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)
    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_tax_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Tax"].values
    database_data["Tax"].fillna("Tax", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))
    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)
    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_prepaid_expences_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Prepaid expenses and other current assets"].values
    database_data["Prepaid expenses and other current assets"].fillna("Prepaid expenses and other current assets", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)
    
    
    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_prepaid_expences_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Right-of-use assets for operating leases"].values
    database_data["Right-of-use assets for operating leases"].fillna("Right-of-use assets for operating leases", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    
    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)
    
    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_net_properties_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Net property and equipment"].values
    database_data["Net property and equipment"].fillna("Net property and equipment", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    
    
    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)
    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_real_estate_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Real Estate Assets"].values
    database_data["Real Estate Assets"].fillna("Real Estate Assets", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))
            
            
    #if len(exact_cash_line_item_name_list) > 1:
        # exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
        # tmp1 = col1_cash_data[0]/2
        # tmp2 = col1_cash_data[0]/2
        # col1_cash_data = []
        # col1_cash_data = []
        # col1_cash_data.append(tmp1)
        # col1_cash_data.append(tmp2)
        # print("investments_and_other_assets_list_column1 :", col1_cash_data)
        # print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_investment_assets_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Investments and other assets"].values
    database_data["Investments and other assets"].fillna("Investments and other assets", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))
            
            
    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_investment_in_other_companies_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Investment in Other Companies"].values
    database_data["Investment in Other Companies"].fillna("Investment in Other Companies", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))
            
            
    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_pentions_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Pensions Assets"].values
    database_data["Pensions Assets"].fillna("Pensions Assets", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))
            
            
    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_goodwill_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Goodwill"].values
    database_data["Goodwill"].fillna("Goodwill", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))
            
            
    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_Indefinite_lived_and_amortizable_intangible_assets_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Indefinite-lived and amortizable intangible assets"].values
    database_data["Indefinite-lived and amortizable intangible assets"].fillna("Indefinite-lived and amortizable intangible assets", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))


    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_deferred_income_taxes_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Deferred income taxes"].values
    database_data["Deferred income taxes"].fillna("Deferred income taxes", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))


    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_total_intangible_and_other_assets_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Total intangible and other assets"].values
    database_data["Total intangible and other assets"].fillna("Total intangible and other assets", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))


    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_assets_for_discontinued_business_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Assets for Discontinued Business"].values
    database_data["Assets for Discontinued Business"].fillna("Assets for Discontinued Business", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))


    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_current_portion_of_long_term_debt_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Current portion of long-term debt"].values
    database_data["Current portion of long-term debt"].fillna("Current portion of long-term debt", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))


    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_current_portion_of_operating_lease_liability_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Current portion of operating lease liability"].values
    database_data["Current portion of operating lease liability"].fillna("Current portion of operating lease liability", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))


    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_accounts_payable_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Accounts payable"].values
    database_data["Accounts payable"].fillna("Accounts payable", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))


    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_accrued_liabilities_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Accrued liabilities"].values
    database_data["Accrued liabilities"].fillna("Accrued liabilities", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))


    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_income_taxes_payable_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Income taxes payable"].values
    database_data["Income taxes payable"].fillna("Income taxes payable", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(int(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(int(col2_cash_data))


    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_deferred_income_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Deferred income"].values
    database_data["Deferred income"].fillna("Deferred income", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))


    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_total_intangible_and_other_assets_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Total intangible and other assets"].values
    database_data["Total intangible and other assets"].fillna("Total intangible and other assets", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))


    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_assets_for_discontinued_business_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Assets for Discontinued Business"].values
    database_data["Assets for Discontinued Business"].fillna("Assets for Discontinued Business", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))


    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_contracts_payable_for_programming_rights_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Contracts payable for programming rights"].values
    database_data["Contracts payable for programming rights"].fillna("Contracts payable for programming rights", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))


    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_total_current_liabilities_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Total current liabilities"].values
    database_data["Total current liabilities"].fillna("Total current liabilities", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))


    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_lease_liabilities_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Lease Liabilities"].values
    database_data["Lease Liabilities"].fillna("Lease Liabilities", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))


    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_long_term_debt_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Long-term debt"].values
    database_data["Long-term debt"].fillna("Long-term debt", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))


    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_pension_liabilities_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Pension liabilities"].values
    database_data["Pension liabilities"].fillna("Pension liabilities", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))


    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_deferred_income_tax_liability_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Deferred income tax liability"].values
    database_data["Deferred income tax liability"].fillna("Deferred income tax liability", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))


    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_long_term_tax_liabilities_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Long-term tax liabilities"].values
    database_data["Long-term tax liabilities"].fillna("Long-term tax liabilities", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))


    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_other_noncurrent_liabilities_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Other noncurrent liabilities"].values
    database_data["Other noncurrent liabilities"].fillna("Other noncurrent liabilities", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))


    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_total_liabilities_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Total liabilities"].values
    database_data["Total liabilities"].fillna("Total liabilities", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))


    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_redeemable_noncontrolling_interest_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Redeemable noncontrolling interest"].values
    database_data["Redeemable noncontrolling interest"].fillna("Redeemable noncontrolling interest", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))


    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_total_equity_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Total equity"].values
    database_data["Total equity"].fillna("Total equity", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))


    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df

def get_total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int,float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]


    # Getting all of the cash list
    cash_list = database_data["Total liabilities, redeemable noncontrolling interest and equity"].values
    database_data["Total liabilities, redeemable noncontrolling interest and equity"].fillna("Total liabilities, redeemable noncontrolling interest and equity", inplace= True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :",col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :",col2_cash_data)
            # Checking if the col1 and col2 datatypes are int, float or not
            if type(col1_cash_data) in datatype_filtered_list:
                col1_cash_data = col1_cash_data
            if type(col2_cash_data) in datatype_filtered_list:
                col2_cash_data = col2_cash_data
            else:
                # Cleaning ($ and ,)
                if ("$") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace("$", "")
                if ("$") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace("$", "")
                if (",") in col1_cash_data:
                    col1_cash_data = col1_cash_data.replace(",", "")
                if (",") in col2_cash_data:
                    col2_cash_data = col2_cash_data.replace(",", "")
            print("Column 1 cash Data after filteration is :",col1_cash_data)
            print("Column 2 cash Data after filteration is :",col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))


    # if len(exact_cash_line_item_name_list) > 1:
    #     exact_cash_line_item_name_list=exact_cash_line_item_name_list[0]
    #     tmp1 = col1_cash_data[0]/2
    #     tmp2 = col1_cash_data[0]/2
    #     col1_cash_data = []
    #     col1_cash_data = []
    #     col1_cash_data.append(tmp1)
    #     col1_cash_data.append(tmp2)
    #     print("investments_and_other_assets_list_column1 :", col1_cash_data)
    #     print("investments_and_other_assets_list_column2 :", col1_cash_data)

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame({data.columns.values[0]:exact_cash_line_item_name_list,
                            data.columns[1]: col1_cash_data,
                            data.columns[2]:col2_cash_data
                            })
    return cash_df




# Current Assets
cash_data_breakdown = get_cash_and_cash_equivalents_breakdown(reference_bs_data, database_data)
market_data_breakdown = get_marketable_secrities_data_breakdown(reference_bs_data, database_data)
rights_data_breakdown  =  get_rights_data_breakdown(reference_bs_data, database_data)
inventories_data_breakdown  = get_inventories_data_breakdown(reference_bs_data, database_data)
accounts_data_breakdown  = get_accounts_data_breakdown(reference_bs_data, database_data)
other_recievables_data_breakdown = get_other_recievables_data_breakdown(reference_bs_data, database_data)
tax_data_breakdown = get_tax_data_breakdown(reference_bs_data, database_data)
prepaid_expences_data_breakdown = get_prepaid_expences_data_breakdown(reference_bs_data, database_data)

# # Long Term Assets
net_properties_data_breakdown = get_net_properties_data_breakdown(reference_bs_data, database_data)
real_estate_data_breakdown = get_real_estate_data_breakdown(reference_bs_data, database_data)
investment_assets_data_breakdown = get_investment_assets_data_breakdown(reference_bs_data, database_data)
investment_in_other_companies_data_breakdown = get_investment_in_other_companies_data_breakdown(reference_bs_data, database_data)
pentions_data_breakdown = get_pentions_data_breakdown(reference_bs_data, database_data)
goodwill_data_breakdown = get_goodwill_data_breakdown(reference_bs_data, database_data)
Indefinite_lived_and_amortizable_intangible_assets_data_breakdown =get_Indefinite_lived_and_amortizable_intangible_assets_data_breakdown(reference_bs_data, database_data)
deferred_income_taxes_data_breakdown = get_deferred_income_taxes_data_breakdown(reference_bs_data, database_data)
total_intangible_and_other_assets_data_breakdown = get_total_intangible_and_other_assets_data_breakdown(reference_bs_data, database_data)
assets_for_discontinued_business_data_breakdown = get_assets_for_discontinued_business_data_breakdown(reference_bs_data, database_data)



# Liabilities:
current_portion_of_long_term_debt_data_breakdown = get_current_portion_of_long_term_debt_data_breakdown(reference_bs_data, database_data)
current_portion_of_operating_lease_liability_breakdown = get_current_portion_of_operating_lease_liability_breakdown(reference_bs_data, database_data)
accounts_payable_data_breakdown = get_accounts_payable_breakdown(reference_bs_data, database_data)
accrued_liabilities_breakdown = get_accrued_liabilities_breakdown(reference_bs_data, database_data)
income_taxes_payable_breakdown = get_income_taxes_payable_breakdown(reference_bs_data, database_data)
deferred_income_data_breakdown = get_deferred_income_data_breakdown(reference_bs_data, database_data)
total_intangible_and_other_assets_data_breakdown = get_total_intangible_and_other_assets_data_breakdown(reference_bs_data, database_data)
assets_for_discontinued_business_data_breakdown = get_assets_for_discontinued_business_data_breakdown(reference_bs_data, database_data)
contracts_payable_for_programming_rights_data_breakdown = get_contracts_payable_for_programming_rights_data_breakdown(reference_bs_data, database_data)
total_current_liabilities_data_breakdown = get_total_current_liabilities_data_breakdown(reference_bs_data, database_data)
lease_liabilities_data_breakdown = get_lease_liabilities_data_breakdown(reference_bs_data, database_data)
long_term_debt_data_breakdown = get_long_term_debt_data_breakdown(reference_bs_data, database_data)
pension_liabilities_data_breakdown = get_pension_liabilities_data_breakdown(reference_bs_data, database_data)
deferred_income_tax_liability_data_breakdown = get_deferred_income_tax_liability_data_breakdown(reference_bs_data, database_data)
long_term_tax_liabilities_data_breakdown = get_long_term_tax_liabilities_data_breakdown(reference_bs_data, database_data)
other_noncurrent_liabilities_data_breakdown = get_other_noncurrent_liabilities_data_breakdown(reference_bs_data, database_data)
total_liabilities_data_breakdown = get_total_liabilities_data_breakdown(reference_bs_data, database_data)
redeemable_noncontrolling_interest_data_breakdown = get_redeemable_noncontrolling_interest_data_breakdown(reference_bs_data, database_data)
total_equity_data_breakdown = get_total_equity_data_breakdown(reference_bs_data, database_data)
total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown = get_total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown(reference_bs_data, database_data)


################### MERGING BREAKDOWNS ########################################
cash_and_market_data_breakdown = pd.concat([cash_data_breakdown,
                                            market_data_breakdown], axis = 0)
cash_and_market_data_breakdown = cash_and_market_data_breakdown.reset_index()
cash_and_market_data_breakdown.drop(["index"], axis = 1, inplace = True)
cash_and_market_data_breakdown.drop_duplicates(inplace = True)


current_assets_breakdown = pd.concat([cash_data_breakdown,
                            market_data_breakdown,
                            rights_data_breakdown,
                            inventories_data_breakdown,
                            accounts_data_breakdown,
                            other_recievables_data_breakdown,
                            tax_data_breakdown,
                            prepaid_expences_data_breakdown], axis = 0)
current_assets_breakdown = current_assets_breakdown.reset_index()
current_assets_breakdown.drop(["index"], axis = 1, inplace = True)
current_assets_breakdown.drop_duplicates(inplace = True)


long_term_assets_breakdown = pd.concat([net_properties_data_breakdown,
                              real_estate_data_breakdown,
                              investment_assets_data_breakdown,
                              investment_in_other_companies_data_breakdown,
                              pentions_data_breakdown,
                              goodwill_data_breakdown], axis = 0)


long_term_assets_breakdown = long_term_assets_breakdown.reset_index()
long_term_assets_breakdown.drop(["index"], axis = 1, inplace = True)
long_term_assets_breakdown.drop_duplicates(inplace = True)

long_term_liabilities_breakdown = pd.concat([current_portion_of_long_term_debt_data_breakdown,
                                current_portion_of_operating_lease_liability_breakdown,
                                accounts_payable_data_breakdown,
                                accrued_liabilities_breakdown,
                                income_taxes_payable_breakdown,
                                deferred_income_data_breakdown,
                                total_intangible_and_other_assets_data_breakdown,
                                assets_for_discontinued_business_data_breakdown,
                                contracts_payable_for_programming_rights_data_breakdown,
                                total_current_liabilities_data_breakdown,
                                lease_liabilities_data_breakdown,
                                long_term_debt_data_breakdown,
                                pension_liabilities_data_breakdown,
                                deferred_income_tax_liability_data_breakdown,
                                long_term_tax_liabilities_data_breakdown,
                                other_noncurrent_liabilities_data_breakdown,
                                total_liabilities_data_breakdown,
                                redeemable_noncontrolling_interest_data_breakdown,
                                total_equity_data_breakdown,
                                total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown
                                ], axis = 0)


long_term_liabilities_breakdown= long_term_liabilities_breakdown.reset_index()
long_term_liabilities_breakdown.drop(["index"], axis = 1, inplace = True)
long_term_liabilities_breakdown.drop_duplicates(inplace = True)



#current_assets_standerdized_for_breakdown = get_stabnderdized_total_current_assets_data(database_data,reference_bs_data)

# st.markdown(
#     '''
#     <style>
#     .streamlit-expanderHeader {
#         background-color: red;
#         color: black; # Adjust this for expander header color
#     }
#     .streamlit-expanderContent {
#         background-color: white;
#         color: black; # Expander content color
#     }
#     </style>
#     ''',
#     unsafe_allow_html=True
# )

tab_titles = ['Current Assets', 'Long Term Assets', 'Current Liabilities', 'Long Term Liabilities']
tab1, tab2, tab3, tab4 = st.tabs(tab_titles)


with tab1:
    st.header(":blue[CURRENT ASSETS BREAKDOWN VIEW]")
    st.markdown(
        '''
        <style>
        .streamlit-expanderHeader {
            background-color: white;
            color: black; # Adjust this for expander header color
        }
        .streamlit-expanderContent {
            background-color: white;
            color: black; # Expander content color
        }
        </style>
        ''',
        unsafe_allow_html=True
    )
    # st.header('Topic A')
    # st.write('Topic A content')
    # col1, col2 = st.columns([0.95, 0.4])
    # with col1:
    with tab1:
        with st.expander("Current Assets Breakdown"):
            st.dataframe(standerdised_ticker_data_full_and_final.iloc[0:11,:].to_dict())
        
        with st.sidebar:
            with st.expander("Cash And Marketable Securities Breakdown"):
                st.dataframe(cash_and_market_data_breakdown.to_dict())
                
        accounts_trade_receivables_breakdown_data =pd.concat([accounts_data_breakdown,
                                                                other_recievables_data_breakdown], axis = 0)
        accounts_trade_receivables_breakdown_data = accounts_trade_receivables_breakdown_data.reset_index()
        accounts_trade_receivables_breakdown_data.drop(["index"], axis = 1, inplace = True)
        accounts_trade_receivables_breakdown_data.drop_duplicates(inplace = True)
            
        with st.sidebar:
            with st.expander("Accounts/Trade Receivables Breakdown"):
                st.dataframe(accounts_trade_receivables_breakdown_data.to_dict())
        
        other_current_assets_breakdown = pd.concat([prepaid_expences_data_breakdown], axis = 0)
        other_current_assets_breakdown = other_current_assets_breakdown.reset_index()
        other_current_assets_breakdown.drop(["index"], axis = 1, inplace = True)
        other_current_assets_breakdown.drop_duplicates(inplace = True)
            
        with st.sidebar:
            with st.expander("Other Current Assets"):
                st.dataframe(other_current_assets_breakdown.to_dict())
        
with tab2:
    st.header(":blue[LONG TERM ASSETS BREAKDOWN VIEW]")
    with st.expander("Long Term Assets Breakdown"):
        st.dataframe(standerdised_ticker_data_full_and_final.iloc[11:23,:].to_dict())

    intangable_assets_breakdown = pd.concat([goodwill_data_breakdown,
                                            Indefinite_lived_and_amortizable_intangible_assets_data_breakdown],
                                            axis = 0)
    intangable_assets_breakdown = intangable_assets_breakdown.reset_index()
    intangable_assets_breakdown.drop(["index"], axis = 1, inplace = True)
    intangable_assets_breakdown.drop_duplicates(inplace = True)
    with st.sidebar:
        with st.expander("Intangible Assets	Breakdown"):
            st.dataframe(intangable_assets_breakdown.to_dict())
            

        other_assets_breakdown_view = pd.concat([deferred_income_taxes_data_breakdown,
                                                total_intangible_and_other_assets_data_breakdown],
                                                axis = 0)
        other_assets_breakdown_view = other_assets_breakdown_view.reset_index()
        other_assets_breakdown_view.drop(["index"], axis = 1, inplace = True)
        other_assets_breakdown_view.drop_duplicates(inplace = True)
        with st.expander("Other Current Assets Breakdown"):
            st.dataframe(other_assets_breakdown_view.to_dict())


    
    

# with st.expander("Current Assets Breakdown"):
#     st.dataframe(standerdised_ticker_data_full_and_final.iloc[0:11,:].to_dict())

#     st.markdown(
#     '''
#     <style>
#     .streamlit-expanderHeader {
#         background-color: yellow;
#         color: black; # Adjust this for expander header color
#     }
#     .streamlit-expanderContent {
#         background-color: white;
#         color: black; # Expander content color
#     }
#     </style>
#     ''',
#     unsafe_allow_html=True
# )
    


# with col2:
    
    
with tab3:
    current_liabilities_breakdown_view = standerdised_ticker_data_full_and_final.iloc[27:34,:]
    current_liabilities_breakdown_view = current_liabilities_breakdown_view.reset_index()
    current_liabilities_breakdown_view.drop(["index"], axis = 1, inplace = True)
    st.header(":blue[CURRENT LIABILITIES BREAKDOWN VIEW]")
    with st.expander("CURRENT Liabilities Breakdown"):
        st.dataframe(current_liabilities_breakdown_view.to_dict())
        
    standerdised_ticker_data_full_and_final = standerdised_ticker_data_full_and_final.reset_index()
    standerdised_ticker_data_full_and_final.drop(["index"], axis = 1, inplace = True)


with tab4: 
    st.header(":blue[LONG TERM LIABILITIES BREAKDOWN VIEW]")
    with st.expander("LONG TERM Liabilities Breakdown"):
        st.dataframe(standerdised_ticker_data_full_and_final.iloc[34:40,:].to_dict())

    other_noncurrent_liabilities_breakdown_view = pd.concat([long_term_tax_liabilities_data_breakdown,
                                                            other_noncurrent_liabilities_data_breakdown],
                                            axis = 0)
    other_noncurrent_liabilities_breakdown_view = other_noncurrent_liabilities_breakdown_view.reset_index()
    other_noncurrent_liabilities_breakdown_view.drop(["index"], axis = 1, inplace = True)
    other_noncurrent_liabilities_breakdown_view.drop_duplicates(inplace = True)
    with st.sidebar:
        with st.expander("Other Noncurrent Liabilities Breakdown"):
            st.dataframe(other_noncurrent_liabilities_breakdown_view.to_dict(),  width=700, height=400)


# with st.expander("Long Term Assets Breakdown"):
#     st.dataframe(standerdised_ticker_data_full_and_final.iloc[11:23,:].to_dict())

# with st.expander("Cash And Marketable Securities Breakdown"):
#     st.dataframe(cash_and_market_data_breakdown.to_dict())


# accounts_trade_receivables_breakdown_data =pd.concat([
#                             accounts_data_breakdown,
#                             other_recievables_data_breakdown], axis = 0)
# accounts_trade_receivables_breakdown_data = accounts_trade_receivables_breakdown_data.reset_index()
# accounts_trade_receivables_breakdown_data.drop(["index"], axis = 1, inplace = True)
# accounts_trade_receivables_breakdown_data.drop_duplicates(inplace = True)
# with st.expander("Accounts/Trade Receivables Breakdown"):
#     st.dataframe(accounts_trade_receivables_breakdown_data.to_dict())
    
# # Other Current Assets BREAKDOWN VIEW with live line items:
# other_current_assets_breakdown = pd.concat([prepaid_expences_data_breakdown], axis = 0)
# other_current_assets_breakdown = other_current_assets_breakdown.reset_index()
# other_current_assets_breakdown.drop(["index"], axis = 1, inplace = True)
# other_current_assets_breakdown.drop_duplicates(inplace = True)
# with st.expander("Other Current Assets"):
#     st.dataframe(other_current_assets_breakdown.to_dict())
 	
# with st.expander("Long Term Assets Breakdown"):
#     st.dataframe(long_term_assets_breakdown.to_dict())
    
# with st.expander("Liabilities Breakdown"):
#     st.dataframe(long_term_liabilities_breakdown.to_dict())

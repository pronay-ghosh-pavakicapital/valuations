import streamlit as st
from tqdm import tqdm
import pandas as pd
import numpy as np
import requests
import time

sheet_id_database_data = "1O04GWbcZztoIadx1KK2CQ8X58ZqPQcRqpv4ck4Yxz54"
sheet_name_database_data = "sheet2"
url_database_data = f"https://docs.google.com/spreadsheets/d/{sheet_id_database_data}/gviz/tq?tqx=out:csv&sheet={sheet_name_database_data}"
database_data = pd.read_csv(url_database_data)

sheet_id_select_reject = "1eripQyKChiD0co-xrl22mFV6MCwlSJzm7m7DscIubOI"
sheet_name_select_reject = "data_columns"
url_select_reject = f"https://docs.google.com/spreadsheets/d/{sheet_id_select_reject}/gviz/tq?tqx=out:csv&sheet={sheet_name_select_reject}"
data = pd.read_csv(url_select_reject)

selected_data = data[data["Select/Reject"] == "select"]
selected_data = data[data["Select/Reject"] == "select"]
balance_sheet_corpus = selected_data["data_columns"]
reject_list = data[data["Select/Reject"] == "reject"]


#  This method returns the cik and the ticker given a company name
def return_ticker_data(ticker):
    return_data = pd.DataFrame({"cik_str": [0], "ticker": [0], "title": [0]})
    company_ticker = ticker
    # create request header
    headers = {"User-Agent": "ghosh.pronay18071997@gmail.com"}

    # get all companies data
    companyTickers = requests.get(
        "https://www.sec.gov/files/company_tickers.json", headers=headers
    )
    # format response to dictionary and get first key/value
    firstEntry = companyTickers.json()["0"]
    # parse CIK // without leading zeros
    directCik = companyTickers.json()["0"]["cik_str"]
    # dictionary to dataframe
    companyData = pd.DataFrame.from_dict(companyTickers.json(), orient="index")

    for tick in companyData["ticker"]:
        if tick == company_ticker:
            return_data = companyData[companyData["ticker"] == tick]

    cik = "000" + str(return_data["cik_str"].values[0])
    if len(cik) < 10:
        cik_buffer = 10 - len(cik)
        cik = "0" * cik_buffer + cik
    else:
        cik = cik
    ticker = return_data["ticker"].values[0]
    company_name = return_data["title"].values[0]
    print(return_data["cik_str"])
    print(return_data["ticker"])
    print(return_data["title"])
    return cik, ticker, company_name


def get_asset_data_given_cik(cik):
    # get company concept data
    headers = {"User-Agent": "ghosh.pronay18071997@gmail.com"}
    companyConcept = requests.get(
        (
            f"https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}"
            f"/us-gaap/Assets.json"
        ),
        headers=headers,
    )

    # review data
    companyConcept.json().keys()
    companyConcept.json()["units"]
    companyConcept.json()["units"].keys()
    companyConcept.json()["units"]["USD"]
    companyConcept.json()["units"]["USD"][0]

    # parse assets from single filing
    companyConcept.json()["units"]["USD"][0]["val"]

    # get all filings data
    assetsData = pd.DataFrame.from_dict((companyConcept.json()["units"]["USD"]))
    return assetsData


def filter_accn(accn_data):
    accn_cleaned_list = []
    for accn in accn_data:
        print(accn)
        accn_cleaned = accn.split("-")[1]
        accn = accn.replace("-", "")
        accn_cleaned_list.append(accn)
        return accn_cleaned_list[0]


# This method scrapes the latest_balance_sheet_data given accn
# This method scrapes the latest_balance_sheet_data given accn
def scrape_latest_balance_sheet_and_standerdize_columns(
    accn, balance_sheet_corpus, cik
):
    """
    args:
    - accn : the accn of the company
    - balance sheet corpus: the overall data of the (select reject list) that has been defined above

    retrun:
    - the final balance sheet data in a dataframe --> var name : mydata
    """
    balance_sheet_cols_list = []
    html_report_list = []
    accn_folder_url = "https://www.sec.gov/Archives/edgar/data/" + cik + "/" + accn
    header = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
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

        data_url = (
            "https://www.sec.gov/Archives/edgar/data/"
            + cik
            + "/"
            + accn
            + "/"
            + report_index
        )
        # print()
        print("Trying for ", data_url)
        try:
            header = {
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
                "X-Requested-With": "XMLHttpRequest",
            }
            time.sleep(2)
            r = requests.get(data_url, headers=header)
            if r.status_code != 200:
                continue
            else:
                print("request successful for ", data_url)
                dfs = pd.read_html(r.text)
                mydata = dfs[0]
                keywords_list = ["Balance Sheet", "BALANCE SHEET", "balance sheet"]
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
                        balance_keyword = balance_keyword.rstrip()
                        print("Starting White space removed!!!!")
                        balance_keyword = balance_keyword.lstrip()
                        print("Leading White space removed!!!!")
                        print("The Exact Balance Keyword is :", balance_keyword)
                        print(
                            "Checking if the exact keyword is there in the balance_sheet_keyword_list or not"
                        )
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
                                print("Saving data...", mydata)
                                mydata = mydata.fillna(0)
                                mydata.to_csv(f"./original_data_folder/data.csv")

                            elif "($)  " in balance_keyword:
                                print("[($)  ] found...")
                                print("Checking for [  $]...")
                                if "  $" in balance_keyword:
                                    print("Saving data...", mydata)
                                    mydata = mydata.fillna(0)
                                    mydata.to_csv("./original_data_folder/data.csv")
                                    flag = 1
                                    if flag == 1:
                                        mydata.to_csv("./original_data_folder/data.csv")
                                        break
                                    break

                            elif "  $" in balance_keyword:
                                print("Saving data...", mydata)
                                mydata = mydata.fillna(0)
                                mydata.to_csv(f"./original_data_folder/data.csv")
                                flag = 1
                                if flag == 1:
                                    mydata.to_csv(f"./original_data_folder/data.csv")
                                    break
                                break
                            else:
                                print("No match found with ", balance_keyword)

                        time.sleep(2)
        except Exception as e:
            print(e)
            pass

    return mydata


sheet_id_database_data = "1O04GWbcZztoIadx1KK2CQ8X58ZqPQcRqpv4ck4Yxz54"
sheet_name_database_data = "sheet2"
url_database_data = f"https://docs.google.com/spreadsheets/d/{sheet_id_database_data}/gviz/tq?tqx=out:csv&sheet={sheet_name_database_data}"
database_data = pd.read_csv(url_database_data)

sheet_id_select_reject = "1eripQyKChiD0co-xrl22mFV6MCwlSJzm7m7DscIubOI"
sheet_name_select_reject = "data_columns"
url_select_reject = f"https://docs.google.com/spreadsheets/d/{sheet_id_select_reject}/gviz/tq?tqx=out:csv&sheet={sheet_name_select_reject}"
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
reference_bs_data.drop(["Unnamed: 0"], axis=1, inplace=True)


# from data_standerdization_dropdown.frontend_finalized_view import *
def get_cash_and_cash_equivalents_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns[0]].tolist()
    balance_col_values = [x.lower() for x in balance_col_values]

    # balance_col_values = reference_bs_data[reference_bs_data.columns.values[0]].values
    # print("balance_col_values: ", balance_col_values)
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    database_data["Cash and cash equivalents"].fillna(
        "Cash and cash equivalents", inplace=True
    )
    cash_list = database_data["Cash and cash equivalents"].values
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_marketable_secrities_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    # balance_col_values = data[data.columns.values[0]].values
    balance_col_values = data[data.columns[0]].tolist()
    balance_col_values = [x.lower() for x in balance_col_values]
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    database_data["Marketable securities"].fillna("Marketable securities", inplace=True)
    cash_list = database_data["Marketable securities"].values
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_rights_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    database_data["Rights-of-use relating to leases"].fillna(
        "Rights-of-use relating to leases", inplace=True
    )
    cash_list = database_data["Rights-of-use relating to leases"].values

    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

        print("investments_and_other_assets_list_column1 :", col1_cash_data)
        print("investments_and_other_assets_list_column2 :", col1_cash_data)
    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_inventories_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Inventories"].values
    database_data["Inventories"].fillna("Inventories", inplace=True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_accounts_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Accounts receivable, net of allowances"].values
    database_data["Accounts receivable, net of allowances"].fillna(
        "Accounts receivable, net of allowances", inplace=True
    )
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_other_recievables_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Other receivables"].values
    database_data["Other receivables"].fillna("Other receivables", inplace=True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_tax_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Tax"].values
    database_data["Tax"].fillna("Tax", inplace=True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_prepaid_expences_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Prepaid expenses and other current assets"].values
    database_data["Prepaid expenses and other current assets"].fillna(
        "Prepaid expenses and other current assets", inplace=True
    )
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_restricted_cash(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Restricted cash"].values
    database_data["Restricted cash"].fillna("Restricted cash", inplace=True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_right_of_use_assets_for_operating_leases_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Right-of-use assets for operating leases"].values
    database_data["Right-of-use assets for operating leases"].fillna(
        "Right-of-use assets for operating leases", inplace=True
    )
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_net_properties_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Net property and equipment"].values
    database_data["Net property and equipment"].fillna(
        "Net property and equipment", inplace=True
    )
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_real_estate_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Real Estate Assets"].values
    database_data["Real Estate Assets"].fillna("Real Estate Assets", inplace=True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_investment_assets_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Investments and other assets"].values
    database_data["Investments and other assets"].fillna(
        "Investments and other assets", inplace=True
    )
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_investment_in_other_companies_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Investment in Other Companies"].values
    database_data["Investment in Other Companies"].fillna(
        "Investment in Other Companies", inplace=True
    )
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_pentions_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Pensions Assets"].values
    database_data["Pensions Assets"].fillna("Pensions Assets", inplace=True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_goodwill_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Goodwill"].values
    database_data["Goodwill"].fillna("Goodwill", inplace=True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_Indefinite_lived_and_amortizable_intangible_assets_data_breakdown(
    data, database_data
):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data[
        "Indefinite-lived and amortizable intangible assets"
    ].values
    database_data["Indefinite-lived and amortizable intangible assets"].fillna(
        "Indefinite-lived and amortizable intangible assets", inplace=True
    )
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_deferred_income_taxes_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Deferred income taxes"].values
    database_data["Deferred income taxes"].fillna("Deferred income taxes", inplace=True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_total_intangible_and_other_assets_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Total intangible and other assets"].values
    database_data["Total intangible and other assets"].fillna(
        "Total intangible and other assets", inplace=True
    )
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_assets_for_discontinued_business_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Assets for Discontinued Business"].values
    database_data["Assets for Discontinued Business"].fillna(
        "Assets for Discontinued Business", inplace=True
    )
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_current_portion_of_long_term_debt_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Current portion of long-term debt"].values
    database_data["Current portion of long-term debt"].fillna(
        "Current portion of long-term debt", inplace=True
    )
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_current_portion_of_operating_lease_liability_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Current portion of operating lease liability"].values
    database_data["Current portion of operating lease liability"].fillna(
        "Current portion of operating lease liability", inplace=True
    )
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_accounts_payable_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Accounts payable"].values
    database_data["Accounts payable"].fillna("Accounts payable", inplace=True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_accrued_liabilities_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Accrued liabilities"].values
    database_data["Accrued liabilities"].fillna("Accrued liabilities", inplace=True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_income_taxes_payable_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Income taxes payable"].values
    database_data["Income taxes payable"].fillna("Income taxes payable", inplace=True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(int(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(int(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_deferred_income_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Deferred income"].values
    database_data["Deferred income"].fillna("Deferred income", inplace=True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_total_intangible_and_other_assets_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Total intangible and other assets"].values
    database_data["Total intangible and other assets"].fillna(
        "Total intangible and other assets", inplace=True
    )
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_assets_for_discontinued_business_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Assets for Discontinued Business"].values
    database_data["Assets for Discontinued Business"].fillna(
        "Assets for Discontinued Business", inplace=True
    )
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_contracts_payable_for_programming_rights_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Contracts payable for programming rights"].values
    database_data["Contracts payable for programming rights"].fillna(
        "Contracts payable for programming rights", inplace=True
    )
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_total_current_liabilities_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Total current liabilities"].values
    database_data["Total current liabilities"].fillna(
        "Total current liabilities", inplace=True
    )
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_lease_liabilities_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Lease Liabilities"].values
    database_data["Lease Liabilities"].fillna("Lease Liabilities", inplace=True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_long_term_debt_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Long-term debt"].values
    database_data["Long-term debt"].fillna("Long-term debt", inplace=True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_pension_liabilities_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Pension liabilities"].values
    database_data["Pension liabilities"].fillna("Pension liabilities", inplace=True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_deferred_income_tax_liability_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Deferred income tax liability"].values
    database_data["Deferred income tax liability"].fillna(
        "Deferred income tax liability", inplace=True
    )
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_long_term_tax_liabilities_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Long-term tax liabilities"].values
    database_data["Long-term tax liabilities"].fillna(
        "Long-term tax liabilities", inplace=True
    )
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_other_noncurrent_liabilities_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Other noncurrent liabilities"].values
    database_data["Other noncurrent liabilities"].fillna(
        "Other noncurrent liabilities", inplace=True
    )
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_total_liabilities_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Total liabilities"].values
    database_data["Total liabilities"].fillna("Total liabilities", inplace=True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_redeemable_noncontrolling_interest_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Redeemable noncontrolling interest"].values
    database_data["Redeemable noncontrolling interest"].fillna(
        "Redeemable noncontrolling interest", inplace=True
    )
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_total_equity_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Total equity"].values
    database_data["Total equity"].fillna("Total equity", inplace=True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown(
    data, database_data
):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data[
        "Total liabilities, redeemable noncontrolling interest and equity"
    ].values
    database_data[
        "Total liabilities, redeemable noncontrolling interest and equity"
    ].fillna(
        "Total liabilities, redeemable noncontrolling interest and equity", inplace=True
    )
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


def get_total_current_assets_data_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Total current assets"].values
    database_data["Total current assets"].fillna("Total current assets", inplace=True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print(
                "Column 1 Total current assets data after filteration is :",
                col1_cash_data,
            )
            print(
                "Column 2 Total current assets data after filteration is :",
                col2_cash_data,
            )
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


col1, col2, col3, col4, col5, col6 = st.columns(6)
with col6:
    st.image("./logo.jpg", width=200)
# with col2:
with col3:
    st.title("Valuations")
ticker_name = st.text_input("Enter Ticker Name: ", key="name")
tickers_list = [ticker_name]
cik = 0
for ticker in tickers_list:
    cik, ticker, company_name = return_ticker_data(ticker)
    # If the length of cik is less than 10 then filter the number of zeros that needs to be added and form the new cik number
    if len(cik) < 10:
        cik_buffer = 10 - len(cik)
        cik = "0" * cik_buffer + cik
    else:
        cik = cik
    print(cik)
    try:
        asset_data = get_asset_data_given_cik(cik)
        asset_data["filed"].max()
        balance_sheet_data_10q = asset_data[asset_data["form"] == "10-Q"]
        balance_sheet_data_10k = asset_data[asset_data["form"] == "10-K"]
        balance_sheet_data = pd.concat(
            [balance_sheet_data_10q, balance_sheet_data_10k], axis=0
        )
        latest_datapoint = balance_sheet_data[
            balance_sheet_data["filed"] == balance_sheet_data["filed"].max()
        ]
        filtered_accn = filter_accn(latest_datapoint["accn"])
        # balance_sheet_col = scrape_latest_balance_sheet_and_standerdize_columns(filtered_accn)
        balance_sheet_data = scrape_latest_balance_sheet_and_standerdize_columns(
            filtered_accn, balance_sheet_corpus, cik
        )
        # balance_sheet_data.to_csv("AMD_actual_bs_data.csv")
        reference_bs_data = balance_sheet_data
    except Exception as e:
        print(f"Sourcing Ran into Exception with : {e}")


def get_total_assets_breakdown(data, database_data):
    """
    Here data is the actual balance sheet dataframe and teh database_data is referred to be as the dictionary
    """
    col1_cash_data = 0
    col2_cash_data = 0
    cash_eqv_and_marketable_segments_list_column1 = []
    cash_eqv_and_marketable_segments_list_column2 = []
    datatype_filtered_list = [int, float]

    # Getting all of the line items
    balance_col_values = data[data.columns.values[0]].values
    balance_col_values_total_current_assets = [x.lower() for x in balance_col_values]

    # Getting all of the cash list
    cash_list = database_data["Total assets"].values
    database_data["Total assets"].fillna("Total assets", inplace=True)
    cash_list = [x.lower() for x in cash_list]

    exact_cash_line_item_name_list = []
    for cash in balance_col_values_total_current_assets:
        if cash in cash_list:
            print("cash value found")
            exact_cash_line_item_name_list.append(cash)
            index_of_cash = balance_col_values_total_current_assets.index(cash)
            col1_cash_data = data[data.columns[1]].values[index_of_cash]
            print("Column 1 cash Data is :", col1_cash_data)
            col2_cash_data = data[data.columns[2]].values[index_of_cash]
            print("Column 2 cash Data is :", col2_cash_data)
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
            print("Column 1 cash Data after filteration is :", col1_cash_data)
            print("Column 2 cash Data after filteration is :", col2_cash_data)
            cash_eqv_and_marketable_segments_list_column1.append(float(col1_cash_data))
            cash_eqv_and_marketable_segments_list_column2.append(float(col2_cash_data))

    # balance_col_values_total_current_assets
    cash_df = pd.DataFrame(
        {
            data.columns.values[0]: exact_cash_line_item_name_list,
            data.columns[1]: col1_cash_data,
            data.columns[2]: col2_cash_data,
        }
    )
    return cash_df


# Calculation of breakdown level data
# Current Assets
cash_data_breakdown = get_cash_and_cash_equivalents_breakdown(
    reference_bs_data, database_data
)
market_data_breakdown = get_marketable_secrities_data_breakdown(
    reference_bs_data, database_data
)
rights_data_breakdown = get_rights_data_breakdown(reference_bs_data, database_data)
inventories_data_breakdown = get_inventories_data_breakdown(
    reference_bs_data, database_data
)
accounts_data_breakdown = get_accounts_data_breakdown(reference_bs_data, database_data)
other_recievables_data_breakdown = get_other_recievables_data_breakdown(
    reference_bs_data, database_data
)
tax_data_breakdown = get_tax_data_breakdown(reference_bs_data, database_data)
prepaid_expences_data_breakdown = get_prepaid_expences_data_breakdown(
    reference_bs_data, database_data
)
total_current_assets_data_breakdown = get_total_current_assets_data_breakdown(
    reference_bs_data, database_data
)

# # Long Term Assets
right_of_use_assets_for_operating_leases_data_breakdown = (
    get_right_of_use_assets_for_operating_leases_data_breakdown(
        reference_bs_data, database_data
    )
)
net_properties_data_breakdown = get_net_properties_data_breakdown(
    reference_bs_data, database_data
)
real_estate_data_breakdown = get_real_estate_data_breakdown(
    reference_bs_data, database_data
)
investment_assets_data_breakdown = get_investment_assets_data_breakdown(
    reference_bs_data, database_data
)
investment_in_other_companies_data_breakdown = (
    get_investment_in_other_companies_data_breakdown(reference_bs_data, database_data)
)
pentions_data_breakdown = get_pentions_data_breakdown(reference_bs_data, database_data)
goodwill_data_breakdown = get_goodwill_data_breakdown(reference_bs_data, database_data)
Indefinite_lived_and_amortizable_intangible_assets_data_breakdown = (
    get_Indefinite_lived_and_amortizable_intangible_assets_data_breakdown(
        reference_bs_data, database_data
    )
)
deferred_income_taxes_data_breakdown = get_deferred_income_taxes_data_breakdown(
    reference_bs_data, database_data
)
total_intangible_and_other_assets_data_breakdown = (
    get_total_intangible_and_other_assets_data_breakdown(
        reference_bs_data, database_data
    )
)
assets_for_discontinued_business_data_breakdown = (
    get_assets_for_discontinued_business_data_breakdown(
        reference_bs_data, database_data
    )
)
restricted_cash_data_breakdown = get_restricted_cash(reference_bs_data, database_data)


# Liabilities:
current_portion_of_long_term_debt_data_breakdown = (
    get_current_portion_of_long_term_debt_data_breakdown(
        reference_bs_data, database_data
    )
)
current_portion_of_operating_lease_liability_breakdown = (
    get_current_portion_of_operating_lease_liability_breakdown(
        reference_bs_data, database_data
    )
)
accounts_payable_data_breakdown = get_accounts_payable_breakdown(
    reference_bs_data, database_data
)
accrued_liabilities_breakdown = get_accrued_liabilities_breakdown(
    reference_bs_data, database_data
)
income_taxes_payable_breakdown = get_income_taxes_payable_breakdown(
    reference_bs_data, database_data
)
deferred_income_data_breakdown = get_deferred_income_data_breakdown(
    reference_bs_data, database_data
)
total_intangible_and_other_assets_data_breakdown = (
    get_total_intangible_and_other_assets_data_breakdown(
        reference_bs_data, database_data
    )
)
assets_for_discontinued_business_data_breakdown = (
    get_assets_for_discontinued_business_data_breakdown(
        reference_bs_data, database_data
    )
)
contracts_payable_for_programming_rights_data_breakdown = (
    get_contracts_payable_for_programming_rights_data_breakdown(
        reference_bs_data, database_data
    )
)
total_current_liabilities_data_breakdown = get_total_current_liabilities_data_breakdown(
    reference_bs_data, database_data
)
lease_liabilities_data_breakdown = get_lease_liabilities_data_breakdown(
    reference_bs_data, database_data
)
long_term_debt_data_breakdown = get_long_term_debt_data_breakdown(
    reference_bs_data, database_data
)
pension_liabilities_data_breakdown = get_pension_liabilities_data_breakdown(
    reference_bs_data, database_data
)
deferred_income_tax_liability_data_breakdown = (
    get_deferred_income_tax_liability_data_breakdown(reference_bs_data, database_data)
)
long_term_tax_liabilities_data_breakdown = get_long_term_tax_liabilities_data_breakdown(
    reference_bs_data, database_data
)
other_noncurrent_liabilities_data_breakdown = (
    get_other_noncurrent_liabilities_data_breakdown(reference_bs_data, database_data)
)
total_liabilities_data_breakdown = get_total_liabilities_data_breakdown(
    reference_bs_data, database_data
)
redeemable_noncontrolling_interest_data_breakdown = (
    get_redeemable_noncontrolling_interest_data_breakdown(
        reference_bs_data, database_data
    )
)
total_equity_data_breakdown = get_total_equity_data_breakdown(
    reference_bs_data, database_data
)
total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown = (
    get_total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown(
        reference_bs_data, database_data
    )
)

# Sourcing Total Assets from SEC Level
total_assets_data_breakdown = get_total_assets_breakdown(
    reference_bs_data, database_data
)


try:
    # Current Assets
    total_assets_data_breakdown_col1 = int(
        total_assets_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    total_assets_data_breakdown_col2 = int(
        total_assets_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for total_assets_data_breakdown Equivalents...")
    print("Returning Zero")
    total_assets_data_breakdown_col1 = 0
    total_assets_data_breakdown_col2 = 0


try:
    # Current Assets
    cash_data_breakdown_col1 = int(
        cash_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    cash_data_breakdown_col2 = int(
        cash_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for Cash and Cash Equivalents...")
    print("Returning Zero")
    cash_data_breakdown_col1 = 0
    cash_data_breakdown_col2 = 0

try:
    market_data_breakdown_col1 = int(
        market_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    market_data_breakdown_col2 = int(
        market_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for Market Data...")
    print("Returning Zero")
    market_data_breakdown_col1 = 0
    market_data_breakdown_col2 = 0

try:
    rights_data_breakdown_col1 = int(
        rights_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    rights_data_breakdown_col2 = int(
        rights_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for Rights-of-use relating to leases Data...")
    print("Returning Zero")
    rights_data_breakdown_col1 = 0
    rights_data_breakdown_col2 = 0


try:
    inventories_data_col1 = int(
        inventories_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    inventories_data_col2 = int(
        inventories_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for Inventories Data...")
    print("Returning Zero")
    inventories_data_col1 = 0
    inventories_data_col2 = 0


try:
    accounts_data_col1 = int(
        accounts_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    accounts_data_col2 = int(
        accounts_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for Accounts Data...")
    print("Returning Zero")
    accounts_data_col1 = 0
    accounts_data_col2 = 0

# restricted_cash_data
try:
    # total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown
    restricted_cash_data_col1 = int(
        restricted_cash_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    restricted_cash_data_col2 = int(
        restricted_cash_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for restricted_cash_data_breakdown...")
    print("Returning Zero")
    restricted_cash_data_col1 = 0
    restricted_cash_data_col2 = 0

try:
    total_current_assets_data_breakdown_col1 = int(
        total_current_assets_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    total_current_assets_data_breakdown_col2 = int(
        total_current_assets_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for total_current_assets_data_breakdown...")
    print("Returning Zero")
    total_current_assets_data_breakdown_col1 = 0
    total_current_assets_data_breakdown_col2 = 0


try:
    other_recievables_data_col1 = int(
        other_recievables_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    other_recievables_data_col2 = int(
        other_recievables_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for Other Recievables Data...")
    print("Returning Zero")
    other_recievables_data_col1 = 0
    other_recievables_data_col2 = 0

try:
    tax_data_col1 = int(tax_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0])
    tax_data_col2 = int(tax_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0])
except Exception as e:
    print("Data Not Matched for Tax Data...")
    print("Returning Zero")
    tax_data_col1 = 0
    tax_data_col2 = 0

try:
    prepaid_expences_data_col1 = int(
        prepaid_expences_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    prepaid_expences_data_col2 = int(
        prepaid_expences_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for Tax Data...")
    print("Returning Zero")
    prepaid_expences_data_col1 = 0
    prepaid_expences_data_col2 = 0


try:
    right_of_use_assets_for_operating_leases_data_breakdown_col1 = int(
        right_of_use_assets_for_operating_leases_data_breakdown.iloc[
            :1, 1:2
        ].values.tolist()[0][0]
    )
    right_of_use_assets_for_operating_leases_data_breakdown_col2 = int(
        right_of_use_assets_for_operating_leases_data_breakdown.iloc[
            :1, 2:3
        ].values.tolist()[0][0]
    )
except Exception as e:
    print(
        "Data Not Matched for right_of_use_assets_for_operating_leases_data_breakdown Data..."
    )
    print("Returning Zero")
    right_of_use_assets_for_operating_leases_data_breakdown_col1 = 0
    right_of_use_assets_for_operating_leases_data_breakdown_col2 = 0


try:
    # net_properties_data_breakdown
    net_properties_data_breakdown_col1 = int(
        net_properties_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    net_properties_data_breakdown_col2 = int(
        net_properties_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for net_properties_data_breakdown...")
    print("Returning Zero")
    net_properties_data_breakdown_col1 = 0
    net_properties_data_breakdown_col2 = 0


try:
    # real_estate_data_breakdown
    real_estate_data_breakdown_col1 = int(
        real_estate_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    real_estate_data_breakdown_col2 = int(
        real_estate_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for real_estate_data_breakdown...")
    print("Returning Zero")
    real_estate_data_breakdown_col1 = 0
    real_estate_data_breakdown_col2 = 0

try:
    # investment_assets_data_breakdown
    investment_assets_data_breakdown_col1 = int(
        investment_assets_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    investment_assets_data_breakdown_col2 = int(
        investment_assets_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for investment_assets_data_breakdown...")
    print("Returning Zero")
    investment_assets_data_breakdown_col1 = 0
    investment_assets_data_breakdown_col2 = 0

try:
    # investment_in_other_companies_data_breakdown
    investment_in_other_companies_data_breakdown_col1 = int(
        investment_in_other_companies_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    investment_in_other_companies_data_breakdown_col2 = int(
        investment_in_other_companies_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for investment_in_other_companies_data_breakdown...")
    print("Returning Zero")
    investment_in_other_companies_data_breakdown_col1 = 0
    investment_in_other_companies_data_breakdown_col2 = 0

try:
    # pentions_data_breakdown
    pentions_data_breakdown_col1 = int(
        pentions_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    pentions_data_breakdown_col2 = int(
        pentions_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for pentions_data_breakdown...")
    print("Returning Zero")
    pentions_data_breakdown_col1 = 0
    pentions_data_breakdown_col2 = 0

try:
    # goodwill_data_breakdown
    goodwill_data_breakdown_col1 = int(
        goodwill_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    goodwill_data_breakdown_col2 = int(
        goodwill_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for goodwill_data_breakdown...")
    print("Returning Zero")
    goodwill_data_breakdown_col1 = 0
    goodwill_data_breakdown_col2 = 0

# Indefinite_lived_and_amortizable_intangible_assets_data_breakdown
try:
    # Indefinite_lived_and_amortizable_intangible_assets_data_breakdown
    Indefinite_lived_and_amortizable_intangible_assets_data_breakdown_col1 = int(
        Indefinite_lived_and_amortizable_intangible_assets_data_breakdown.iloc[
            :1, 1:2
        ].values.tolist()[0][0]
    )
    Indefinite_lived_and_amortizable_intangible_assets_data_breakdown_col2 = int(
        Indefinite_lived_and_amortizable_intangible_assets_data_breakdown.iloc[
            :1, 2:3
        ].values.tolist()[0][0]
    )
except Exception as e:
    print(
        "Data Not Matched for Indefinite_lived_and_amortizable_intangible_assets_data_breakdown..."
    )
    print("Returning Zero")
    Indefinite_lived_and_amortizable_intangible_assets_data_breakdown_col1 = 0
    Indefinite_lived_and_amortizable_intangible_assets_data_breakdown_col2 = 0

# deferred_income_taxes_data_breakdown
try:
    # deferred_income_taxes_data_breakdown
    deferred_income_taxes_data_breakdown_col1 = int(
        deferred_income_taxes_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    deferred_income_taxes_data_breakdown_col2 = int(
        deferred_income_taxes_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for deferred_income_taxes_data_breakdown...")
    print("Returning Zero")
    deferred_income_taxes_data_breakdown_col1 = 0
    deferred_income_taxes_data_breakdown_col2 = 0

# total_intangible_and_other_assets_data_breakdown
try:
    # total_intangible_and_other_assets_data_breakdown
    total_intangible_and_other_assets_data_breakdown_col1 = int(
        total_intangible_and_other_assets_data_breakdown.iloc[:1, 1:2].values.tolist()[
            0
        ][0]
    )
    total_intangible_and_other_assets_data_breakdown_col2 = int(
        total_intangible_and_other_assets_data_breakdown.iloc[:1, 2:3].values.tolist()[
            0
        ][0]
    )
except Exception as e:
    print("Data Not Matched for total_intangible_and_other_assets_data_breakdown...")
    print("Returning Zero")
    total_intangible_and_other_assets_data_breakdown_col1 = 0
    total_intangible_and_other_assets_data_breakdown_col2 = 0

# assets_for_discontinued_business_data_breakdown
try:
    # assets_for_discontinued_business_data_breakdown
    assets_for_discontinued_business_data_breakdown_col1 = int(
        assets_for_discontinued_business_data_breakdown.iloc[:1, 1:2].values.tolist()[
            0
        ][0]
    )
    assets_for_discontinued_business_data_breakdown_col2 = int(
        assets_for_discontinued_business_data_breakdown.iloc[:1, 2:3].values.tolist()[
            0
        ][0]
    )
except Exception as e:
    print("Data Not Matched for assets_for_discontinued_business_data_breakdown...")
    print("Returning Zero")
    assets_for_discontinued_business_data_breakdown_col1 = 0
    assets_for_discontinued_business_data_breakdown_col2 = 0

# current_portion_of_long_term_debt_data_breakdown
try:
    # current_portion_of_long_term_debt_data_breakdown
    current_portion_of_long_term_debt_data_breakdown_col1 = int(
        current_portion_of_long_term_debt_data_breakdown.iloc[:1, 1:2].values.tolist()[
            0
        ][0]
    )
    current_portion_of_long_term_debt_data_breakdown_col2 = int(
        current_portion_of_long_term_debt_data_breakdown.iloc[:1, 2:3].values.tolist()[
            0
        ][0]
    )
except Exception as e:
    print("Data Not Matched for current_portion_of_long_term_debt_data_breakdown...")
    print("Returning Zero")
    current_portion_of_long_term_debt_data_breakdown_col1 = 0
    current_portion_of_long_term_debt_data_breakdown_col2 = 0

# current_portion_of_operating_lease_liability_breakdown
try:
    # current_portion_of_operating_lease_liability_breakdown
    current_portion_of_operating_lease_liability_breakdown_col1 = int(
        current_portion_of_operating_lease_liability_breakdown.iloc[
            :1, 1:2
        ].values.tolist()[0][0]
    )
    current_portion_of_operating_lease_liability_breakdown_col2 = int(
        current_portion_of_operating_lease_liability_breakdown.iloc[
            :1, 2:3
        ].values.tolist()[0][0]
    )
except Exception as e:
    print(
        "Data Not Matched for current_portion_of_operating_lease_liability_breakdown..."
    )
    print("Returning Zero")
    current_portion_of_operating_lease_liability_breakdown_col1 = 0
    current_portion_of_operating_lease_liability_breakdown_col2 = 0

# accounts_payable_data_breakdown
try:
    # accounts_payable_data_breakdown
    accounts_payable_data_breakdown_col1 = int(
        accounts_payable_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    accounts_payable_data_breakdown_col2 = int(
        accounts_payable_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for accounts_payable_data_breakdown...")
    print("Returning Zero")
    accounts_payable_data_breakdown_col1 = 0
    accounts_payable_data_breakdown_col2 = 0

# accrued_liabilities_breakdown
try:
    # accrued_liabilities_breakdown
    accrued_liabilities_breakdown_col1 = int(
        accrued_liabilities_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    accrued_liabilities_breakdown_col2 = int(
        accrued_liabilities_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for accrued_liabilities_breakdown...")
    print("Returning Zero")
    accrued_liabilities_breakdown_col1 = 0
    accrued_liabilities_breakdown_col2 = 0

# income_taxes_payable_breakdown
try:
    # income_taxes_payable_breakdown
    income_taxes_payable_breakdown_col1 = int(
        income_taxes_payable_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    income_taxes_payable_breakdown_col2 = int(
        income_taxes_payable_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for income_taxes_payable_breakdown...")
    print("Returning Zero")
    income_taxes_payable_breakdown_col1 = 0
    income_taxes_payable_breakdown_col2 = 0

# deferred_income_data_breakdown
try:
    # deferred_income_data_breakdown
    deferred_income_data_breakdown_col1 = int(
        deferred_income_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    deferred_income_data_breakdown_col2 = int(
        deferred_income_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for deferred_income_data_breakdown...")
    print("Returning Zero")
    deferred_income_data_breakdown_col1 = 0
    deferred_income_data_breakdown_col2 = 0

# total_intangible_and_other_assets_data_breakdown
try:
    # total_intangible_and_other_assets_data_breakdown
    total_intangible_and_other_assets_data_breakdown_col1 = int(
        total_intangible_and_other_assets_data_breakdown.iloc[:1, 1:2].values.tolist()[
            0
        ][0]
    )
    total_intangible_and_other_assets_data_breakdown_col2 = int(
        total_intangible_and_other_assets_data_breakdown.iloc[:1, 2:3].values.tolist()[
            0
        ][0]
    )
except Exception as e:
    print("Data Not Matched for total_intangible_and_other_assets_data_breakdown...")
    print("Returning Zero")
    total_intangible_and_other_assets_data_breakdown_col1 = 0
    total_intangible_and_other_assets_data_breakdown_col2 = 0

# assets_for_discontinued_business_data_breakdown
try:
    # assets_for_discontinued_business_data_breakdown
    assets_for_discontinued_business_data_breakdown_col1 = int(
        assets_for_discontinued_business_data_breakdown.iloc[:1, 1:2].values.tolist()[
            0
        ][0]
    )
    assets_for_discontinued_business_data_breakdown_col2 = int(
        assets_for_discontinued_business_data_breakdown.iloc[:1, 2:3].values.tolist()[
            0
        ][0]
    )
except Exception as e:
    print("Data Not Matched for assets_for_discontinued_business_data_breakdown...")
    print("Returning Zero")
    assets_for_discontinued_business_data_breakdown_col1 = 0
    assets_for_discontinued_business_data_breakdown_col2 = 0

# contracts_payable_for_programming_rights_data_breakdown
try:
    # contracts_payable_for_programming_rights_data_breakdown
    contracts_payable_for_programming_rights_data_breakdown_col1 = int(
        contracts_payable_for_programming_rights_data_breakdown.iloc[
            :1, 1:2
        ].values.tolist()[0][0]
    )
    contracts_payable_for_programming_rights_data_breakdown_col2 = int(
        contracts_payable_for_programming_rights_data_breakdown.iloc[
            :1, 2:3
        ].values.tolist()[0][0]
    )
except Exception as e:
    print(
        "Data Not Matched for contracts_payable_for_programming_rights_data_breakdown..."
    )
    print("Returning Zero")
    contracts_payable_for_programming_rights_data_breakdown_col1 = 0
    contracts_payable_for_programming_rights_data_breakdown_col2 = 0

# total_current_liabilities_data_breakdown
try:
    # total_current_liabilities_data_breakdown
    total_current_liabilities_data_breakdown_col1 = int(
        total_current_liabilities_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    total_current_liabilities_data_breakdown_col2 = int(
        total_current_liabilities_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for total_current_liabilities_data_breakdown...")
    print("Returning Zero")
    total_current_liabilities_data_breakdown_col1 = 0
    total_current_liabilities_data_breakdown_col2 = 0

# lease_liabilities_data_breakdown
try:
    # lease_liabilities_data_breakdown
    lease_liabilities_data_breakdown_col1 = int(
        lease_liabilities_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    lease_liabilities_data_breakdown_col2 = int(
        lease_liabilities_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for lease_liabilities_data_breakdown...")
    print("Returning Zero")
    lease_liabilities_data_breakdown_col1 = 0
    lease_liabilities_data_breakdown_col2 = 0

# long_term_debt_data_breakdown
try:
    # long_term_debt_data_breakdown
    long_term_debt_data_breakdown_col1 = int(
        long_term_debt_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    long_term_debt_data_breakdown_col2 = int(
        long_term_debt_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for long_term_debt_data_breakdown...")
    print("Returning Zero")
    long_term_debt_data_breakdown_col1 = 0
    long_term_debt_data_breakdown_col2 = 0

# pension_liabilities_data_breakdown
try:
    # pension_liabilities_data_breakdown
    pension_liabilities_data_breakdown_col1 = int(
        pension_liabilities_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    pension_liabilities_data_breakdown_col2 = int(
        pension_liabilities_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for pension_liabilities_data_breakdown...")
    print("Returning Zero")
    pension_liabilities_data_breakdown_col1 = 0
    pension_liabilities_data_breakdown_col2 = 0

# deferred_income_tax_liability_data_breakdown
try:
    # deferred_income_tax_liability_data_breakdown
    deferred_income_tax_liability_data_breakdown_col1 = int(
        deferred_income_tax_liability_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    deferred_income_tax_liability_data_breakdown_col2 = int(
        deferred_income_tax_liability_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for deferred_income_tax_liability_data_breakdown...")
    print("Returning Zero")
    deferred_income_tax_liability_data_breakdown_col1 = 0
    deferred_income_tax_liability_data_breakdown_col2 = 0

# long_term_tax_liabilities_data_breakdown
try:
    # long_term_tax_liabilities_data_breakdown
    long_term_tax_liabilities_data_breakdown_col1 = int(
        long_term_tax_liabilities_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    long_term_tax_liabilities_data_breakdown_col2 = int(
        long_term_tax_liabilities_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for long_term_tax_liabilities_data_breakdown...")
    print("Returning Zero")
    long_term_tax_liabilities_data_breakdown_col1 = 0
    long_term_tax_liabilities_data_breakdown_col2 = 0

# other_noncurrent_liabilities_data_breakdown
try:
    # other_noncurrent_liabilities_data_breakdown
    other_noncurrent_liabilities_data_breakdown_col1 = int(
        other_noncurrent_liabilities_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    other_noncurrent_liabilities_data_breakdown_col2 = int(
        other_noncurrent_liabilities_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for other_noncurrent_liabilities_data_breakdown...")
    print("Returning Zero")
    other_noncurrent_liabilities_data_breakdown_col1 = 0
    other_noncurrent_liabilities_data_breakdown_col2 = 0

# total_liabilities_data_breakdown
try:
    # total_liabilities_data_breakdown
    total_liabilities_data_breakdown_col1 = int(
        total_liabilities_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    total_liabilities_data_breakdown_col2 = int(
        total_liabilities_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for total_liabilities_data_breakdown...")
    print("Returning Zero")
    total_liabilities_data_breakdown_col1 = 0
    total_liabilities_data_breakdown_col2 = 0

# redeemable_noncontrolling_interest_data_breakdown
try:
    # redeemable_noncontrolling_interest_data_breakdown
    redeemable_noncontrolling_interest_data_breakdown_col1 = int(
        redeemable_noncontrolling_interest_data_breakdown.iloc[:1, 1:2].values.tolist()[
            0
        ][0]
    )
    redeemable_noncontrolling_interest_data_breakdown_col2 = int(
        redeemable_noncontrolling_interest_data_breakdown.iloc[:1, 2:3].values.tolist()[
            0
        ][0]
    )
except Exception as e:
    print("Data Not Matched for redeemable_noncontrolling_interest_data_breakdown...")
    print("Returning Zero")
    redeemable_noncontrolling_interest_data_breakdown_col1 = 0
    redeemable_noncontrolling_interest_data_breakdown_col2 = 0

# total_equity_data_breakdown
try:
    # total_equity_data_breakdown
    total_equity_data_breakdown_col1 = int(
        total_equity_data_breakdown.iloc[:1, 1:2].values.tolist()[0][0]
    )
    total_equity_data_breakdown_col2 = int(
        total_equity_data_breakdown.iloc[:1, 2:3].values.tolist()[0][0]
    )
except Exception as e:
    print("Data Not Matched for total_equity_data_breakdown...")
    print("Returning Zero")
    total_equity_data_breakdown_col1 = 0
    total_equity_data_breakdown_col2 = 0


# total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown
try:
    # total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown
    total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown_col1 = int(
        total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown.iloc[
            :1, 1:2
        ].values.tolist()[
            0
        ][
            0
        ]
    )
    total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown_col2 = int(
        total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown.iloc[
            :1, 2:3
        ].values.tolist()[
            0
        ][
            0
        ]
    )
except Exception as e:
    print("Data Not Matched for total_equity_data_breakdown...")
    print("Returning Zero")
    total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown_col1 = (
        0
    )
    total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown_col2 = (
        0
    )

# Total Calculation
total_current_assets_calculated_col1 = sum(
    [
        cash_data_breakdown_col1,
        market_data_breakdown_col1,
        inventories_data_col1,
        accounts_data_col1,
        other_recievables_data_col1,
        tax_data_col1,
        prepaid_expences_data_col1,
        restricted_cash_data_col1,
    ]
)


total_current_assets_calculated_col2 = sum(
    [
        cash_data_breakdown_col2,
        market_data_breakdown_col2,
        inventories_data_col2,
        accounts_data_col2,
        other_recievables_data_col2,
        tax_data_col2,
        prepaid_expences_data_col1,
        restricted_cash_data_col2,
    ]
)

total_long_term_assets_calculated_col1 = sum(
    [
        net_properties_data_breakdown_col1,
        real_estate_data_breakdown_col1,
        investment_assets_data_breakdown_col1,
        investment_in_other_companies_data_breakdown_col1,
        pentions_data_breakdown_col1,
        goodwill_data_breakdown_col1,
        Indefinite_lived_and_amortizable_intangible_assets_data_breakdown_col1,
        deferred_income_taxes_data_breakdown_col1,
        total_intangible_and_other_assets_data_breakdown_col1,
        assets_for_discontinued_business_data_breakdown_col1,
    ]
)


total_long_term_assets_calculated_col2 = sum(
    [
        net_properties_data_breakdown_col2,
        real_estate_data_breakdown_col2,
        investment_assets_data_breakdown_col2,
        investment_in_other_companies_data_breakdown_col2,
        pentions_data_breakdown_col2,
        goodwill_data_breakdown_col2,
        Indefinite_lived_and_amortizable_intangible_assets_data_breakdown_col2,
        deferred_income_taxes_data_breakdown_col2,
        total_intangible_and_other_assets_data_breakdown_col2,
        assets_for_discontinued_business_data_breakdown_col2,
    ]
)

total_current_liabilities_calculated_col1 = sum(
    [
        current_portion_of_long_term_debt_data_breakdown_col1,
        current_portion_of_operating_lease_liability_breakdown_col1,
        accounts_payable_data_breakdown_col1,
        accrued_liabilities_breakdown_col1,
        income_taxes_payable_breakdown_col1,
        deferred_income_data_breakdown_col1,
        total_intangible_and_other_assets_data_breakdown_col1,
        deferred_income_taxes_data_breakdown_col1,
        assets_for_discontinued_business_data_breakdown_col1,
        contracts_payable_for_programming_rights_data_breakdown_col1,
    ]
)
total_current_liabilities_calculated_col2 = sum(
    [
        current_portion_of_long_term_debt_data_breakdown_col2,
        current_portion_of_operating_lease_liability_breakdown_col2,
        accounts_payable_data_breakdown_col2,
        accrued_liabilities_breakdown_col2,
        income_taxes_payable_breakdown_col2,
        deferred_income_data_breakdown_col2,
        total_intangible_and_other_assets_data_breakdown_col2,
        deferred_income_taxes_data_breakdown_col2,
        assets_for_discontinued_business_data_breakdown_col2,
        contracts_payable_for_programming_rights_data_breakdown_col2,
    ]
)

total_long_term_liabilities_calculated_col1 = sum(
    [
        current_portion_of_long_term_debt_data_breakdown_col1,
        current_portion_of_operating_lease_liability_breakdown_col1,
        accounts_payable_data_breakdown_col1,
        accrued_liabilities_breakdown_col1,
        income_taxes_payable_breakdown_col1,
        deferred_income_data_breakdown_col1,
        total_intangible_and_other_assets_data_breakdown_col1,
        deferred_income_taxes_data_breakdown_col1,
        assets_for_discontinued_business_data_breakdown_col1,
        contracts_payable_for_programming_rights_data_breakdown_col1,
    ]
)
total_long_term_liabilities_calculated_col2 = sum(
    [
        current_portion_of_long_term_debt_data_breakdown_col2,
        current_portion_of_operating_lease_liability_breakdown_col2,
        accounts_payable_data_breakdown_col2,
        accrued_liabilities_breakdown_col2,
        income_taxes_payable_breakdown_col2,
        deferred_income_data_breakdown_col2,
        total_intangible_and_other_assets_data_breakdown_col2,
        deferred_income_taxes_data_breakdown_col2,
        assets_for_discontinued_business_data_breakdown_col2,
        contracts_payable_for_programming_rights_data_breakdown_col2,
    ]
)


# Error Calculation:
# Current Assets Error Calculation
current_assets_error_col1 = (
    total_current_assets_data_breakdown_col1 - total_current_assets_calculated_col1
)
current_assets_error_col2 = (
    total_current_assets_data_breakdown_col2 - total_current_assets_calculated_col2
)

# Total Assets Error Calculation
total_assets_calculated_col1 = (
    total_current_assets_data_breakdown_col1 + total_long_term_assets_calculated_col1
)
total_assets_calculated_col2 = (
    total_current_assets_data_breakdown_col2 + total_long_term_assets_calculated_col2
)

total_assets_error_col1 = (
    total_assets_calculated_col1 - total_assets_data_breakdown_col1
)
total_assets_error_col2 = (
    total_assets_calculated_col2 - total_assets_data_breakdown_col2
)

# Total Current liabilities Error Calculation
total_current_liabilities_error_col1 = (
    total_current_liabilities_data_breakdown_col1
    - total_current_liabilities_calculated_col1
)
total_current_liabilities_error_col2 = (
    total_current_liabilities_data_breakdown_col2
    - total_current_liabilities_calculated_col2
)

# Total liabilities,redeemable noncontrolling interest and equity Error Calculation
total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown_calculated_col1 = (
    redeemable_noncontrolling_interest_data_breakdown_col1
    + total_equity_data_breakdown_col1
)
total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown_calculated_col2 = (
    redeemable_noncontrolling_interest_data_breakdown_col2
    + total_equity_data_breakdown_col2
)

total_liabilities_redeemable_noncontrolling_interest_and_equity_error_col1 = (
    total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown_col1
    - total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown_calculated_col1
)
total_liabilities_redeemable_noncontrolling_interest_and_equity_error_col2 = (
    total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown_col2
    - total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown_calculated_col2
)

# Getting the Column Names
standerdized_data_columns = reference_bs_data.columns.tolist()
standerdized_data_columns

# if len(reference_bs_data.columns) >=3:
standerdized_data = pd.DataFrame(
    {
        standerdized_data_columns[0]: [
            "Cash Equivalents & Marketable Sec",
            "Rights-of-use relating to leases",
            "Inventories",
            "Accounts/Trade Receivables",
            "Tax",
            "Other Current Assets",
            "Total Current Assets Calculated",
            "Total Current Assets Actual",
            "Total Current Assets (Error)",
            "Rights-of-use relating to leases",
            "Plant, Property, and Equipment",
            "Real Estate Assets",
            "Investment Assets",
            "Investment in Other Companies",
            "Pensions Assets",
            "Intangible assets",
            "Other Assets",
            "Assets for Discontinued Business",
            "Total Long Term Assets",
            "Total Assets (Calculated)",
            "Total Assets (Actual)",
            "Total Assets (Error)",
            "Short-term debt",
            "Leases",
            "Accounts/Trade Payables",
            "Accrued liabilities",
            "Income taxes payable",
            "Deferred Tax or Revenues",
            "Other Current Liabilities",
            "Total Current Liabilities(Actual)",
            "Total Current Liabilities(Calculated)",
            "Total Current Liabilities(Error)",
            "Lease Liabilities",
            "Long Term Debt",
            "Pensions Liability",
            "Deferred Tax or Revenues",
            "Other noncurrent liabilities",
            "Total Long Term Liabilties",
            "Total liabilities",
            "Redeemable noncontrolling interest",
            "Total equity",
            "Total liabilities, redeemable noncontrolling interest and equity(Actual)",
            "Total liabilities, redeemable noncontrolling interest and equity(Calculated)",
            "Total liabilities, redeemable noncontrolling interest and equity(Error)",
        ],
        standerdized_data_columns[1]: [
            str(sum([cash_data_breakdown_col1, market_data_breakdown_col1])),
            str(rights_data_breakdown_col1),
            str(inventories_data_col1),
            str(sum([accounts_data_col1 + other_recievables_data_col1])),
            str(tax_data_col1),
            str(sum([prepaid_expences_data_col1, restricted_cash_data_col1])),
            str(total_current_assets_data_breakdown_col1),
            str(total_current_assets_calculated_col1),
            str(
                total_current_assets_data_breakdown_col1
                - total_current_assets_calculated_col1
            ),
            str(right_of_use_assets_for_operating_leases_data_breakdown_col1),
            str(net_properties_data_breakdown_col1),
            str(real_estate_data_breakdown_col1),
            str(investment_assets_data_breakdown_col1),
            str(investment_in_other_companies_data_breakdown_col1),
            str(pentions_data_breakdown_col1),
            str(
                sum(
                    [
                        goodwill_data_breakdown_col1
                        + Indefinite_lived_and_amortizable_intangible_assets_data_breakdown_col1
                    ]
                )
            ),
            str(
                sum(
                    [
                        deferred_income_taxes_data_breakdown_col1
                        + total_intangible_and_other_assets_data_breakdown_col1
                    ]
                )
            ),
            str(assets_for_discontinued_business_data_breakdown_col1),
            str(total_long_term_assets_calculated_col1),
            str(total_assets_calculated_col1),
            str(total_assets_data_breakdown_col1),
            str(total_assets_error_col1),
            str(current_portion_of_long_term_debt_data_breakdown_col1),
            str(current_portion_of_operating_lease_liability_breakdown_col1),
            str(accounts_payable_data_breakdown_col1),
            str(accrued_liabilities_breakdown_col1),
            str(income_taxes_payable_breakdown_col1),
            str(deferred_income_taxes_data_breakdown_col1),
            str(contracts_payable_for_programming_rights_data_breakdown_col1),
            str(total_current_liabilities_data_breakdown_col1),
            str(total_current_liabilities_calculated_col1),
            str(total_current_liabilities_error_col1),
            str(lease_liabilities_data_breakdown_col1),
            str(long_term_debt_data_breakdown_col1),
            str(pension_liabilities_data_breakdown_col1),
            str(deferred_income_tax_liability_data_breakdown_col1),
            str(
                long_term_tax_liabilities_data_breakdown_col1
                + other_noncurrent_liabilities_data_breakdown_col1
            ),
            str(total_long_term_liabilities_calculated_col1),
            str(
                total_current_liabilities_data_breakdown_col1
                + total_long_term_liabilities_calculated_col1
            ),
            str(redeemable_noncontrolling_interest_data_breakdown_col1),
            str(total_equity_data_breakdown_col1),
            str(
                total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown_col1
            ),
            str(
                total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown_calculated_col1
            ),
            str(
                total_liabilities_redeemable_noncontrolling_interest_and_equity_error_col1
            ),
        ],
        standerdized_data_columns[2]: [
            str(sum([cash_data_breakdown_col2, market_data_breakdown_col2])),
            str(rights_data_breakdown_col2),
            str(inventories_data_col2),
            str(sum([accounts_data_col2 + other_recievables_data_col2])),
            str(tax_data_col2),
            str(sum([prepaid_expences_data_col2, restricted_cash_data_col2])),
            str(total_current_assets_data_breakdown_col2),
            str(total_current_assets_calculated_col2),
            str(
                total_current_assets_data_breakdown_col2
                - total_current_assets_calculated_col2
            ),
            str(right_of_use_assets_for_operating_leases_data_breakdown_col2),
            str(net_properties_data_breakdown_col2),
            str(real_estate_data_breakdown_col2),
            str(investment_assets_data_breakdown_col2),
            str(investment_in_other_companies_data_breakdown_col2),
            str(pentions_data_breakdown_col2),
            str(
                sum(
                    [
                        goodwill_data_breakdown_col2
                        + Indefinite_lived_and_amortizable_intangible_assets_data_breakdown_col2
                    ]
                )
            ),
            str(
                sum(
                    [
                        deferred_income_taxes_data_breakdown_col2
                        + total_intangible_and_other_assets_data_breakdown_col2
                    ]
                )
            ),
            str(assets_for_discontinued_business_data_breakdown_col2),
            str(total_long_term_assets_calculated_col2),
            str(total_assets_calculated_col2),
            str(total_assets_data_breakdown_col2),
            str(total_assets_error_col2),
            str(current_portion_of_long_term_debt_data_breakdown_col1),
            str(current_portion_of_operating_lease_liability_breakdown_col2),
            str(accounts_payable_data_breakdown_col2),
            str(accrued_liabilities_breakdown_col2),
            str(income_taxes_payable_breakdown_col2),
            str(deferred_income_taxes_data_breakdown_col2),
            str(contracts_payable_for_programming_rights_data_breakdown_col2),
            str(total_current_liabilities_data_breakdown_col2),
            str(total_current_liabilities_calculated_col2),
            str(total_current_liabilities_error_col2),
            str(lease_liabilities_data_breakdown_col2),
            str(long_term_debt_data_breakdown_col2),
            str(pension_liabilities_data_breakdown_col2),
            str(deferred_income_tax_liability_data_breakdown_col2),
            str(
                long_term_tax_liabilities_data_breakdown_col2
                + other_noncurrent_liabilities_data_breakdown_col2
            ),
            str(total_long_term_liabilities_calculated_col2),
            str(
                total_current_liabilities_data_breakdown_col2
                + total_long_term_liabilities_calculated_col2
            ),
            str(redeemable_noncontrolling_interest_data_breakdown_col2),
            str(total_equity_data_breakdown_col2),
            str(
                total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown_col2
            ),
            str(
                total_liabilities_redeemable_noncontrolling_interest_and_equity_data_breakdown_calculated_col2
            ),
            str(
                total_liabilities_redeemable_noncontrolling_interest_and_equity_error_col2
            ),
        ],
    }
)


print("*" * 50)
print("Original Balance Sheet Data is : ")
print(reference_bs_data)
print("*" * 50)
print("Standerdized Balance Sheet Data is : ")
print(standerdized_data)
print("*" * 50)
with st.spinner("Application Loading..."):
    time.sleep(5)
    (
        original_balance_sheet_tab,
        standerdized_balance_sheet_tab,
        assets_breakdown_sheet_tab,
        liabilities_breakdown_sheet_tab,
    ) = st.tabs(
        [
            "Original Balance Sheet Preview",
            "Standerdized Balance Sheet Preview",
            "Assets Breakdown",
            "Liabilities Breakdown",
        ]
    )
    with original_balance_sheet_tab:
        with st.expander("Expand to preview Original Balance Sheet Data"):
            st.dataframe(reference_bs_data)
    with standerdized_balance_sheet_tab:
        with st.expander("Expand to preview Original Standerdized Balance Sheet Data"):
            st.dataframe(standerdized_data)

    with assets_breakdown_sheet_tab:
        st.subheader("Current Assets Breakdown")
        with st.expander("Cash Equivalents & Marketable Securities"):
            cash_eqv_and_market = pd.concat(
                [cash_data_breakdown, market_data_breakdown], axis=0
            )
            # Droppping Duplicate Data
            cash_eqv_and_market = cash_eqv_and_market.drop_duplicates()
            st.dataframe(cash_eqv_and_market)

        with st.expander("Accounts/Trade Receivables"):
            accounts_trade_receivables_breakdown_data = pd.concat(
                [accounts_data_breakdown, other_recievables_data_breakdown], axis=0
            )
            accounts_trade_receivables_breakdown_data = (
                accounts_trade_receivables_breakdown_data.reset_index()
            )
            accounts_trade_receivables_breakdown_data.drop(
                ["index"], axis=1, inplace=True
            )
            accounts_trade_receivables_breakdown_data.drop_duplicates(inplace=True)
            st.dataframe(accounts_trade_receivables_breakdown_data)

        with st.expander("Other Current Assets"):
            other_current_assets_breakdown = pd.concat(
                [prepaid_expences_data_breakdown], axis=0
            )
            other_current_assets_breakdown = (
                other_current_assets_breakdown.reset_index()
            )
            other_current_assets_breakdown.drop(["index"], axis=1, inplace=True)
            other_current_assets_breakdown.drop_duplicates(inplace=True)
            st.dataframe(other_current_assets_breakdown)

        st.subheader("Long-Term Assets Breakdown")
        with st.expander("Intangable Assets"):
            intangable_assets_breakdown = pd.concat(
                [
                    goodwill_data_breakdown,
                    Indefinite_lived_and_amortizable_intangible_assets_data_breakdown,
                ],
                axis=0,
            )
            intangable_assets_breakdown = intangable_assets_breakdown.reset_index()
            intangable_assets_breakdown.drop(["index"], axis=1, inplace=True)
            intangable_assets_breakdown.drop_duplicates(inplace=True)
            st.dataframe(intangable_assets_breakdown)
        with st.expander("Other Assets"):
            other_assets_breakdown_view = pd.concat(
                [
                    deferred_income_taxes_data_breakdown,
                    total_intangible_and_other_assets_data_breakdown,
                ],
                axis=0,
            )
            other_assets_breakdown_view = other_assets_breakdown_view.reset_index()
            other_assets_breakdown_view.drop(["index"], axis=1, inplace=True)
            other_assets_breakdown_view.drop_duplicates(inplace=True)
            st.dataframe(other_assets_breakdown_view)

    with liabilities_breakdown_sheet_tab:
        with st.expander("Other Non-Current Liabilities"):
            other_noncurrent_liabilities_breakdown_view = pd.concat(
                [
                    long_term_tax_liabilities_data_breakdown,
                    other_noncurrent_liabilities_data_breakdown,
                ],
                axis=0,
            )
            other_noncurrent_liabilities_breakdown_view = (
                other_noncurrent_liabilities_breakdown_view.reset_index()
            )
            other_noncurrent_liabilities_breakdown_view.drop(
                ["index"], axis=1, inplace=True
            )
            other_noncurrent_liabilities_breakdown_view.drop_duplicates(inplace=True)
            st.dataframe(other_noncurrent_liabilities_breakdown_view)

    def convert_df(df):
        # IMPORTANT: Cache the conversion to prevent computation on every rerun
        return df.to_csv().encode("utf-8")

    original_balance_sheet_data = convert_df(reference_bs_data)
    final_standerdized_balance_sheet_data = convert_df(standerdized_data)
    st.download_button(
        label="Download Original Data",
        data=original_balance_sheet_data,
        file_name="original_data.csv",
        mime="text/csv",
    )
    st.download_button(
        label="Download Standerdized Data",
        data=final_standerdized_balance_sheet_data,
        file_name="standerdized_data.csv",
        mime="text/csv",
    )
    st.success("Loading Successful!")

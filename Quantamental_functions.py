import re
import gc
import requests
import time
import pickle
import warnings
import concurrent.futures

import pandas as pd
import yfinance as yf
import numpy as np

from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from selenium import webdriver # type: ignore
from selenium.webdriver.chrome.service import Service # type: ignore
from selenium.webdriver.common.by import By # type: ignore
from selenium.webdriver.support.ui import WebDriverWait # type: ignore
from selenium.webdriver.support import expected_conditions as EC # type: ignore
from selenium.common.exceptions import TimeoutException, NoSuchElementException # type: ignore
from selenium.webdriver.chrome.options import Options # type: ignore
from selenium_stealth import stealth # type: ignore

warnings.filterwarnings('ignore', category=FutureWarning)

headers = {"User-Agent": "<username@gmail.com>", "Accept": "application/json"} #modify using your email
keywords_method1 = pd.read_excel('Data/DataRaw/Duplicate_keywords.xlsx', header=0, sheet_name='Method1')
keywords_method4 = pd.read_excel('Data/DataRaw/Duplicate_keywords.xlsx', header=0, sheet_name='Method4')


def get_data(ticker, start_date, end_date):
    """
    Fetch historical stock data for a given ticker from Yahoo Finance
    """
    try:
        data = yf.download(ticker, start=start_date, end=end_date)['Close']
        data = data.dropna()
        return data
    except Exception:
        return None


def get_accession_numbers(cik, form_type, dateb):
    base_url = "https://www.sec.gov"
    search_url = f"{base_url}/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type={form_type}&dateb={dateb}&owner=exclude&count=100&search_text="
    chromedriver_path = "/usr/local/bin/chromedriver"
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.142 Safari/537.36')
    service = Service(chromedriver_path)
    driver = webdriver.Chrome(service=service, options=options)
    stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win64",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
            )
    accession_numbers = []
    try:
        driver.get(search_url)
        wait = WebDriverWait(driver, 30)
        try:
            filings_table = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'tableFile2')))
        except TimeoutException:
            print("Timeout while waiting for the filings table to load.")
            #driver.save_screenshot('timeout_error_screenshot.png')
            raise
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        filings_table = soup.find('table', {'class': 'tableFile2'})
        if not filings_table:
            print(f"No filings found for CIK {cik} with form type {form_type} up to date {dateb}")
            raise Exception(f"No filings found for CIK {cik} with form type {form_type} up to date {dateb}")
        rows = filings_table.find_all('tr')[1:]
        for row in rows:
            description_cell = row.find('td', class_='small')
            if description_cell:
                acc_no_text = description_cell.get_text()
                acc_no_match = re.search(r'Acc-no:\s*(\d+-\d+-\d+)', acc_no_text)
                if acc_no_match:
                    acc_no = acc_no_match.group(1).replace('-', '')
                    accession_numbers.append(acc_no)
    except TimeoutException as e:
        print(f"Timeout occurred: {e}")
    except NoSuchElementException as e:
        print(f"Element not found: {e}")
    finally:
        driver.quit()
    
    return accession_numbers


def generate_dates(start, end):
    start_date = datetime.strptime(start, "%m%d")
    end_date = datetime.strptime(end, "%m%d")
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date.strftime("%m%d"))
        current_date += timedelta(days=1)
    
    return dates


def generate_edgar_urls(cik, accession_numbers, ticker, dateb):
    base_url = "https://www.sec.gov/Archives/edgar/data"
    cik = cik.lstrip('0')
    quarters_groups = [
        ["0929","0930", "1001", "1029", "1030", "1031", "1101", "1102", "1130"],
        ["0630", "0701", "0702", "0730", "0731", "0801", "0802", "0831"],
        ["0228","0331", "0401", "0430", "0501", "0502"]
    ]
    dateb = datetime.strptime(dateb, "%Y%m%d")
    urls = []
    for i, acc_no in enumerate(accession_numbers):
        year_suffix = int(acc_no[10:12])
        base_year = 2000 + year_suffix if year_suffix < 50 else 1900 + year_suffix
        group_index = i % len(quarters_groups)
        quarters = quarters_groups[group_index]
        for quarter in quarters:
            report_date_str = f"{base_year}{quarter}"
            try:
                report_date = datetime.strptime(report_date_str, "%Y%m%d")
            except ValueError:
                continue
            if report_date < dateb:
                base_url_with_date = f"{base_url}/{cik}/{acc_no}/{ticker.lower()}-{report_date_str}.htm"
                urls.append(base_url_with_date)
                modified_url_x10q = base_url_with_date.replace(".htm", "x10q.htm")
                urls.append(modified_url_x10q)
                parts = modified_url_x10q.split('-')
                if len(parts) > 1:
                    report_date_part = parts[-1].replace('x10q.htm', '')
                    if report_date_part[4] == '0':
                        report_date_part = report_date_part[:4] + report_date_part[5:]
                        further_modified_url = '-'.join(parts[:-1]) + '-' + report_date_part + "x10q.htm"
                        urls.append(further_modified_url)
                additional_url = f"{base_url}/{cik}/{acc_no}/{ticker.lower()}-10q_{report_date_str}.htm"
                urls.append(additional_url)
                extra_url = f"{base_url}/{cik}/{acc_no}/{ticker.lower()}{report_date_str}_10q.htm"
                urls.append(extra_url)
        if (i + 1) % len(quarters_groups) == 0:
            base_year -= 1
    
    return urls


#In process_urls: break_limit = 254 if month in ['09', '10', '11'] else 325 if month in ['02', '03', '04', '05'] else 320
def generate_edgar_urls_extended(cik, accession_numbers, ticker, dateb):
    base_url = "https://www.sec.gov/Archives/edgar/data"
    cik = cik.lstrip('0')
    quarters_groups = [
        generate_dates("0929", "1130"),
        generate_dates("0629", "0831"),
        generate_dates("0227", "0502")
    ]
    dateb = datetime.strptime(dateb, "%Y%m%d")
    urls = []
    for i, acc_no in enumerate(accession_numbers):
        year_suffix = int(acc_no[10:12])
        base_year = 2000 + year_suffix if year_suffix < 50 else 1900 + year_suffix
        group_index = i % len(quarters_groups)
        quarters = quarters_groups[group_index]
        for quarter in quarters:
            report_date_str = f"{base_year}{quarter}"
            try:
                report_date = datetime.strptime(report_date_str, "%Y%m%d")
            except ValueError:
                continue
            if report_date < dateb:
                base_url_with_date = f"{base_url}/{cik}/{acc_no}/{ticker.lower()}-{report_date_str}.htm"
                urls.append(base_url_with_date)
                modified_url_x10q = base_url_with_date.replace(".htm", "x10q.htm")
                urls.append(modified_url_x10q)
                parts = modified_url_x10q.split('-')
                if len(parts) > 1:
                    report_date_part = parts[-1].replace('x10q.htm', '')
                    if report_date_part[4] == '0':
                        report_date_part = report_date_part[:4] + report_date_part[5:]
                        further_modified_url = '-'.join(parts[:-1]) + '-' + report_date_part + "x10q.htm"
                        urls.append(further_modified_url)
                additional_url = f"{base_url}/{cik}/{acc_no}/{ticker.lower()}-10q_{report_date_str}.htm"
                urls.append(additional_url)
                extra_url = f"{base_url}/{cik}/{acc_no}/{ticker.lower()}{report_date_str}_10q.htm"
                urls.append(extra_url)
        if (i + 1) % len(quarters_groups) == 0:
            base_year -= 1
    
    return urls


def extract_accno(url):
    parts = url.split('/')
    if len(parts) > 5:
        return parts[7]
    
    return None


def fetch_10q_document(url):
    chromedriver_path = "/usr/local/bin/chromedriver"
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.142 Safari/537.36')
    service = Service(chromedriver_path)
    driver = webdriver.Chrome(service=service, options=options)
    stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win64",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
            )
    document_content = None
    try:
        driver.get(url)
        wait = WebDriverWait(driver, 30)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        document_content = soup.prettify()
    except TimeoutException as e:
        print(f"Timeout occurred: {e}")
    except NoSuchElementException as e:
        print(f"Element not found: {e}")
    finally:
        driver.quit()
    
    return document_content


def extract_date(url):
    match = re.search(
        r'-(\d{4})(\d{2})(\d{2})x10q\.htm|-(\d{4})(\d{2})(\d{2})\.htm|-(\d{4})(\d)(\d{2})x10q\.htm|-(\d{4})_(\d{2})(\d{2})x10q\.htm|-10q_(\d{4})(\d{2})(\d{2})\.htm|(\d{4})(\d{2})(\d{2})_10q\.htm', url)
    if match:
        if match.group(1):
            year, month, day = match.group(1), match.group(2), match.group(3)
        elif match.group(4):
            year, month, day = match.group(4), match.group(5), match.group(6)
        elif match.group(7):
            year, month, day = match.group(7), match.group(8), match.group(9)
        elif match.group(10):
            year, month, day = match.group(10), match.group(11), match.group(12)
        elif match.group(13):
            year, month, day = match.group(13), match.group(14), match.group(15)
        elif match.group(16):
            year, month, day = match.group(16), match.group(17), match.group(18)
        date_str = f"{year}{month}{day}"
        date_obj = datetime.strptime(date_str, "%Y%m%d")
        formatted_date = date_obj.strftime("%d-%m-%Y")
        
        return formatted_date
    
    return None


def clean_description(desc):
    desc = re.sub(r'\(.*?\)', '', desc)
    desc = desc.lower()
    desc = re.sub(r'[^a-z\s]', '', desc)
    desc = re.sub(r'\s+', ' ', desc).strip()
    
    return desc


def rename_duplicates(df):
    duplicates = dict(zip(keywords_method4.iloc[:, 0], keywords_method4.iloc[:, 1]))
    df['Description'] = df['Description'].replace(duplicates)
    
    return df


def filter_description(df):
    terms_to_keep = [
        "total assets", "tangible assets", "total current assets", "long term debt", "total current liabilities", "accounts payable", "accrued expenses and other",
        "cash and cash equivalents", "marketable securities", "total shareholders equity", "total liabilities and equity", 
        "retained earnings", "goodwill", "accounts receivable", "inventories", "operating leases",

        "cash flow from operations", "cash flow from investing", "cash flow from financing", "change in cash", "capex", "depreciation and amortization",
        
        "total revenues", "basic earnings per share", "diluted earnings per share", "net income", "cost of sales", "total operating expenses", "operating income",
        "total non operating expenses", "income before income taxes"
    ]
    df = df[df['Description'].isin(terms_to_keep)]

    return df


def clean_df(df, url):
    year_found = False
    year_col = None
    target_column = None
    date_str = extract_date(url)
    extracted_year = str(int(date_str.split('-')[2]))
    df.dropna(axis=0, how='all', inplace=True)
    df.dropna(axis=1, how='all', inplace=True)
    df.columns = ['Description'] + [f'Column_{i}' for i in range(1, len(df.columns))]
    df['Description'] = df['Description'].astype(str)
    df = df[~df['Description'].str.lower().str.startswith("common stock") & ~df['Description'].str.lower().str.startswith("treasury stock") & ~df['Description'].str.lower().str.startswith("class a common stock") 
            & ~df['Description'].str.lower().str.startswith("class b common stock") & ~df['Description'].str.lower().str.startswith("shares:") & 
            ~df['Description'].str.contains("Preferred stock|additional paid-in capital|preferred stock", case=False)]
    for col in df.columns[1:]:
        column_values = df[col].astype(str).str.strip()
        if column_values.str.contains(extracted_year).any():
            year_col = col
            year_found = True
            break
    df = df[df['Description'].notna()]
    if year_found:
        for col in df.columns[df.columns.get_loc(year_col):]:
            column_values = df[col].astype(str).str.strip()
            if column_values.str.contains(r'\$').any():
                next_col_idx = df.columns.get_loc(col) + 1
                if next_col_idx < len(df.columns):
                    target_column = df.columns[next_col_idx]
                break
    if target_column is None:
        for col in df.columns[1:]:
            column_values = df[col].astype(str).str.strip()
            if pd.to_numeric(column_values, errors='coerce').notna().sum() > 0:
                target_column = col
                break
    df['Description'] = df['Description'].str.strip().apply(clean_description)
    df = rename_duplicates(df)
    df['Description'] = df['Description'].str.strip().apply(lambda x: 'net income' if x.lower() == 'income' else x.strip().lower())
    df['Description'] = df['Description'].str.strip().apply(lambda x: 'accounts receivable' if x.lower().startswith('accounts receivable') else x.strip().lower())
    df['Description'] = df['Description'].str.strip().apply(lambda x: 'accounts receivable' if x.lower().startswith('receivables') else x.strip().lower())
    df['Description'] = df['Description'].str.strip().apply(lambda x: 'accounts receivable' if x.lower().startswith('trade accounts receivable') else x.strip().lower())
    df['Description'] = df['Description'].str.strip().apply(lambda x: 'accounts receivable' if x.lower().startswith('trade receivables') else x.strip().lower())
    df['Description'] = df['Description'].str.strip().apply(lambda x: 'accounts payable' if x.lower().startswith('accounts payable') else x.strip().lower())
    df['Description'] = df['Description'].str.strip().apply(lambda x: 'tangible assets' if x.lower().startswith('property and equipment') else x.strip().lower())
    df['Description'] = df['Description'].str.strip().apply(lambda x: 'tangible assets' if x.lower().startswith('property plant and equipment') else x.strip().lower())
    if 'basic' in df['Description'].values:
        basic_row = df[df['Description'] == 'basic']
        if basic_row.iloc[:, 1:].map(lambda x: isinstance(x, float)).any().any():
            df.loc[df['Description'] == 'basic', 'Description'] = 'basic earnings per share'  
    if 'diluted' in df['Description'].values:
        diluted_row = df[df['Description'] == 'diluted']
        if diluted_row.iloc[:, 1:].map(lambda x: isinstance(x, float)).any().any():
            df.loc[df['Description'] == 'diluted', 'Description'] = 'diluted earnings per share'
    df = filter_description(df)
    df['Value'] = df[target_column]
    df = df[['Description', 'Value']]
    df.dropna(subset=['Value'], inplace=True)
    df.drop_duplicates(subset=['Description'], inplace=True)
    df['Value'] = df['Value'].astype(str)
    df['Value'] = df['Value'].str.replace('—', '0')
    df['Value'] = df['Value'].str.replace('-', '0')
    df['Value'] = df['Value'].str.replace(r'\s*\(\s*', '-', regex=True)
    df['Value'] = df['Value'].str.replace(r'\s*\)\s*', '', regex=True)
    df['Value'] = df['Value'].str.replace(',', '')
    df['Value'] = df['Value'].astype(float)
    
    return df


def update_df(consolidated_df, df, date_str):
    df.rename(columns={'Value': date_str}, inplace=True)
    consolidated_df = pd.merge(consolidated_df, df, on='Description', how='outer') if not consolidated_df.empty else df
    
    return consolidated_df


def extract_cf(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    tables = soup.find_all('table')
    search_phrases = [
        "Net cash from operations",
        "Cash generated by operating activities",
        "Net cash provided by operating activities",
        "Net cash provided by (used in) operating activities",
        "Net cash provided by (used for) operating activities",
        "Net cash used for operating activities",
        "Net cash used in operating activities",
        "Cash provided by operating activities",
        "Cash provided (used) by operations",
        "Net Cash Provided by Operating Activities",
        "Net Cash Provided By Operating Activities",
        "Net Cash Provided By (Used In) Operating Activities",
        "Net Cash Used for Operating Activities",
        "Net Cash Used In Operating Activities",
        "Net Cash Provided by (Used for) Operating Activities",
        "Net Cash (Used in) Provided by Operating Activities",
        "Net Cash Used in Operating Activities",
        "Net Cash Provided by (Used in) Operating Activities",
        "Net cash (used in)/provided by operating activities",
        "Net cash provided by/(used in) operating activities",
        "Cash flows from operating activities"
    ]
    for table in tables:
        table_text = table.get_text()
        if any(phrase in table_text for phrase in search_phrases):
            try:
                df = pd.read_html(str(table))[0]
                return df
            except ValueError:
                continue
    
    return None


def extract_bs(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    tables = soup.find_all('table')
    search_phrases = [
        "Total assets",
        "Total Assets",
        "TOTAL ASSETS"
    ]
    for table in tables:
        table_text = table.get_text()
        if any(phrase in table_text for phrase in search_phrases):
            try:
                df = pd.read_html(str(table))[0]
                return df
            except ValueError:
                continue
    
    return None


def extract_is(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    tables = soup.find_all('table')
    search_phrases = [
        "Income before income taxes",
        "Income before provision for income taxes",
        "Income (loss) before income taxes",
        "Income from continuing operations before income taxes",
        "Income (loss) from continuing operations before income taxes",
        "Income from operations, before income taxes",
        "Income from continuing operations, before income taxes",
        "Earnings before provision for income taxes",
        "Income before income tax",
        "Consolidated profit before taxes",
        "Income Before Income Taxes",
        "Loss before income taxes",
        "Loss before provision for income taxes",
        "Income (Loss) Before Income Taxes",
        "(Loss) Income Before Income Taxes",
        "Earnings before income taxes",
        "Earnings Before Income Taxes"
    ]
    for table in tables:
        table_text = table.get_text()
        if any(phrase in table_text for phrase in search_phrases):
            try:
                df = pd.read_html(str(table))[0]
                return df
            except ValueError:
                continue
    
    return None


def get_consolidated_dfs(ciks, tickers, form_type, dateb):
    consolidated_cf_dfs = {}
    consolidated_bs_dfs = {}
    consolidated_is_dfs = {}

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_tickers, cik, ticker, form_type, dateb) for cik, ticker in zip(ciks, tickers)]
        for future in concurrent.futures.as_completed(futures):
            ticker, cf_df, bs_df, is_df = future.result()
            consolidated_cf_dfs[ticker] = cf_df
            consolidated_bs_dfs[ticker] = bs_df
            consolidated_is_dfs[ticker] = is_df

    return consolidated_cf_dfs, consolidated_bs_dfs, consolidated_is_dfs


def process_table(url, extract_func, consolidated_df):
    html_content = fetch_10q_document(url)
    if html_content:
        table = extract_func(html_content)
        if table is not None:
            cleaned_df = clean_df(table, url)
            date_str = extract_date(url)
            consolidated_df = update_df(consolidated_df, cleaned_df, date_str).fillna(0)

    return consolidated_df


def process_urls(urls):
    acc_proc = set()
    acc_notab = set()
    acc_notab_count = {}
    consolidated_cf_df = pd.DataFrame()
    consolidated_bs_df = pd.DataFrame()
    consolidated_is_df = pd.DataFrame()
    for url in urls:
        accession_number = extract_accno(url)
        if accession_number in acc_proc:
            continue
        temp_cf_df = process_table(url, extract_cf, pd.DataFrame())
        temp_bs_df = process_table(url, extract_bs, pd.DataFrame())
        temp_is_df = process_table(url, extract_is, pd.DataFrame())
        if not temp_bs_df.empty:
            total_liabilities_and_equity = temp_bs_df[temp_bs_df['Description'] == 'total liabilities and equity']
            if total_liabilities_and_equity.empty:
                break
        if not temp_cf_df.empty and not temp_bs_df.empty and not temp_is_df.empty:
            consolidated_cf_df = pd.merge(consolidated_cf_df, temp_cf_df, on='Description', how='outer').fillna(0) if not consolidated_cf_df.empty else temp_cf_df.fillna(0)
            consolidated_bs_df = pd.merge(consolidated_bs_df, temp_bs_df, on='Description', how='outer').fillna(0) if not consolidated_bs_df.empty else temp_bs_df.fillna(0)
            consolidated_is_df = pd.merge(consolidated_is_df, temp_is_df, on='Description', how='outer').fillna(0) if not consolidated_is_df.empty else temp_is_df.fillna(0)
            acc_proc.add(accession_number)
            if accession_number in acc_notab:
                acc_notab.remove(accession_number)
            if accession_number in acc_notab_count:
                acc_notab_count[accession_number] = 0
        else:
            acc_notab.add(accession_number)
            if accession_number not in acc_notab_count:
                acc_notab_count[accession_number] = 0
            acc_notab_count[accession_number] += 1
            formatted_date = extract_date(url)
            if formatted_date:
                month = formatted_date.split('-')[1]
                break_limit = 38 if month in ['09', '10', '11'] else 30 if month in ['02', '03', '04', '05'] else 40
                if acc_notab_count[accession_number] >= break_limit:
                    break
    
    return consolidated_cf_df, consolidated_bs_df, consolidated_is_df


def process_tickers(cik, ticker, form_type, dateb):
    accession_numbers = get_accession_numbers(cik, form_type, dateb)
    urls = generate_edgar_urls(cik, accession_numbers, ticker, dateb)
    cf_df, bs_df, is_df = process_urls(urls)
    print(f"CF DataFrame for {ticker}")
    print(f"BS DataFrame for {ticker}")
    print(f"IS DataFrame for {ticker}")

    return ticker, cf_df.fillna(0), bs_df.fillna(0), is_df.fillna(0)


def update_df_tickers(df, html_content, extract_function, url):
    table = extract_function(html_content)
    if table is not None:
        cleaned_df = clean_df(table, url)
        date_str = extract_date(url)
        df = update_df(df, cleaned_df, date_str)
        
    return df


def transpose_df(df):
    melted_df = pd.melt(df, id_vars='Description', var_name='Date', value_name='Value')
    transposed_df = melted_df.pivot(index='Date', columns='Description', values='Value')
    transposed_df.index = pd.to_datetime(transposed_df.index, format="%d-%m-%Y")
    transposed_df.sort_index(inplace=True)
    transposed_df.index = transposed_df.index.strftime("%d-%m-%Y")
    transposed_df.columns.name = None
    
    return transposed_df


def filter_columns(cf_df, bs_df, is_df):
    cf_columns_to_keep = [
        "cash flow from operations", "cash flow from investing", "cash flow from financing", 
        "change in cash", "capex", "depreciation and amortization"
    ]
    bs_columns_to_keep = [
        "total assets", "tangible assets", "total current assets", "long term debt", "total current liabilities", 
        "accounts payable", "accrued expenses and other", "cash and cash equivalents", "marketable securities", 
        "total shareholders equity", "total liabilities and equity", "retained earnings", "goodwill", 
        "accounts receivable", "inventories", "operating leases"
    ]
    is_columns_to_keep = [
        "total revenues", "basic earnings per share", "diluted earnings per share", "net income", 
        "cost of sales", "total operating expenses", "operating income", "total non operating expenses", 
        "income before income taxes"
    ]
    filtered_cf_df = cf_df[[col for col in cf_columns_to_keep if col in cf_df.columns]]
    filtered_bs_df = bs_df[[col for col in bs_columns_to_keep if col in bs_df.columns]]
    filtered_is_df = is_df[[col for col in is_columns_to_keep if col in is_df.columns]]
    filtered_cf_df = filtered_cf_df.sort_index(axis=1)
    filtered_bs_df = filtered_bs_df.sort_index(axis=1)
    filtered_is_df = filtered_is_df.sort_index(axis=1)

    return filtered_cf_df, filtered_bs_df, filtered_is_df


def combine_combo_dfs(consolidated_cf_dfs, consolidated_bs_dfs, consolidated_is_dfs, tickers, bond_yield, start_date, end_date):
    combined_data = {}
    for ticker in tickers:
        share_price = get_data(ticker, start_date, end_date)
        cf_df = transpose_df(consolidated_cf_dfs[ticker])
        bs_df = transpose_df(consolidated_bs_dfs[ticker])
        is_df = transpose_df(consolidated_is_dfs[ticker])
        cf_df, bs_df, is_df = filter_columns(cf_df, bs_df, is_df)
        columns_to_check = {"marketable securities": 0, "accrued expenses and other": 0, "depreciation and amortization": 0, "tangible assets": 0}
        is_df['nshares'] = (is_df['net income'] / is_df['diluted earnings per share']).astype(int)
        combo_df = pd.concat([cf_df, bs_df, is_df], axis=1)
        combo_df = combo_df.sort_index(axis=1)
        combo_df_dates = combo_df.index.to_series().unique()
        combo_df = combo_df.assign(bond_yield=combo_df.index.to_series().map(lambda date: bond_yield.asof(date)))
        combo_df = combo_df.assign(share_price=combo_df.index.to_series().map(lambda date: share_price.asof(date)))
        combo_df["eps growth rate"] = combo_df['diluted earnings per share'].pct_change().fillna(0)
        combo_df["intrinsic value"] = combo_df['diluted earnings per share'] * (8.5 + 2 * combo_df["eps growth rate"]) * 4.4 / combo_df['bond_yield']
        combo_df["owner earnings"] = combo_df["net income"] - combo_df["capex"]
        for column in columns_to_check:
            if column not in combo_df.columns:
                combo_df[column] = columns_to_check[column]
        combo_df["invested capital"] = (combo_df["total assets"] - combo_df["cash and cash equivalents"] - combo_df["marketable securities"] + combo_df["accounts payable"] + combo_df["accrued expenses and other"])
        combo_df["roic"] = combo_df["owner earnings"] / combo_df["invested capital"]
        combo_df["current ratio"] = combo_df["total current assets"] / combo_df["total current liabilities"]
        combo_df["net current assets"] = combo_df["total current assets"] - combo_df["total current liabilities"]
        combo_df["bvps"] = combo_df["total shareholders equity"] / combo_df["nshares"]
        combo_df["pe ratio"] = combo_df["share_price"] / combo_df["diluted earnings per share"]
        combo_df["pb ratio"] = combo_df["share_price"] / (combo_df["total shareholders equity"] / combo_df["nshares"])
        combo_df["multiplier pb"] = combo_df["pe ratio"] * combo_df["pb ratio"]
        combo_df["margin safety"] = 1 - (combo_df["share_price"] / combo_df["intrinsic value"])
        combo_df["total debt"] = combo_df["total liabilities and equity"] - combo_df["total shareholders equity"]
        combo_df["tbvps"] = combo_df["tangible assets"] / combo_df["nshares"]
        combo_df["ncav"] = (combo_df["total current assets"] - combo_df["total debt"]) / combo_df["nshares"]
        combined_data[ticker] = {
            "combined_df": combo_df,
            "dates": combo_df_dates
        }

    return combined_data


def load_pickle(file_path):
    file_name = f'Data/DataDerived/{file_path}'
    with open(file_name, 'rb') as f:

        return pickle.load(f)


def fetch_company_info():
    url = "https://www.sec.gov/files/company_tickers.json"
    response = requests.get(url, headers=headers)
    if response.ok:
        data = response.json()
        company_info = pd.DataFrame(data).T
        company_info = company_info.drop_duplicates(subset='cik_str', keep='first')
        company_info['title'] = company_info['title'].str.upper()

        return company_info
    else:
        print(f"Failed to retrieve data, status code: {response.status_code}")
        return None
    

def fetch_form_accno():
    company_info = fetch_company_info()
    url_submissions = []
    url_facts = []
    cik_data = {}
    for i in range(len(company_info)):
        CIK = company_info['cik_str'][i]
        url_submission = f"https://data.sec.gov/submissions/CIK{str(CIK).zfill(10)}.json"
        url_submissions.append(url_submission)
        url_fact = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{str(CIK).zfill(10)}.json"
        url_facts.append(url_fact)

    def fetch_url(url, cik_str):
        response = requests.get(url, headers=headers)
        if response.ok:
            data = response.json()
            forms = data['filings']['recent']['form']
            accessionNumbers = data['filings']['recent']['accessionNumber']
            filtered_data = [acc_num for form, acc_num in zip(forms, accessionNumbers) if form == "10-Q"]
            return cik_str, filtered_data
        else:
            return cik_str, []
        
    #total_tasks = len(url_facts)
    #completed_tasks = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_url = {executor.submit(fetch_url, url, company_info['cik_str'][i]): i for i, url in enumerate(url_submissions)}
        for future in concurrent.futures.as_completed(future_to_url):
            cik_str, filtered_data = future.result()
            if filtered_data:
                cik_data[cik_str] = {'accessionNumber': filtered_data, 'facts': []}
            #completed_tasks += 1
            #print(f"Completed: {completed_tasks}/{total_tasks}")

    with open('Data/DataDerived/accno_data.pkl', 'wb') as f:
        pickle.dump((cik_data, cik_data, url_facts, url_submissions), f)

    del cik_data
    del url_facts
    del url_submissions
    gc.collect()

    return 'Data/DataDerived/accno_data.pkl'


def fetch_data(fact, period):
    url = f"https://data.sec.gov/api/xbrl/frames/us-gaap/{fact}/USD/{period}.json"
    response = requests.get(url, headers=headers)
    if response.ok:
        return response.json()
    

def fetch_data_parallel(fact, period):
    data = fetch_data(fact, period)

    return fact, period, data

 
def fetch_facts(cik_data, url_facts):
    required_fact_names = set(keywords_method1.iloc[:, 0].tolist())
    total_tasks = len(url_facts)
    completed_tasks = 0

    def process_url(url_fact):
        try:
            response = requests.get(url_fact, headers=headers)
            if response.ok:
                facts_data = response.json()
                for cik, data in cik_data.items():
                    accession_numbers_set = set(data['accessionNumber'])
                    cik_facts = facts_data.get('facts', {})
                    for facts in cik_facts.values():
                        for fact_name, fact_details in facts.items():
                            if fact_name not in required_fact_names:
                                continue
                            for unit_details in fact_details.get('units', {}).values():
                                for detail in unit_details:
                                    temp_frame = detail.get('frame')
                                    if detail['accn'] in accession_numbers_set and temp_frame is None:
                                        val = detail.get('val', 'N/A')
                                        fp = detail.get('fp', 'N/A')
                                        fy = detail.get('fy', 'N/A')
                                        cik_data[cik]['facts'].append({
                                            'fact_name': fact_name,
                                            'val': val,
                                            'fp': fp,
                                            'fy': fy
                                        })
                del facts_data
                gc.collect()
                return True
            else:
                return None
        except:
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_url = {executor.submit(process_url, url): url for url in url_facts}
        for future in concurrent.futures.as_completed(future_to_url):
            result = future.result()
            if result:
                completed_tasks += 1
                print(f"Completed: {completed_tasks}/{total_tasks}")
            time.sleep(0.1)

    with open('Data/DataDerived/facts_data.pkl', 'wb') as f:
        pickle.dump((cik_data), f)

    return 'Data/DataDerived/facts_data.pkl'


def fetch_concept_url(cik_data):
    required_fact_names = set(keywords_method1.iloc[:, 0].tolist())
    positive_responses = []
    total_tasks = len(cik_data) * len(required_fact_names)
    completed_tasks = 0
    delay = 1 / 10

    def fetch_links(cik, fact_name):
        url_concept = f"https://data.sec.gov/api/xbrl/companyconcept/CIK{str(cik).zfill(10)}/us-gaap/{fact_name}.json"
        try:
            response = requests.get(url_concept, headers=headers)
            if response.ok:
                return url_concept
            else:
                return None
        except requests.exceptions.RequestException:
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for cik in cik_data.keys():
            for fact_name in required_fact_names:
                futures.append(executor.submit(fetch_links, cik, fact_name))
                time.sleep(delay)
                completed_tasks += 1
                if completed_tasks % 1000 == 0:
                    print(f"Completed: {completed_tasks}/{total_tasks}")

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                positive_responses.append(result)

    with open('Data/DataDerived/concept_urls.pkl', 'wb') as f:
        pickle.dump(positive_responses, f)

    return 'Data/DataDerived/concept_urls.pkl'


def fetch_concept(cik_data, concept_urls):
    total_tasks = len(concept_urls)
    completed_tasks = 0

    def fetch_concept_data(url):
        try:
            response = requests.get(url, headers=headers)
            if response.ok:
                return url, response.json()
            else:
                return url, None
        except requests.exceptions.RequestException:
            return url, None

    def process_concept_data(url, concept_data):
        cik = int(url.split('CIK')[1].split('/')[0])
        fact_name = url.split('us-gaap/')[1].split('.json')[0]
        for _, unit_details in concept_data.get('units', {}).items():
            for detail in unit_details:
                if detail['accn'] in cik_data[cik]['accessionNumber'] and detail.get('frame') is None:
                    cik_data[cik]['facts'].append({
                        'fact_name': fact_name,
                        'val': detail.get('val', 'N/A'),
                        'fp': detail.get('fp', 'N/A'),
                        'fy': detail.get('fy', 'N/A')
                    })

    for url in concept_urls:
        url, concept_data = fetch_concept_data(url)
        if concept_data:
            process_concept_data(url, concept_data)
        completed_tasks += 1
        if completed_tasks % 100 == 0:
            print(f"Completed: {completed_tasks}/{total_tasks}")
        del concept_data
        gc.collect()

    with open('Data/DataDerived/concept_data.pkl', 'wb') as f:
        pickle.dump(cik_data, f)

    return 'Data/DataDerived/concept_data.pkl'


def calculate_financial_metrics(df, bond_yield, share_price):
    default_columns = {
        "marketable securities": 0,
        "accrued expenses and other": 0
    }
    for column, default_value in default_columns.items():
        if column not in df.columns:
            df[column] = default_value
    df['bond_yield'] = df.index.map(bond_yield.asof)
    if share_price is not None and not share_price.empty:
        df['share_price'] = df.index.map(share_price.asof)
        df['share_price'] = df['share_price'].ffill().bfill()
    if 'diluted earnings per share' in df.columns:
        df['eps growth rate'] = df['diluted earnings per share'].pct_change().fillna(0)
    if 'diluted earnings per share' in df.columns and 'bond_yield' in df.columns:
        df['intrinsic value'] = df['diluted earnings per share'] * (8.5 + 2 * df['eps growth rate']) * 4.4 / df['bond_yield']
    if 'share_price' in df.columns and 'intrinsic value' in df.columns:
        df['margin safety'] = 1 - (df['share_price'] / df['intrinsic value'])
    if 'total current assets' in df.columns and 'total current liabilities' in df.columns:
        df['current ratio'] = df['total current assets'] / df['total current liabilities']
        df['net current assets'] = df['total current assets'] - df['total current liabilities']
    if 'share_price' in df.columns and 'diluted earnings per share' in df.columns:
        df['pe ratio'] = df['share_price'] / df['diluted earnings per share']
    if 'capex' in df.columns and 'depreciation and amortization' in df.columns and 'net income' in df.columns:
        df['owner earnings'] = df['net income'] - df['capex']
    if {'total assets', 'cash and cash equivalents', 'accounts payable'}.issubset(df.columns):
        df['invested capital'] = (
            df['total assets'] - df['cash and cash equivalents'] - df['marketable securities'] + 
            df['accounts payable'] + df['accrued expenses and other']
        )
        if 'owner earnings' in df.columns:
            df['roic'] = df['owner earnings'] / df['invested capital']
    if 'total shareholders equity' in df.columns and 'nshares' in df.columns:
        df['bvps'] = df['total shareholders equity'] / df['nshares']
        if 'share_price' in df.columns:
            df['pb ratio'] = df['share_price'] / df['bvps']
            if 'pe ratio' in df.columns:
                df['multiplier pb'] = df['pe ratio'] * df['pb ratio']
    if 'total liabilities and equity' in df.columns and 'total shareholders equity' in df.columns:
        df['total debt'] = df['total liabilities and equity'] - df['total shareholders equity']
    if 'tangible assets' in df.columns and 'nshares' in df.columns:
        df['tbvps'] = df['tangible assets'] / df['nshares']
    if 'total debt' in df.columns and 'total current assets' in df.columns and 'nshares' in df.columns:
        df['ncav'] = (df['total current assets'] - df['total debt']) / df['nshares']
    df = df.loc[:, ~df.columns.duplicated()]
    
    return df


def convert_fp_to_date(fp, fy):
    fp_to_date = {
        'Q1': '0331',
        'Q2': '0630',
        'Q3': '0930',
        'Q4': '1231'
    }
    fy_str = str(int(fy))

    return pd.to_datetime(f"{fy_str}{fp_to_date[fp]}", format='%Y%m%d')


def convert_quarters(index_name):
    if 'Q' in index_name:
        year_quarter = index_name.split('CY')[1]
        year = year_quarter[:4]
        quarter = year_quarter[4:]
        if quarter == 'Q1':
            return pd.Timestamp(f'{year}-03-31')
        elif quarter == 'Q2':
            return pd.Timestamp(f'{year}-06-30')
        elif quarter == 'Q3':
            return pd.Timestamp(f'{year}-09-30')
        elif quarter == 'Q4':
            return pd.Timestamp(f'{year}-12-31')
        
    return index_name


def df_facts_ticker(cik, ticker_data, bond_yield):
    facts = ticker_data['facts']
    df = pd.DataFrame(facts)
    if df.empty:
        return df
    df = df[(df['fp'] != 'FY') & (df['fy'].notna())]
    df['date'] = df.apply(lambda row: convert_fp_to_date(row['fp'], row['fy']), axis=1)
    df = df.drop(columns=['fp', 'fy'])
    df = df.drop_duplicates(subset=['date', 'fact_name'])
    start_date = df['date'].min()
    end_date = df['date'].max()
    complete_dates = pd.date_range(start=start_date, end=end_date, freq='Q')
    df = df.pivot(index='date', columns='fact_name', values='val')
    df = df.reindex(complete_dates).sort_index()
    df = df.rename_axis(None, axis=0).rename_axis(None, axis=1)
    df = df.dropna(axis=1, how='all')
    df = df.loc[:, (df.sum(axis=0) != 0)]
    df = df.round(2).fillna(0)
    df = transform_df(df)
    df = df.sort_index()
    column_mapping = dict(zip(keywords_method1.iloc[:, 0], keywords_method1.iloc[:, 1]))
    df.rename(columns=column_mapping, inplace=True)
    df = df.reindex(sorted(df.columns), axis=1)
    df = df.replace(0, pd.NA)
    df = df.ffill().bfill()
    ticker = get_ticker_from_cik(cik)
    if ticker is not None:
        share_price = get_data(ticker, start_date, end_date)
        if share_price is not None and not share_price.empty:
            df = df.assign(share_price=share_price)
            df['share_price'] = df['share_price'].ffill().bfill()
            df = calculate_financial_metrics(df, bond_yield, share_price)
        else:
            df = calculate_financial_metrics(df, bond_yield, None)
    else:
        df = calculate_financial_metrics(df, bond_yield, None)
    
    return df.fillna(0)


def dfs_facts_tickers(cik_data, bond_yield, filename_prefix):
    ticker_dfs = {}
    for cik, data in cik_data.items():
        ticker_dfs[cik] = df_facts_ticker(cik, data, bond_yield)
        print(f"Completed {len(ticker_dfs)}/{len(cik_data)}")

    filename = f"Data/DataDerived/{filename_prefix}_df_file.pkl"
    with open(filename, 'wb') as f:
        pickle.dump(ticker_dfs, f)
        
    return filename


def process_data(start_year, end_year):
    quarters = ['Q4', 'Q3', 'Q2', 'Q1']
    cik_database = {}
    futures = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for _, row in keywords_method1.iterrows():
            fact = row[0]
            for year in range(start_year, end_year + 1):
                for quarter in quarters:
                    period = f"CY{year}{quarter}"
                    future = executor.submit(fetch_data_parallel, fact, period)
                    futures.append(future)
        for future in concurrent.futures.as_completed(futures):
            fact, period, data = future.result()
            if data:
                for item in data['data']:
                    cik = item['cik']
                    if cik not in cik_database:
                        cik_database[cik] = {}
                    cik_database[cik][f"{fact}_{period}"] = item['val']

    with open('Data/DataDerived/frames_data.pkl', 'wb') as f:
        pickle.dump(cik_database, f)

    del cik_database
    
    return 'Data/DataDerived/frames_data.pkl'


def transform_df(df):
    if 'CapitalizedComputerSoftwareAmortization' in df.columns and 'CapitalizedComputerSoftwareAmortization1' in df.columns:
        df['CapitalizedComputerSoftwareAmortization'] = df.apply(lambda row: row['CapitalizedComputerSoftwareAmortization1'] if row['CapitalizedComputerSoftwareAmortization'] == 0 else row['CapitalizedComputerSoftwareAmortization'], axis=1)
        df = df.drop(columns=['CapitalizedComputerSoftwareAmortization1'])

    columns_to_sum = ['CapitalizedComputerSoftwareAmortization', 'AmortizationOfIntangibleAssets', 'Depreciation']
    existing_columns = [col for col in columns_to_sum if col in df.columns]
    if existing_columns:
        df['depreciation and amortisation'] = df[existing_columns].sum(axis=1)
        df = df.drop(columns=existing_columns)

    if 'FiniteLivedIntangibleAssetsNet' in df.columns and 'IntangibleAssetsNetExcludingGoodwill' in df.columns:
        df['intangible assets'] = df.apply(lambda row: row['FiniteLivedIntangibleAssetsNet'] if row['IntangibleAssetsNetExcludingGoodwill'] == 0 else row['IntangibleAssetsNetExcludingGoodwill'], axis=1)
        df = df.drop(columns=['FiniteLivedIntangibleAssetsNet', 'IntangibleAssetsNetExcludingGoodwill'])

    if 'EntityCommonStockSharesOutstanding' in df.columns and 'CommonStockSharesOutstanding' in df.columns:
        df['nshares'] = df.apply(lambda row: row['EntityCommonStockSharesOutstanding'] if row['CommonStockSharesOutstanding'] == 0 else row['CommonStockSharesOutstanding'], axis=1)
        df = df.drop(columns=['EntityCommonStockSharesOutstanding', 'CommonStockSharesOutstanding'])

    if 'ShortTermInvestments' not in df.columns:
        if 'CashCashEquivalentsRestrictedCashAndCashRestrictedCashEquivalents' in df.columns and 'CashCashEquivalentsAndShortTermInvestments' in df.columns:
            df['marketable securities'] = df['CashCashEquivalentsAndShortTermInvestments'] - df['CashCashEquivalentsRestrictedCashAndCashRestrictedCashEquivalents']
            df = df.drop(columns=['CashCashEquivalentsAndShortTermInvestments'])
    elif 'CashCashEquivalentsAndShortTermInvestments' in df.columns:
        df = df.drop(columns=['CashCashEquivalentsAndShortTermInvestments'])

    if 'AccountsReceivableNet' in df.columns and 'AccountsReceivableNetCurrent' in df.columns:
        df['accounts receivable'] = df.apply(lambda row: row['AccountsReceivableNetCurrent'] if row['AccountsReceivableNet'] == 0 else row['AccountsReceivableNet'], axis=1)
        df = df.drop(columns=['AccountsReceivableNetCurrent', 'AccountsReceivableNet'])

    if 'LongTermDebt' in df.columns and 'LongTermDebtNoncurrent' in df.columns:
        df['long term debt'] = df.apply(lambda row: row['LongTermDebtNoncurrent'] if row['LongTermDebt'] == 0 else row['LongTermDebt'], axis=1)
        df = df.drop(columns=['LongTermDebtNoncurrent', 'LongTermDebt'])

    if 'OperatingLeaseRightOfUseAsset' in df.columns and 'RightOfUseAssetObtainedInExchangeForOperatingLeaseLiability' in df.columns:
        df['operating leases'] = df.apply(lambda row: row['RightOfUseAssetObtainedInExchangeForOperatingLeaseLiability'] if row['OperatingLeaseRightOfUseAsset'] == 0 else row['OperatingLeaseRightOfUseAsset'], axis=1)
        df = df.drop(columns=['RightOfUseAssetObtainedInExchangeForOperatingLeaseLiability', 'OperatingLeaseRightOfUseAsset'])

    if 'NonoperatingIncomeExpense' in df.columns and 'OtherNonoperatingIncomeExpense' in df.columns:
        df['total non operating expenses'] = df.apply(lambda row: row['OtherNonoperatingIncomeExpense'] if row['NonoperatingIncomeExpense'] == 0 else row['NonoperatingIncomeExpense'], axis=1)
        df = df.drop(columns=['OtherNonoperatingIncomeExpense', 'NonoperatingIncomeExpense'])

    if 'NetIncomeLoss' in df.columns and 'ComprehensiveIncomeNetOfTax' in df.columns:
        df['net income'] = df.apply(lambda row: row['ComprehensiveIncomeNetOfTax'] if row['NetIncomeLoss'] == 0 else row['NetIncomeLoss'], axis=1)
        df = df.drop(columns=['ComprehensiveIncomeNetOfTax', 'NetIncomeLoss'])

    if 'PaymentsToAcquirePropertyPlantAndEquipment' in df.columns and 'NoncashOrPartNonCashAcquisitionFixedAssetsAcquired1' in df.columns:
        df['capex'] = df.apply(lambda row: row['NoncashOrPartNonCashAcquisitionFixedAssetsAcquired1'] if row['PaymentsToAcquirePropertyPlantAndEquipment'] == 0 else row['PaymentsToAcquirePropertyPlantAndEquipment'], axis=1)
        df = df.drop(columns=['NoncashOrPartNonCashAcquisitionFixedAssetsAcquired1', 'PaymentsToAcquirePropertyPlantAndEquipment'])

    if 'CashAndCashEquivalentsPeriodIncreaseDecrease' in df.columns and 'CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect' in df.columns:
        df['change in cash'] = df.apply(lambda row: row['CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect'] if row['CashAndCashEquivalentsPeriodIncreaseDecrease'] == 0 else row['CashAndCashEquivalentsPeriodIncreaseDecrease'], axis=1)
        df = df.drop(columns=['CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect', 'CashAndCashEquivalentsPeriodIncreaseDecrease'])

    if 'NetCashProvidedByUsedInOperatingActivities' in df.columns and 'NetCashProvidedByUsedInOperatingActivitiesContinuingOperations' in df.columns:
        df['cash flow from operations'] = df.apply(lambda row: row['NetCashProvidedByUsedInOperatingActivitiesContinuingOperations'] if row['NetCashProvidedByUsedInOperatingActivities'] == 0 else row['NetCashProvidedByUsedInOperatingActivities'], axis=1)
        df = df.drop(columns=['NetCashProvidedByUsedInOperatingActivitiesContinuingOperations', 'NetCashProvidedByUsedInOperatingActivities'])

    if 'NetCashProvidedByUsedInInvestingActivities' in df.columns and 'NetCashProvidedByUsedInInvestingActivitiesContinuingOperations' in df.columns:
        df['cash flow from investing'] = df.apply(lambda row: row['NetCashProvidedByUsedInInvestingActivitiesContinuingOperations'] if row['NetCashProvidedByUsedInInvestingActivities'] == 0 else row['NetCashProvidedByUsedInInvestingActivities'], axis=1)
        df = df.drop(columns=['NetCashProvidedByUsedInInvestingActivitiesContinuingOperations', 'NetCashProvidedByUsedInInvestingActivities'])

    if 'NetCashProvidedByUsedInFinancingActivities' in df.columns and 'NetCashProvidedByUsedInFinancingActivitiesContinuingOperations' in df.columns:
        df['cash flow from financing'] = df.apply(lambda row: row['NetCashProvidedByUsedInFinancingActivitiesContinuingOperations'] if row['NetCashProvidedByUsedInFinancingActivities'] == 0 else row['NetCashProvidedByUsedInFinancingActivities'], axis=1)
        df = df.drop(columns=['NetCashProvidedByUsedInFinancingActivitiesContinuingOperations', 'NetCashProvidedByUsedInFinancingActivities'])

    columns_to_check = ['Goodwill', 'GoodwillImpairedAccumulatedImpairmentLoss', 'GoodwillImpairmentLoss']
    existing_columns = [col for col in columns_to_check if col in df.columns]
    if len(existing_columns) > 1:
        if 'Goodwill' in df.columns:
            if 'GoodwillImpairmentLoss' in df.columns:
                df['Goodwill'] = df.apply(lambda row: row['GoodwillImpairmentLoss'] if row['Goodwill'] == 0 else row['Goodwill'], axis=1)
            if 'GoodwillImpairedAccumulatedImpairmentLoss' in df.columns:
                df['Goodwill'] = df.apply(lambda row: row['GoodwillImpairedAccumulatedImpairmentLoss'] if row['Goodwill'] == 0 else row['Goodwill'], axis=1)
            df = df.drop(columns=[col for col in columns_to_check if col in df.columns and col != 'Goodwill'])
        else:
            if 'GoodwillImpairmentLoss' in df.columns:
                df['Goodwill'] = df['GoodwillImpairmentLoss']
            elif 'GoodwillImpairedAccumulatedImpairmentLoss' in df.columns:
                df['Goodwill'] = df['GoodwillImpairedAccumulatedImpairmentLoss']
            df = df.drop(columns=[col for col in columns_to_check if col in df.columns and col != 'Goodwill'])

    columns_to_check = ['Revenues', 'SalesRevenueGoodsNet', 'SalesRevenueNet']
    existing_columns = [col for col in columns_to_check if col in df.columns]
    if len(existing_columns) > 1:
        if 'Revenues' in df.columns:
            if 'SalesRevenueNet' in df.columns:
                df['Revenues'] = df.apply(lambda row: row['SalesRevenueNet'] if row['Revenues'] == 0 else row['Revenues'], axis=1)
            if 'SalesRevenueGoodsNet' in df.columns:
                df['Revenues'] = df.apply(lambda row: row['SalesRevenueGoodsNet'] if row['Revenues'] == 0 else row['Revenues'], axis=1)
            df = df.drop(columns=[col for col in columns_to_check if col in df.columns and col != 'Revenues'])
        else:
            if 'SalesRevenueNet' in df.columns:
                df['Revenues'] = df['SalesRevenueNet']
            elif 'SalesRevenueGoodsNet' in df.columns:
                df['Revenues'] = df['SalesRevenueGoodsNet']
            df = df.drop(columns=[col for col in columns_to_check if col in df.columns and col != 'Revenues'])

    columns_to_check = [
        'IncomeLossFromContinuingOperationsBeforeIncomeTaxesDomestic', 
        'IncomeLossFromContinuingOperationsBeforeIncomeTaxesForeign',
        'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
        'IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethod'
    ]
    existing_columns = [col for col in columns_to_check if col in df.columns]
    if len(existing_columns) > 1:
        if 'IncomeLossFromContinuingOperationsBeforeIncomeTaxesDomestic' in df.columns:
            if 'IncomeLossFromContinuingOperationsBeforeIncomeTaxesForeign' in df.columns:
                df['IncomeLossFromContinuingOperationsBeforeIncomeTaxesDomestic'] = df.apply(lambda row: row['IncomeLossFromContinuingOperationsBeforeIncomeTaxesForeign'] if row['IncomeLossFromContinuingOperationsBeforeIncomeTaxesDomestic'] == 0 else row['IncomeLossFromContinuingOperationsBeforeIncomeTaxesDomestic'], axis=1)
            if 'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest' in df.columns:
                df['IncomeLossFromContinuingOperationsBeforeIncomeTaxesDomestic'] = df.apply(lambda row: row['IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest'] if row['IncomeLossFromContinuingOperationsBeforeIncomeTaxesDomestic'] == 0 else row['IncomeLossFromContinuingOperationsBeforeIncomeTaxesDomestic'], axis=1)
            if 'IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethod' in df.columns:
                df['IncomeLossFromContinuingOperationsBeforeIncomeTaxesDomestic'] = df.apply(lambda row: row['IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethod'] if row['IncomeLossFromContinuingOperationsBeforeIncomeTaxesDomestic'] == 0 else row['IncomeLossFromContinuingOperationsBeforeIncomeTaxesDomestic'], axis=1)
            df = df.drop(columns=[col for col in columns_to_check if col in df.columns and col != 'IncomeLossFromContinuingOperationsBeforeIncomeTaxesDomestic'])
        else:
            if 'IncomeLossFromContinuingOperationsBeforeIncomeTaxesForeign' in df.columns:
                df['IncomeLossFromContinuingOperationsBeforeIncomeTaxesDomestic'] = df['IncomeLossFromContinuingOperationsBeforeIncomeTaxesForeign']
            elif 'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest' in df.columns:
                df['IncomeLossFromContinuingOperationsBeforeIncomeTaxesDomestic'] = df['IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest']
            elif 'IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethod' in df.columns:
                df['IncomeLossFromContinuingOperationsBeforeIncomeTaxesDomestic'] = df['IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethod']
            df = df.drop(columns=[col for col in columns_to_check if col in df.columns and col != 'IncomeLossFromContinuingOperationsBeforeIncomeTaxesDomestic'])

    columns_to_check = ['CostOfRevenue', 'CostOfGoodsSold', 'CostOfGoodsAndServicesSold']
    existing_columns = [col for col in columns_to_check if col in df.columns]
    if len(existing_columns) > 1:
        if 'CostOfRevenue' in df.columns:
            if 'CostOfGoodsSold' in df.columns:
                df['CostOfRevenue'] = df.apply(lambda row: row['CostOfGoodsSold'] if row['CostOfRevenue'] == 0 else row['CostOfRevenue'], axis=1)
            if 'CostOfGoodsAndServicesSold' in df.columns:
                df['CostOfRevenue'] = df.apply(lambda row: row['CostOfGoodsAndServicesSold'] if row['CostOfRevenue'] == 0 else row['CostOfRevenue'], axis=1)
            df = df.drop(columns=[col for col in columns_to_check if col in df.columns and col != 'CostOfRevenue'])
        else:
            if 'CostOfGoodsSold' in df.columns:
                df['CostOfRevenue'] = df['CostOfGoodsSold']
            elif 'CostOfGoodsAndServicesSold' in df.columns:
                df['CostOfRevenue'] = df['CostOfGoodsAndServicesSold']
            df = df.drop(columns=[col for col in columns_to_check if col in df.columns and col != 'CostOfRevenue'])
    
    return df


def process_single_cik(cik, data, bond_yield, start_date, end_date):
    transformed_data = [{'fact': k.rsplit('_', 1)[0], 'period': k.rsplit('_', 1)[1], 'value': v} for k, v in data.items()]
    df = pd.DataFrame(transformed_data).pivot(index='period', columns='fact', values='value').reset_index()
    df['period'] = df['period'].apply(convert_quarters)
    start_date = df['period'].min()
    end_date = df['period'].max()
    complete_dates = pd.date_range(start=start_date, end=end_date, freq='Q')
    df.set_index('period', inplace=True)
    df = df.reindex(complete_dates).sort_index().rename_axis(None, axis=0).rename_axis(None, axis=1)
    df.dropna(axis=1, how='all', inplace=True)
    df = df.loc[:, df.sum(axis=0) != 0].round(2).fillna(0)
    df = transform_df(df)
    column_mapping = dict(zip(keywords_method1.iloc[:, 0], keywords_method1.iloc[:, 1]))
    df.rename(columns=column_mapping, inplace=True)
    df = df.reindex(sorted(df.columns), axis=1)
    df.replace(0, pd.NA, inplace=True)
    df.ffill().bfill(inplace=True)
    df.fillna(0, inplace=True)
    ticker = get_ticker_from_cik(cik)
    if ticker:
        share_price = get_data(ticker, start_date, end_date)
        if share_price is not None and not share_price.empty:
            df = df.assign(share_price=share_price)
            df['share_price'] = df['share_price'].ffill().bfill()
            df = calculate_financial_metrics(df, bond_yield, share_price)
        else:
            df = calculate_financial_metrics(df, bond_yield, None)
    else:
        df = calculate_financial_metrics(df, bond_yield, None)

    return cik, df


def process_cik_data(cik_database, bond_yield, start_date, end_date):
    cik_dfs = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_single_cik, cik, data, bond_yield, start_date, end_date): cik for cik, data in cik_database.items()}
        for future in concurrent.futures.as_completed(futures):
            cik, df = future.result()
            cik_dfs[cik] = df
            print(f"Completed {len(cik_dfs)}/{len(cik_database)}")
    
    with open('Data/DataDerived/frames_df_file.pkl', 'wb') as f:
        pickle.dump(cik_dfs, f)

    return 'Data/DataDerived/frames_df_file.pkl'


def get_ticker_from_cik(cik):
    company_info = fetch_company_info()
    cik = str(int(cik))
    company_info['cik_str'] = company_info['cik_str'].astype(str)
    ticker = company_info.loc[company_info['cik_str'] == cik, 'ticker'].values
    if len(ticker) == 0:
        return None
    
    return ticker[0]


def show_ticker_from_cik(cik, api_df=None, selenium_df=None):
    def display_classifications(ticker, classifications):
        if "NCAV" in classifications:
            print(f"{ticker} is classified as NCAV.")
        elif "Defensive" in classifications:
            print(f"{ticker} is classified as Defensive.")
        elif "Enterprising" in classifications:
            print(f"{ticker} is classified as Enterprising.")
        else:
            print(f"{ticker} cannot be classified due to insufficient data.")

    def classify_and_display(df, ticker):
        company_price = df['share_price'] if 'share_price' in df.columns else None
        classifications = classify_stock_relaxed(df, company_price, ticker, multiple=False)
        display_classifications(ticker, classifications)
        display(df)  # type: ignore

    ticker = get_ticker_from_cik(cik)
    if not ticker:
        print(f"Invalid CIK: {cik}")
        return
    if selenium_df is not None:
        if ticker in selenium_df:
            combo_df = selenium_df[ticker]["combined_df"]
            print(f"Ticker: {ticker}")
            classify_and_display(combo_df, ticker)
        else:
            print(f"No data found for ticker: {ticker}")
        return
    if api_df is not None:
        cik_int = int(cik)
        if cik_int in api_df:
            df = api_df[cik_int]
            print(f"CIK: {cik}")
            classify_and_display(df, ticker)
        else:
            print(f"No data found for CIK: {cik}")
    else:
        print("No data source provided")


def process_multiple_classifications(cik, api_dict, selenium_dict):
    ticker = get_ticker_from_cik(cik)
    if not ticker:
        return None, None, None, None
    if ticker in selenium_dict:
        ticker_data = selenium_dict[ticker]
        combo_df = ticker_data.get("combined_df")
        if combo_df is not None:
            company_price = combo_df.get('share_price')
            defensive_df, enterprising_df, ncav_df = classify_stock_relaxed(combo_df, company_price, ticker=ticker, multiple=True)
            return defensive_df, enterprising_df, ncav_df, None
    elif cik in api_dict:
        df = api_dict[cik]
        company_price = df.get('share_price')
        defensive_df, enterprising_df, ncav_df = classify_stock_relaxed(df, company_price, ticker=ticker, multiple=True)
        return defensive_df, enterprising_df, ncav_df, None

    return None, None, None, None


def classify_multiple_ciks(cik_list, file_name, api_df=None, selenium_df=None):
    api_dict = api_df if isinstance(api_df, dict) else api_df.to_dict(orient='index') if api_df is not None else {}
    selenium_dict = selenium_df.to_dict(orient='index') if isinstance(selenium_df, pd.DataFrame) else selenium_df if isinstance(selenium_df, dict) else {}
    defensive_results = []
    enterprising_results = []
    ncav_results = []
    errors = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_multiple_classifications, cik, api_dict, selenium_dict) for cik in cik_list]
        for future in futures:
            defensive_df, enterprising_df, ncav_df, error = future.result()
            if defensive_df is not None and not defensive_df.empty:
                defensive_results.append(defensive_df)
            elif enterprising_df is not None and not enterprising_df.empty:
                enterprising_results.append(enterprising_df)
            elif ncav_df is not None and not ncav_df.empty:
                ncav_results.append(ncav_df)
            if error:
                errors.append(error)

    defensive_df_total = pd.concat(defensive_results, ignore_index=True) if defensive_results else pd.DataFrame(columns=['Defensive Assets'])
    enterprising_df_total = pd.concat(enterprising_results, ignore_index=True) if enterprising_results else pd.DataFrame(columns=['Enterprising Assets'])
    ncav_df_total = pd.concat(ncav_results, ignore_index=True) if ncav_results else pd.DataFrame(columns=['NCAV Assets'])
    if errors:
        print("Errors:")
        for error in errors:
            print(error)

    excel_file_name = f'Summaries_{file_name}.xlsx'
    with pd.ExcelWriter(f'Data/DataSummaries/{excel_file_name}') as writer:
        defensive_df_total.to_excel(writer, sheet_name='Defensive', index=False)
        enterprising_df_total.to_excel(writer, sheet_name='Enterprising', index=False)
        ncav_df_total.to_excel(writer, sheet_name='NCAV', index=False)
    
    return defensive_df_total, enterprising_df_total, ncav_df_total


def check_defensive_criteria(df, company_price):
    current_price = company_price.iloc[-1]
    last_pe_ratio = df['pe ratio'].iloc[-1]
    last_pb_ratio = df['pb ratio'].iloc[-1]
    last_mult_pb = df['multiplier pb'].iloc[-1]
    graham_number = np.sqrt(22.5 * df['diluted earnings per share'].mean() * df['bvps'].iloc[-1])
    criteria = {
        "Criterion 1: Annual Sales >= $'2B'": df['total revenues'] >= 2000,
        "Criterion 2: Current Assets >= 2 * Current Liabilities": df['total current assets'] >= 2 * df['total current liabilities'],
        "Criterion 3: Long-term Debt <= Net Current Assets": df['long term debt'] <= df['net current assets'],
        "Criterion 4: Positive Earnings for Past 10 Years": all(df['diluted earnings per share'].tail(9) > 0),
        "Criterion 6: Price <= 15 * Average EPS": current_price <= 15 * df['diluted earnings per share'].mean(),
        "Criterion 7: Price <= 1.5 * Book Value": current_price <= 1.5 * df['bvps'].iloc[-1],
        "Criterion 8: Healthy Current Ratio (1.5 - 3)": (df['current ratio'] >= 1.5) & (df['current ratio'] <= 3),
        "Criterion 9: PA Ratio <= 1.5 or (P/E < 15 and P/A * P/B < 22.5)": last_pb_ratio <= 1.5 or (last_pe_ratio < 15 and last_pb_ratio * last_pe_ratio < 22.5),
        "Criterion 10: Price <= 2/3 * Intrinsic Value": company_price <= 2/3 * df['intrinsic value'],
        "Criterion 11: Undervalued": current_price < df['intrinsic value'],
        "Rule of Thumb: PE Ratio * PB Ratio <= 22.5": last_mult_pb <= 22.5,
        "Graham Number": round(graham_number, 2),
        "Current Ratio": df['current ratio'],
        "EPS": df['diluted earnings per share'].tail(9),
        "PE Ratio": round(df['pe ratio'], 2),
        "Margin of Safety": df['margin safety']
    }
    if len(df) >= 9:
        criterion5 = df['diluted earnings per share'].iloc[-1] >= 1.33 * df['diluted earnings per share'].iloc[-9]
        earnings_growth = df['diluted earnings per share'].iloc[-1] / df['diluted earnings per share'].iloc[-9]
        criteria["Criterion 5: EPS Growth >= 33% in 10 Years"] = criterion5
        criteria["Earnings Growth"] = round(earnings_growth, 4)

    return criteria


def check_enterprising_criteria(df, company_price):
    serenity_number = np.sqrt(12 * df['diluted earnings per share'].mean() * df['tbvps'].iloc[-1])
    criteria = {
        "Criterion 1: Current Assets >= 1.5 * Current Liabilities": df['total current assets'] >= 1.5 * df['total current liabilities'],
        "Criterion 2: Total Debt <= 1.1 * Net Current Assets": df['total debt'] <= 1.1 * df['net current assets'],
        "Criterion 3: No Deficit in Last 5 Years": all(df['net income'] > 0),
        "Criterion 4: Price < 120% Net Tangible Assets": company_price < 1.2 * df['tbvps'],
        "Serenity Number": round(serenity_number, 2),
        "TBVPS": round(df['tbvps'], 2)
    }

    return criteria


def check_ncav_criteria(df, company_price):
    criteria = {
        "Criterion 1: NCAV <= Price": df['ncav'] <= company_price,
        "NCAV": round(df['ncav'], 2)
    }

    return criteria


def classify_stock_relaxed(df, company_price, ticker=None, multiple=False):
    defensive_assets = []
    enterprising_assets = []
    ncav_assets = []
    if company_price is None or company_price.empty:
        current_price = None
    else:
        current_price = company_price.iloc[-1]
    if len(df) >= 2 and 'diluted earnings per share' in df.columns:
        eps_prev = df['diluted earnings per share'].iloc[-2]
        eps_last = df['diluted earnings per share'].iloc[-1]
        eps_condition = eps_last >= 1.33 * eps_prev
    else:
        eps_condition = False
    total_revenues = df['total revenues'].iloc[-1] if 'total revenues' in df.columns else None
    total_current_assets = df['total current assets'].iloc[-1] if 'total current assets' in df.columns else None
    total_current_liabilities = df['total current liabilities'].iloc[-1] if 'total current liabilities' in df.columns else None
    net_current_assets = df['net current assets'].iloc[-1] if 'net current assets' in df.columns else None
    long_term_debt = df['long term debt'].iloc[-1] if 'long term debt' in df.columns else None
    bvps = df['bvps'].iloc[-1] if 'bvps' in df.columns else None
    current_ratio = df['current ratio'].iloc[-1] if 'current ratio' in df.columns else None
    intrinsic_value = df['intrinsic value'].iloc[-1] if 'intrinsic value' in df.columns else None
    multiplier_pb = df['multiplier pb'].iloc[-1] if 'multiplier pb' in df.columns else None
    total_debt = df['total debt'].iloc[-1] if 'total debt' in df.columns else None
    net_income = df['net income'].iloc[-1] if 'net income' in df.columns else None
    tbvps = df['tbvps'].iloc[-1] if 'tbvps' in df.columns else None
    ncav = df['ncav'].iloc[-1] if 'ncav' in df.columns else None
    defensive_conditions = [
        total_revenues is not None and total_revenues >= 2000,
        total_current_assets is not None and total_current_liabilities is not None and total_current_assets >= 2 * total_current_liabilities,
        net_current_assets is not None and (long_term_debt is None or long_term_debt <= net_current_assets),
        eps_condition,
        current_price is not None and bvps is not None and current_price <= 1.5 * bvps,
        current_price is not None and intrinsic_value is not None and current_price <= 2/3 * intrinsic_value,
        current_ratio is not None and 1.2 <= current_ratio <= 4,
        multiplier_pb is not None and multiplier_pb <= 22.5
    ]
    enterprising_conditions = [
        total_current_assets is not None and total_current_liabilities is not None and total_current_assets >= 1.5 * total_current_liabilities,
        net_current_assets is not None and (total_debt is None or total_debt <= 1.1 * net_current_assets),
        net_income is not None and net_income >= 0,
        current_price is not None and tbvps is not None and current_price < 1.2 * tbvps
    ]
    ncav_condition = ncav is not None and current_price is not None and ncav >= current_price
    defensive_classified = sum(defensive_conditions) >= 5
    enterprising_classified = sum(enterprising_conditions) >= 2
    ncav_classified = ncav_condition
    if multiple:
        if defensive_classified:
            defensive_assets.append(ticker)
        elif enterprising_classified:
            enterprising_assets.append(ticker)
        elif ncav_classified:
            ncav_assets.append(ticker)

        defensive_df = pd.DataFrame(defensive_assets, columns=['Defensive Assets'])
        enterprising_df = pd.DataFrame(enterprising_assets, columns=['Enterprising Assets'])
        ncav_df = pd.DataFrame(ncav_assets, columns=['NCAV Assets'])

        return defensive_df, enterprising_df, ncav_df
    else:
        classifications = []
        if defensive_classified:
            classifications.append("Defensive")
        if enterprising_classified:
            classifications.append("Enterprising")
        if ncav_classified:
            classifications.append("NCAV")

        return classifications
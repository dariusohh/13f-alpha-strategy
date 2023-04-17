import asyncio
import re
import time
from io import StringIO

import aiohttp
import pandas as pd
import requests
import xmltodict
from tqdm import tqdm


async def asynchronous_scrape_13f(inputs):
    async def get(input, session):
        headers = {'User-Agent': 'Mozilla/5.0'}
        async with session.get(url="https://www.sec.gov/Archives/" + input["filename"],
                               headers=headers) as response:
            content = await response.text()
            try:
                return scrape_13f(content, input["cik"], input["company_name"], input["date_filed"])
            except:
                try:
                    return scrape_13f_old(content, input["cik"], input["company_name"], input["date_filed"])
                except Exception:
                    print(f"Error with scraping {input}")
                    return None

    results = []
    # SEC limits 10 requests/second
    for i in tqdm(range(0, len(inputs), 10)):
        start = time.time()
        connector = aiohttp.TCPConnector(force_close=True)
        async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:
            tasks = [asyncio.create_task(get(input, session)) for input in inputs[i:i + 10]]
            r = await asyncio.gather(*tasks)
            results += [res for res in r if res is not None]
        stop = time.time()
        remain = start + 1.01 - stop
        if remain > 0:
            time.sleep(remain)
    return results


def scrape_13f(html_content, cik, name, date_filed):
    xml_text = re.findall(r'<XML>(.*?)<\/XML>', html_content, re.DOTALL | re.MULTILINE)[1][1:-1]
    xml_dict = xmltodict.parse(xml_text)
    data = list(xml_dict.values())[0]
    keys = data.keys()
    infotable_key = [k for k in keys if "infoTable" in k][0]
    key_prefix = infotable_key.replace('infoTable', '')
    flattened_dict = pd.json_normalize(data[infotable_key], sep="_")
    df = pd.DataFrame(flattened_dict)
    df.columns = df.columns.str.replace(key_prefix, '')
    df['cik'] = cik
    df['company_name'] = name
    df['date_filed'] = date_filed
    return df.set_index(['cik', 'company_name', 'date_filed']).reset_index()


def clean_13f_old(df):
    df.shrsOrPrnAmt_sshPrnamt = df.shrsOrPrnAmt_sshPrnamt.apply(
        lambda x: pd.to_numeric(str(x).replace(',', ''), errors='coerce'))

    df.value = df.value.apply(
        lambda x: pd.to_numeric(str(x).replace(',', ''), errors='coerce'))
    df = df.dropna(subset=["nameOfIssuer", "cusip", "value", "shrsOrPrnAmt_sshPrnamt"])
    df = df[df.cusip.apply(lambda x: len(x) == 9)]
    return df


def scrape_13f_old(html_content, cik, name, date_filed):
    tables = re.findall(r'<table>(.*?)<\/table>', html_content, re.DOTALL | re.MULTILINE | re.IGNORECASE)
    if len(tables) == 0:
        raise Exception("Error, no data")

    data = []
    for table in tables:
        lines = table.split('\n')
        parse = False
        for l in lines:
            if parse and len(l) > 0:
                data.append(get_line_data(l))
            if '<C>' in l:
                parse = True
    df = pd.DataFrame.from_records(data, columns=["nameOfIssuer", "titleOfClass", "cusip", "value",
                                                  "shrsOrPrnAmt_sshPrnamt", 'shrsOrPrnAmt_sshPrnamtType', 'putCall'])
    df['cik'] = cik
    df['company_name'] = name
    df['date_filed'] = date_filed
    return clean_13f_old(df.set_index(['cik', 'company_name', 'date_filed']).reset_index())


def get_line_data(line):
    line = line.replace('\t', '  ').replace("$", "")
    if ' put ' in line or ' PUT ' in line:
        pc = "PUT"
    elif ' call ' in line or ' CALL ' in line:
        pc = "CALL"
    else:
        pc = ""
    line = line.lstrip()
    issuer = line.split('  ')[0]
    line = line.replace(issuer, '').lstrip()
    class_title = line.split('  ')[0]
    line = line.replace(class_title, '').lstrip()
    if len(class_title) == 9 and ' ' not in class_title and any(i.isdigit() for i in class_title):
        cusip = class_title
        class_title = ""
    else:
        cusip = line.split(' ')[0]
        line = line.replace(cusip, '').lstrip()
    value = line.split(' ')[0]
    line = line.replace(value, '').lstrip()
    shr_amt = line.split(' ')[0]
    line = line.replace(shr_amt, '')
    if "SH" in shr_amt or "sh" in shr_amt:
        sh_prn = "SH"
        shr_amt = shr_amt.replace("SH", "").replace("sh", "")
    elif line.startswith('  '):
        sh_prn = ""
    else:
        line = line.lstrip()
        sh_prn = line.split(' ')[0]
    return issuer, class_title, cusip, value, shr_amt, sh_prn, pc


def download_quarter_13f(year: int,
                         quarter: int):
    # Define the url at which to get the index file.
    url = f'https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx'
    headers = {'User-Agent': 'Mozilla/5.0'}
    r = requests.get(url, stream=True, headers=headers)

    # Get all 13F filings filed in the quarter
    columns = ["cik", "company_name", "form_type", "date_filed", "filename"]
    masterlist = pd.read_csv(StringIO(r.text), sep="|", skiprows=11, header=None, names=columns)
    list_13f = masterlist[masterlist["form_type"] == "13F-HR"]
    # Keep only latest 13F filing for the quarter for each company
    list_13f = list_13f.sort_values('date_filed').drop_duplicates('company_name', keep='last')

    # Loop through each filing to get holding data for each filing
    if year < 2013 or (year == 2013 and quarter <= 2):
        data = pd.concat(asyncio.run(asynchronous_scrape_13f(list_13f.to_dict('records'))), ignore_index=True)
    else:
        data = pd.concat(asyncio.run(asynchronous_scrape_13f(list_13f.to_dict('records'))), ignore_index=True)
    data.to_csv(f'./13f_data/{year}Q{quarter}.csv', index=False)
    print(f"13F Data for {year}Q{quarter} successfully downloaded")


def download_masterlist():
    data = pd.DataFrame()
    for year in tqdm(range(2000, 2023)):
        for quarter in range(1, 5):
            if year == 2022 and quarter > 2:
                continue
            url = f'https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx'
            headers = {'User-Agent': 'Mozilla/5.0'}
            r = requests.get(url, stream=True, headers=headers)
            # Get all 13F filings filed in the quarter
            columns = ["cik", "company_name", "form_type", "date_filed", "filename"]
            masterlist = pd.read_csv(StringIO(r.text), sep="|", skiprows=11, header=None, names=columns)
            list_13f = masterlist[masterlist["form_type"] == "13F-HR"]
            # Keep only latest 13F filing for the quarter for each company
            list_13f = list_13f.sort_values('date_filed').drop_duplicates('company_name', keep='last')
            time.sleep(0.1)
            list_13f["year"] = year
            list_13f["quarter"] = quarter
            list_13f = list_13f.set_index(["year", "quarter"]).reset_index()
            data = pd.concat([data, list_13f])
    data.to_csv("./13f_list.csv", index=False)

async def add_holdings_date_masterlist():
    masterlist = pd.read_csv("13f_list.csv")
    async def get(input, session):
        headers = {'User-Agent': 'Mozilla/5.0'}
        async with session.get(url="https://www.sec.gov/Archives/" + input,
                               headers=headers) as response:
            content = await response.text()
            return next((x.split("\t")[1] for x in content.split('\n') if x.startswith("CONFORMED PERIOD OF REPORT")), None)

    results = []
    # SEC limits 10 requests/second
    for i in tqdm(range(0, len(inputs), 10)):
        start = time.time()
        connector = aiohttp.TCPConnector(force_close=True)
        async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:
            tasks = [asyncio.create_task(get(input, session)) for input in inputs[i:i + 10]]
            r = await asyncio.gather(*tasks)
            results += r
        stop = time.time()
        remain = start + 1.01 - stop
        if remain > 0:
            time.sleep(remain)
    masterlist["date_holding"] = result
    masterlist.to_csv("13f_list.csv", index=False)

# download_masterlist()
from sec_api import QueryApi

api_key = "a8ddcb32ea7c93b0635681542d0324da0ed33727696e53ff43c051573546f343"
queryApi = QueryApi(api_key=api_key)


async def asynchronous_scrape_10k(inputs, type):
    async def get(input, session):
        url = input["linkToHtml"]
        if type == "MDA":
            item = "7" if input["formType"] == "10-K" else "part1item2"
        elif type == "market_risk":
            item = "7A" if input["formType"] == "10-K" else "part1item3"
        else:
            item = "1A" if input["formType"] == "10-K" else "part2item1a"
        async with session.get(
                f"https://api.sec-api.io/extractor?url={url}&item={item}&type=text&token={api_key}") as response:
            if response.status == 429:
                print("Error found")
            content = await response.text()
            return content

    results = []
    for i in range(0, len(inputs), 2):
        start = time.time()
        connector = aiohttp.TCPConnector(force_close=True)
        async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:
            tasks = [asyncio.create_task(get(input, session)) for input in inputs.to_dict("records")[i:i + 2]]
            r = await asyncio.gather(*tasks)
            results += r
            stop = time.time()
            remain = start + 1.01 - stop
            if remain > 0:
                time.sleep(remain)
    return results


def download_10f_data(ticker):
    def get_response(start):
        query = {
            "query": {
                "query_string": {
                    "query": f'((formType:"10-K" AND NOT formType:"10-K/A" AND NOT formType:"NT 10-K") OR (formType:"10-Q" AND NOT formType:"10-Q/A" AND NOT formType:"NT 10-Q")) AND ticker:{ticker}'
                }
            },
            "from": start,
            "size": "50",
            "sort": [{"filedAt": {"order": "desc"}}],
        }
        return queryApi.get_filings(query)

    filings = pd.DataFrame(get_response(0)["filings"] + get_response(50)["filings"])
    if len(filings) == 0:
        filings.to_csv(f'./10k_data/{ticker}.csv', index=False)
        return
    filings.filedAt = pd.to_datetime([x.split("T")[0] for x in filings.filedAt])
    filings = filings[filings.filedAt >= "2000-01-01 00:00:00"]
    filings["MDA"] = asyncio.run(asynchronous_scrape_10k(filings, "MDA"))
    filings["market_risk"] = asyncio.run(asynchronous_scrape_10k(filings, "market_risk"))
    filings["risk_factor"] = asyncio.run(asynchronous_scrape_10k(filings, "risk_factor"))
    filings.to_csv(f'./10k_data/{ticker}.csv', index=False)
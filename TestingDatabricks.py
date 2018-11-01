# Databricks notebook source
import requests
import pandas as pd

# Create a dictionary containing the form elements and their values, the button and hidden form input also need to be added:
inputs = {'format': '2', 'layout' : '2', 'decimal_separator' : '1', 'date_format' : '1', 'op':'Go', 'form_id': 'nyx_download_form'}
url_equities = 'https://www.euronext.com/en/popup/data/download?ml=nyx_pd_stocks&cmd=default&formKey=nyx_pd_filter_values%3A1006ef55d4998cc0fad71db6a6f38530'


# POST to the remote endpoint. The Requests library will encode the
# data automatically
r = requests.post(url_equities, data=inputs)

# Get the raw body text back
with open('reponse_file_req', "w") as f:
    x = r.text.splitlines()
    del x[1:4] # delete lines 2, 3 and 4
    f.write("\n".join(x))

df = pd.read_csv('reponse_file_req', error_bad_lines=False, sep=';')


# https://realpython.com/headless-selenium-testing-with-python-and-phantomjs/
for isin in df['ISIN']:
    # https://www.euronext.com/en/products/equities/FR0010285965-ALXP/quotes
    # https://www.euronext.com/nyx_eu_listings/price_chart/download_historical?typefile=csv&layout=vertical&typedate=dmy&separator=point&mic=ALXP&isin=FR0010285965&name=1000MERCIS&namefile=Price_Data_Historical_1000MERCIS&from=656812800000&to=1541030400000&adjusted=1&base=0
    Euronext Growth Paris = ALXP
    Euronext Paris = XPAR
    Euronext Access Paris = XMLI
    Euronext Access Brussels = XBRU
    Euronext Access Lisbon = ALXL
    Euronext Amsterdam = XAMS
    Euronext Growth Brussels = ALXB
    Euronext Growth Lisbon = EXNL
    Euronext Lisbon = XLIS
    Traded not listed Brussels = TNLB
    print(isin)

# COMMAND ----------

df['Market2'] = df['Market'].str.split(',').str[0]
df.groupby('Market2').head(1).sort_values('Market2')['Market2']
# df.head(40)
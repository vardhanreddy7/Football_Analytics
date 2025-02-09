import requests
from bs4 import BeautifulSoup
import polars as pl

from fa_modules.web_scraping.utils import *

if __name__ == '__main__':
    url = 'https://fbref.com/en/comps/9/Premier-League-Stats'
    table_id = 'results2024-202591_overall'
    table_type = TableType.LEAGUE_TABLE
    data_cols = extract_table_cols(url,table_id)
    table_data = extract_table_data(url,table_id,table_type)
    league_data = get_data(url,table_id,table_type)
    league_data.glimpse()
    
    # print(table_data)

from datetime import datetime
import requests
import sys

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders
import pandas as pd


def test_ckan():
    lim = data_loaders.data_loader._default_limit
    data_loaders.data_loader._default_limit = 500
    url = "https://data.virginia.gov"
    dataset = "60506bbb-685f-4360-8a8c-30e137ce3615"
    date_field = "STOP_DATE"
    agency_field = 'AGENCY NAME'
    loader = data_loaders.Ckan(url, dataset, date_field)

    assert not loader.isfile()

    count = loader.get_count()

    r = requests.get(f'https://data.virginia.gov/api/3/action/datastore_search_sql?sql=SELECT COUNT(*) FROM "{dataset}"')
    r.raise_for_status()
    assert count==r.json()['result']['records'][0]['count']>0

    r = requests.get(f'https://data.virginia.gov/api/3/action/datastore_search_sql?sql=SELECT * FROM "{dataset}" '+
                     'ORDER BY "_id" OFFSET 0 LIMIT 100')
    date_col_info = [x for x in r.json()['result']["fields"] if x["id"]==date_field]
    if len(date_col_info)==0:
        raise ValueError(f"Date column {date_field} not found")
    filter_year = date_col_info[0]["type"] not in ['timestamp','date']

    year = 2022
    count = loader.get_count(year=year)

    if filter_year:
        def gen_where(year):
            return '"' + date_field + '"' + rf" LIKE '%{year}%'"

        r = requests.get(f'https://data.virginia.gov/api/3/action/datastore_search_sql?sql=SELECT COUNT(*) FROM "{dataset}" WHERE ' + 
                            gen_where(year))
    else:
        r = requests.get(f'https://data.virginia.gov/api/3/action/datastore_search_sql?sql=SELECT COUNT(*) FROM "{dataset}"' + 
            f""" WHERE "{date_field}" >= '{year}-01-01' AND "{date_field}" < '{year+1}-01-01'""")

    r.raise_for_status()
    assert count==r.json()['result']['records'][0]['count']>0

    agency='William and Mary Police Department'
    opt_filter = {'=':{agency_field:agency}}
    opt_filter = 'LOWER("' + agency_field + '")' + " = '" + agency.lower() + "'"
    count = loader.get_count(year=year, opt_filter=opt_filter)

    if filter_year:
        r = requests.get(f'https://data.virginia.gov/api/3/action/datastore_search_sql?sql=SELECT COUNT(*) FROM "{dataset}" WHERE ' + 
                        gen_where(year) + " AND "+ opt_filter)
    else:
        r = requests.get(f'https://data.virginia.gov/api/3/action/datastore_search_sql?sql=SELECT COUNT(*) FROM "{dataset}"' + 
                    f""" WHERE "{date_field}" >= '{year}-01-01' AND "{date_field}" < '{year+1}-01-01' AND """+
                    opt_filter)
    r.raise_for_status()
    assert count==r.json()['result']['records'][0]['count']>0

    loader._last_count = None
    df = loader.load(year=year, pbar=False, opt_filter=opt_filter)

    assert len(df)==count
    assert (df[agency_field]==agency).all()

    offset = 1
    nrows = count - 2
    df_offset = loader.load(year=year, nrows=nrows, offset=1, pbar=False, opt_filter=opt_filter)

    assert df_offset.equals(df.iloc[offset:offset+nrows].reset_index(drop=True))

    df_offset = loader.load(year=year, offset=1, pbar=False, opt_filter=opt_filter)
    assert df_offset.equals(df.iloc[offset:].reset_index(drop=True))

    if filter_year:
        r = requests.get(f'https://data.virginia.gov/api/3/action/datastore_search_sql?sql=SELECT * FROM "{dataset}" WHERE ' + 
                        gen_where(year) + " AND "+ opt_filter + " ORDER BY _id")
    else:
        r = requests.get(f'https://data.virginia.gov/api/3/action/datastore_search_sql?sql=SELECT * FROM "{dataset}"' + 
                            f""" WHERE "{date_field}" >= '{year}-01-01' AND "{date_field}" < '{year+1}-01-01' AND """+
                            opt_filter + " ORDER BY _id")
    df_comp= pd.DataFrame(r.json()['result']['records'])
    if not filter_year:
        df_comp[date_field] = pd.to_datetime(df_comp[date_field])
    df_comp = df_comp.drop(columns=['_id','_full_text'])
    
    assert df.equals(df_comp)

    cur_year = datetime.now().year
    year_range = [cur_year-1, cur_year]
    df = loader.load(year=year_range, pbar=False, opt_filter=opt_filter)
    assert (df[agency_field]==agency).all()

    data_loaders.data_loader._default_limit = lim


import json
import pandas as pd
import pytest
import re
import sys
import warnings

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders

@pytest.mark.parametrize('url',['https://stacks.stanford.edu/file/druid:yg821jf8611/yg821jf8611_ar_little_rock_2020_04_01.csv.zip',
                                'https://www.chicagopolice.org/wp-content/uploads/legacy/2016-ISR.zip'])
def test_load_single_file_csv_zip(all_datasets, url):
    ds = all_datasets[all_datasets['URL']==url]
    assert len(ds)==1
    if pd.notnull(ds['py_min_version'].iloc[0]):
        if sys.version_info<=tuple(int(x) for x in ds['py_min_version'].iloc[0].split('.')):
            # Dataset does not work in this Python version
            return

    loader = data_loaders.Csv(url)
    df = loader.load(pbar=False)

    headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                # 'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
            }
    

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".+mixed types.+")
        try:
            df_true = pd.read_csv(url)
        except:
            df_true = pd.read_csv(url,encoding_errors='surrogateescape', storage_options=headers)
    assert df.equals(df_true)


# Another CSV with newline characters: https://raw.githubusercontent.com/openpolicedata/opd-datasets/main/data/Texas_Austin_OFFICER-INVOLVED_SHOOTINGS-INCIDENTS.csv
@pytest.mark.parametrize('url, date_field, query',[
    ('https://public.tableau.com/views/PPBOpenDataDownloads/OIS-All.csv?:showVizHome=no', 'Day of Date Time', None),
    ("https://opendata.jaxsheriff.org/OIS/Export", "IncidentDate", None),
    ('https://raw.githubusercontent.com/openpolicedata/opd-datasets/refs/heads/main/data/Wisconsin_Milwaukee_COMPLAINTS.csv', 'DateReported', '{“Department”:”Milwaukee Police Department”}'),
    ('https://raw.githubusercontent.com/openpolicedata/opd-datasets/main/data/Texas_Austin_OFFICER-INVOLVED_SHOOTINGS-INCIDENTS.csv', 'date', None),
])
def test_csv(url, date_field, query):
    if pd.notnull(query):
        # Remove any curly quotes
        query = query.replace('“','"').replace('”','"')
        query = json.loads(query)

    loader = data_loaders.Csv(url, date_field=date_field, query=query)
    assert loader.isfile()
    df = loader.load(pbar=False)

    offset = 1
    nrows = len(df)-offset
    df_offset = loader.load(offset=offset,nrows=nrows, pbar=False)
    assert df_offset.equals(df.iloc[offset:nrows+offset].reset_index(drop=True))
    
    df_offset = loader.load(offset=offset, pbar=False)
    assert df_offset.equals(df.iloc[offset:].reset_index(drop=True))

    df_comp = pd.read_csv(url)
    if bool(query):
        for k,v in query.items():
            df_comp = df_comp[df_comp[k]==v].reset_index(drop=True)

    def convert_date_field(df):
        df[date_field] = df[date_field].apply(lambda x: re.sub(r'(\d\d)20(\d\d)', r'\1/20\2', x) if isinstance(x,str) else x)
        return df.astype({date_field: 'datetime64[ns]'})
    
    df_comp = convert_date_field(df_comp)
    df = convert_date_field(df)

    count = loader.get_count(force=True)
    assert len(df_comp) == count
    # Test using cached value
    assert count == loader.get_count()

    assert df_comp.equals(df)

    with pytest.raises(ValueError):
        loader.get_years()

    years = loader.get_years(force=True)

    df = convert_date_field(df)
    assert list(df[date_field].dt.year.sort_values(ascending=True).dropna().unique()) == years

    if not query:
        nrows = 7
        df = data_loaders.Csv(url).load(nrows=nrows)
        df_comp = pd.read_csv(url, nrows=nrows)

        assert df_comp.equals(df)


@pytest.mark.parametrize('url, date_field',[
    ('https://public.tableau.com/views/PPBOpenDataDownloads/OIS-All.csv?:showVizHome=no', 'Day of Date Time'),
    ("https://opendata.jaxsheriff.org/OIS/Export", "IncidentDate")
])
def test_csv_year_filter(url, date_field):
    loader = data_loaders.Csv(url, date_field=date_field)
    year = 2020
    df = loader.load(year=year, pbar=False)
    with pytest.raises(ValueError):
        count = loader.get_count(year=year)

    count = loader.get_count(year=year, force=True)
    assert len(df) == count

    count2 = loader.get_count(year=year+1, force=True)

    # Ensure that count updates properly with different call (most recent count is cached)
    assert count!=count2

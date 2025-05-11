import pytest
import sys

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders
import pandas as pd

def test_html():
    url = "https://www.openpolicedata.com/StJohnIN/Citations/2023Citations.php"
    date_field = "Date"
    loader = data_loaders.Html(url, date_field=date_field)
    assert loader.isfile()
    df = loader.load(pbar=False)

    # Ensure entire datasets is read in
    assert df[date_field].min() == pd.Timestamp(f"{df[date_field].dt.year.mode().iloc[0]}-01-01")
    assert df[date_field].max() == pd.Timestamp(f"{df[date_field].dt.year.mode().iloc[0]}-12-30")
    assert len(df)==2510

    offset = 1
    nrows = len(df)-offset-1
    df_offset = loader.load(offset=offset,nrows=nrows, pbar=False)
    assert df_offset.equals(df.iloc[offset:nrows+offset].reset_index(drop=True))
    
    df_offset = loader.load(offset=offset, pbar=False)
    assert df_offset.equals(df.iloc[offset:].reset_index(drop=True))

    header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0'}
    df_comp = pd.read_html(url, storage_options=header)[0]
    df_comp = df_comp.astype({date_field: 'datetime64[ns]'})
    df = df.astype({date_field: 'datetime64[ns]'})

    with pytest.raises(ValueError):
        count = loader.get_count()

    count = loader.get_count(force=True)
    assert len(df_comp) == count
    # Test using cached value
    assert count == loader.get_count()

    assert df_comp.equals(df)

    with pytest.raises(ValueError):
        loader.get_years()

    years = loader.get_years(force=True)

    df = df.astype({date_field: 'datetime64[ns]'})
    assert list(df[date_field].dt.year.sort_values(ascending=True).dropna().unique()) == years

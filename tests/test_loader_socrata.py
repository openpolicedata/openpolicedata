import sys

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders, defs
import pandas as pd
try:
    import geopandas as gpd
    _has_gpd = True
except:
    _has_gpd = False

from test_utils import check_for_dataset


def test_socrata_geopandas():
    if _has_gpd:
        url = "data.montgomerycountymd.gov"
        data_set = "4mse-ku6q"
        date_field = "date_of_stop"
        year = 2020
        nrows = 1000
        df = data_loaders.Socrata(url=url, data_set=data_set, date_field=date_field).load(date=year, nrows=nrows)

        assert type(df) == gpd.GeoDataFrame
    else:
        pass

def test_socrata_pandas():
    url = "data.montgomerycountymd.gov"
    data_set = "usip-62e2"
    date_field = "created_dt"
    year = 2020
    loader = data_loaders.Socrata(url=url, data_set=data_set, date_field=date_field)
    
    data_loaders._use_gpd_force = False
    try:
        df = loader.load(date=year, pbar=False, nrows=1000)
    except:
        raise
    finally:
        data_loaders._use_gpd_force = None

    assert type(df) == pd.DataFrame

def test_socrata_agency_filter():
    url = "data.ct.gov/"
    dataset = "nahi-zqrt"
    date_field = "interventiondatetime"
    agency_field = 'department_name'
    loader = data_loaders.Socrata(url, dataset, date_field)

    agency='Winsted'
    opt_filter = 'LOWER(' + agency_field + ") = '" + agency.lower() + "'"
    df = loader.load(date=2018, opt_filter=opt_filter)

    assert (df[agency_field]==agency).all()

    df = loader.load(date=[2018,2019], opt_filter=opt_filter)

    assert (df[agency_field]==agency).all()

def test_socrata():
    if not check_for_dataset('Richmond', defs.TableType.SHOOTINGS):
        return
    
    url = "www.transparentrichmond.org"
    data_set = "asfd-zcvn"
    
    lim = data_loaders.data_loader._default_limit
    data_loaders.data_loader._default_limit = 500
    try:
        loader = data_loaders.Socrata(url, data_set)
        df =loader.load(pbar=False)
        assert not loader.isfile()
        count = loader.get_count()

        assert len(df)==count

        offset = 1
        nrows = len(df)-offset-1
        df_offset = loader.load(offset=offset,nrows=nrows, pbar=False)
        assert set(df.columns)==set(df_offset.columns)
        df_offset = df_offset[df.columns]
        assert df_offset.equals(df.iloc[offset:nrows+offset].reset_index(drop=True))
    except:
        raise
    finally:
        data_loaders.data_loader._default_limit = lim

    client = data_loaders.socrata.SocrataClient(url, data_loaders.socrata.default_sodapy_key, timeout=60)
    results = client.get(data_set, order=":id", limit=100000)
    rows = pd.DataFrame.from_records(results)

    assert rows.equals(df)

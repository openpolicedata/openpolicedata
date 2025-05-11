import requests
import sys

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders
import pandas as pd
try:
    import geopandas as gpd
    _has_gpd = True
except:
    _has_gpd = False


def test_carto():
    lim = data_loaders.data_loader._default_limit
    data_loaders.data_loader._default_limit = 500
    url = "phl"
    dataset = "shootings"
    date_field = "date_"
    loader = data_loaders.Carto(url, dataset, date_field)

    assert not loader.isfile()

    count = loader.get_count()

    r = requests.get(f"https://phl.carto.com/api/v2/sql?q=SELECT count(*) FROM {dataset}")
    r.raise_for_status()
    assert count==r.json()["rows"][0]["count"]>0

    year = 2019
    count = loader.get_count(year=year)

    r = requests.get(f"https://phl.carto.com/api/v2/sql?q=SELECT count(*) FROM {dataset} WHERE {date_field} >= '{year}-01-01' AND {date_field} < '{year+1}-01-01'")
    r.raise_for_status()
    assert count==r.json()["rows"][0]["count"]>0

    df = loader.load(year=year, pbar=False)

    assert len(df)==count

    offset = 1
    nrows = count - 2
    df_offset = loader.load(year=year, nrows=nrows, offset=1, pbar=False)

    assert df_offset.equals(df.iloc[offset:offset+nrows].reset_index(drop=True))

    df_offset = loader.load(year=year, offset=1, pbar=False)
    assert df_offset.equals(df.iloc[offset:].reset_index(drop=True))

    r = requests.get(f"https://phl.carto.com/api/v2/sql?format=GeoJSON&q=SELECT * FROM {dataset} WHERE {date_field} >= '{year}-01-01' AND {date_field} < '{year+1}-01-01'")
    features = r.json()["features"]
    df_comp= pd.DataFrame.from_records([x["properties"] for x in features])
    df_comp[date_field] = pd.to_datetime(df_comp[date_field])
    
    try:
        import geopandas as gpd
        from shapely.geometry import Point
        geometry = []
        for feat in features:
            if "geometry" not in feat or feat["geometry"]==None or len(feat["geometry"]["coordinates"])<2:
                geometry.append(None)
            else:
                geometry.append(Point(feat["geometry"]["coordinates"][0], feat["geometry"]["coordinates"][1]))

        df_comp = gpd.GeoDataFrame(df_comp, crs=4326, geometry=geometry)
    except:
        geometry = [feat["geometry"] if "geometry" in feat else None for feat in features]
        df_comp["geolocation"] = geometry

    assert df.equals(df_comp)

    data_loaders.data_loader._default_limit = lim

    if _has_gpd:
        assert type(df) == gpd.GeoDataFrame
        data_loaders.carto._has_gpd = False
        df = loader.load(year=year, nrows=nrows, pbar=False)
        data_loaders.carto._has_gpd = True
        assert isinstance(df, pd.DataFrame)

    url2 = "https://phl.carto.com/api/v2/sql?"
    loader2 = data_loaders.Carto(url2, dataset, date_field)
    assert loader.url==loader2.url

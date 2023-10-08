from io import BytesIO
import pytest
import requests
if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders
import pandas as pd
try:
    import geopandas as gpd
    _has_gpd = True
except:
    _has_gpd = False

import warnings
warnings.filterwarnings(action='ignore', module='arcgis')

class TestProduct:
    def test_process_date_input_empty(self, csvfile, source, last, skip, loghtml):
        with pytest.raises(ValueError):
            data_loaders._process_date([])
        
    def test_process_date_too_many(self, csvfile, source, last, skip, loghtml):
        year = [2021, 2022, 2023]
        with pytest.raises(ValueError):
            data_loaders._process_date(year)

    def test_process_dates_year_input_wrong_order(self, csvfile, source, last, skip, loghtml):
        year = [2023, 2021]
        with pytest.raises(ValueError):
            data_loaders._process_date(year)

    def test_carto(self, csvfile, source, last, skip, loghtml):
        lim = data_loaders._default_limit
        data_loaders._default_limit = 500
        url = "phl"
        dataset = "shootings"
        date_field = "date_"
        loader = data_loaders.Carto(url, dataset, date_field)

        assert not loader.isfile()

        count = loader.get_count()

        r = requests.get(f"https://phl.carto.com/api/v2/sql?q=SELECT count(*) FROM {dataset}")
        r.raise_for_status()
        assert count==r.json()["rows"][0]["count"]

        year = 2019
        count = loader.get_count(year=year)

        r = requests.get(f"https://phl.carto.com/api/v2/sql?q=SELECT count(*) FROM {dataset} WHERE {date_field} >= '{year}-01-01' AND {date_field} < '{year+1}-01-01'")
        r.raise_for_status()
        assert count==r.json()["rows"][0]["count"]

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

        data_loaders._default_limit = lim

        if data_loaders._has_gpd:
            assert type(df) == gpd.GeoDataFrame
            data_loaders._has_gpd = False
            df = loader.load(year=year, nrows=nrows, pbar=False)
            data_loaders._has_gpd = True
            assert isinstance(df, pd.DataFrame)

        url2 = "https://phl.carto.com/api/v2/sql?"
        loader2 = data_loaders.Carto(url2, dataset, date_field)
        assert loader.url==loader2.url

    def test_arcgis(self, csvfile, source, last, skip, loghtml):
        lim = data_loaders._default_limit
        data_loaders._default_limit = 500
        data_loaders._verify_arcgis = True
        url = "https://gis.charlottenc.gov/arcgis/rest/services/CMPD/CMPD/MapServer/16"
        gis = data_loaders.Arcgis(url)
        assert not gis.isfile()
        try:
            # Check if arcgis is installed
            from arcgis.features import FeatureLayerCollection
            # Verify that verify is True by getting active layer 
            _ = gis._Arcgis__active_layer
            # Load with verification
            gis.load()
            gis.get_count()
        except:
            pass

        data_loaders._verify_arcgis = False

        # Now load without verification as user would
        gis = data_loaders.Arcgis(url)
        # Confirm that verfication is not set
        with pytest.raises(AttributeError):
            gis._Arcgis__active_layer
        df = gis.load()
        count = gis.get_count()

        assert len(df)==count

        offset = 1
        nrows = count-offset
        df_offset = gis.load(nrows=nrows, offset=offset)
        assert df_offset.equals(df.iloc[offset:offset+nrows].reset_index(drop=True))

        df_offset = gis.load(offset=offset)
        assert df_offset.equals(df.iloc[offset:].reset_index(drop=True))
        
        data_loaders._default_limit = lim

        try:
            from arcgis.features import FeatureLayerCollection
            last_slash = url.rindex("/")
            layer_num = url[last_slash+1:]
            base_url = url[:last_slash]
            layer_collection = FeatureLayerCollection(base_url)

            is_table = True
            active_layer = None
            for layer in layer_collection.layers:
                layer_url = layer.url
                if layer_url[-1] == "/":
                    layer_url = layer_url[:-1]
                if layer_num == layer_url[last_slash+1:]:
                    active_layer = layer
                    is_table = False
                    break

            if is_table:
                for layer in layer_collection.tables:
                    layer_url = layer.url
                    if layer_url[-1] == "/":
                        layer_url = layer_url[:-1]
                    if layer_num == layer_url[last_slash+1:]:
                        active_layer = layer
                        break

            layer_query_result = active_layer.query(as_df=True)
        except:
            url += "/query"
            params = {}
            params["where"] = "1=1"
            params["outFields"] = "*"
            params["f"] = "json"

            r = requests.get(url, params=params)
            r.raise_for_status()

            features = r.json()["features"]
            params["resultOffset"] = len(features)
            r = requests.get(url, params=params)
            r.raise_for_status()

            features.extend(r.json()["features"])
            
            layer_query_result = pd.DataFrame.from_records([x["attributes"] for x in features])

        assert set(df.columns) == set(layer_query_result.columns)
        assert len(layer_query_result) == count
        df = df[layer_query_result.columns]

        assert layer_query_result.equals(df)


    # No datasets currently trigger usage of the legacy server code
    # def test_arcgis_legacy_server(self, csvfile, source, last, skip, loghtml):
    #     url = "https://egis.baltimorecity.gov/egis/rest/services/GeoSpatialized_Tables/Arrest/FeatureServer/0/" 

    #     gis = data_loaders.Arcgis(url)
    #     gis.load(nrows=1)

    #     url = "https://xmaps.indy.gov/arcgis/rest/services/OpenData/OpenData_NonSpatial/MapServer/5/" 
    #     gis = data_loaders.Arcgis(url)
    #     gis.load(nrows=1)


    def test_arcgis_geopandas(self, csvfile, source, last, skip, loghtml):
        if _has_gpd:
            url = "https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer/32/"
            date_field = "TIME_PHONEPICKUP"
            year_filter = 2020
            nrows = 1000
            df = data_loaders.Arcgis(url, date_field=date_field).load(year=year_filter, nrows=nrows)

            assert type(df) == gpd.GeoDataFrame
        else:
            pass

    def test_arcgis_pandas(self, csvfile, source, last, skip, loghtml):
        data_loaders._use_gpd_force = False
        url = "https://services1.arcgis.com/wpJGOi6N4Rq5cqFv/arcgis/rest/services/Pursuits_2020_2021/FeatureServer/0/"
        date_field = "DATE"
        year_filter = 2020
        gis = data_loaders.Arcgis(url, date_field=date_field)
        df = gis.load(year=year_filter)
        count = gis.get_count(year=year_filter)
        # Reset
        data_loaders._use_gpd_force = None

        assert type(df) == pd.DataFrame
        assert len(df) == count

        count2 = gis.get_count(year=year_filter+1)

        # Ensure that count updates properly with different call (most recent count is cached)
        assert count!=count2

    def test_socrata_geopandas(self, csvfile, source, last, skip, loghtml):
        if _has_gpd:
            url = "data.montgomerycountymd.gov"
            data_set = "4mse-ku6q"
            date_field = "date_of_stop"
            year = 2020
            nrows = 1000
            df = data_loaders.Socrata(url=url, data_set=data_set, date_field=date_field).load(year=year, nrows=nrows)

            assert type(df) == gpd.GeoDataFrame
        else:
            pass

    def test_socrata_pandas(self, csvfile, source, last, skip, loghtml):
        data_loaders._use_gpd_force = False
        url = "data.montgomerycountymd.gov"
        data_set = "usip-62e2"
        date_field = "created_dt"
        year = 2020
        loader = data_loaders.Socrata(url=url, data_set=data_set, date_field=date_field)
        df = loader.load(year=year, pbar=False)
        count = loader.get_count(year=year)

        # Reset
        data_loaders._use_gpd_force = None

        assert type(df) == pd.DataFrame
        assert len(df) == count

        count2 = loader.get_count(year=year+1)

        # Ensure that count updates properly with different call (most recent count is cached)
        assert count!=count2

    def test_socrata(self, csvfile, source, last, skip, loghtml):
        lim = data_loaders._default_limit
        data_loaders._default_limit = 500
        url = "data.austintexas.gov"
        data_set = "sc8s-w4ka"
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

        df_offset = loader.load(offset=offset, pbar=False)
        assert set(df.columns)==set(df_offset.columns)
        df_offset = df_offset[df.columns]
        assert df_offset.equals(df.iloc[offset:].reset_index(drop=True))
        
        data_loaders._default_limit = lim

        client = data_loaders.SocrataClient(url, data_loaders.default_sodapy_key, timeout=60)
        results = client.get(data_set, order=":id", limit=100000)
        rows = pd.DataFrame.from_records(results)

        assert len(df) == count
        assert rows.equals(df)

    def test_csv(self, csvfile, source, last, skip, loghtml):
        url = "https://www.denvergov.org/media/gis/DataCatalog/denver_police_officer_involved_shootings/csv/denver_police_officer_involved_shootings.csv"
        date_field = "INCIDENT_DATE"
        loader = data_loaders.Csv(url, date_field=date_field)
        assert loader.isfile()
        df = loader.load(pbar=False)

        offset = 1
        nrows = len(df)-offset-1
        df_offset = loader.load(offset=offset,nrows=nrows, pbar=False)
        assert df_offset.equals(df.iloc[offset:nrows+offset].reset_index(drop=True))
        
        df_offset = loader.load(offset=offset, pbar=False)
        assert df_offset.equals(df.iloc[offset:].reset_index(drop=True))

        df_comp = pd.read_csv(url)
        df_comp = df_comp.astype({date_field: 'datetime64[ns]'})
        df = df.astype({date_field: 'datetime64[ns]'})

        count = loader.get_count()
        assert len(df_comp) == count
        # Test using cached value
        assert count == loader.get_count()

        assert df_comp.equals(df)

        with pytest.raises(ValueError):
            loader.get_years()

        years = loader.get_years(force=True)

        df = df.astype({date_field: 'datetime64[ns]'})
        assert list(df[date_field].dt.year.sort_values(ascending=True).dropna().unique()) == years

        nrows = 7
        df = data_loaders.Csv(url).load(nrows=nrows)
        df_comp = pd.read_csv(url, nrows=nrows)

        assert df_comp.equals(df)


    def test_csv_year_filter(self, csvfile, source, last, skip, loghtml):
        url = "https://www.denvergov.org/media/gis/DataCatalog/denver_police_officer_involved_shootings/csv/denver_police_officer_involved_shootings.csv"
        loader = data_loaders.Csv(url, date_field="INCIDENT_DATE")
        year = 2020
        df = loader.load(year=year, pbar=False)
        with pytest.raises(ValueError):
            count = loader.get_count(year=year)

        count = loader.get_count(year=year, force=True)
        assert len(df) == count

        count2 = loader.get_count(year=year+1, force=True)

        # Ensure that count updates properly with different call (most recent count is cached)
        assert count!=count2


    def test_excel(self, csvfile, source, last, skip, loghtml):
        url = "https://www.norristown.org/DocumentCenter/View/1789/2017-2018-Use-of-Force"
        date_field = "Date"
        loader = data_loaders.Excel(url, date_field=date_field, sheet='2017-2018')
        assert loader.isfile()
        df = loader.load(pbar=False)

        offset = 1
        nrows = len(df)-offset-1
        df_offset = loader.load(offset=offset,nrows=nrows, pbar=False)
        assert df_offset.equals(df.iloc[offset:nrows+offset].reset_index(drop=True))

        df_offset = loader.load(offset=offset, pbar=False)
        assert df_offset.equals(df.iloc[offset:].reset_index(drop=True))

        df_comp = pd.read_excel(url)
        df_comp = df_comp.convert_dtypes()
        df_comp.columns = [x.strip() if isinstance(x, str) else x for x in df_comp.columns]

        with pytest.raises(ValueError):
            count = loader.get_count()
        count = loader.get_count(force=True)
        assert len(df_comp) == count

        # Testing 2nd call which should used cached value
        assert count == loader.get_count(force=True)

        assert df_comp.equals(df)

        with pytest.raises(ValueError):
            loader.get_years()

        years = loader.get_years(force=True)

        df = df.astype({date_field: 'datetime64[ns]'})
        assert list(df[date_field].dt.year.sort_values(ascending=True).dropna().unique()) == years

        nrows = 7
        df = loader.load(nrows=nrows, pbar=False)        
        df_comp = pd.read_excel(url, nrows=nrows)
        df_comp = df_comp.convert_dtypes()
        df_comp.columns = [x.strip() if isinstance(x, str) else x for x in df_comp.columns]
        assert df_comp.equals(df)


    def test_excel_year_sheets(self, csvfile, source, last, skip, loghtml):
        if skip != None:
            skip = skip.split(",")
            skip = [x.strip() for x in skip]
            if "Northampton" in skip:
                return

        url = "https://northamptonpd.com/images/ODP%20Spreadsheets/2014-2020_MV_Pursuits_incident_level_data.xlsx"
        loader = data_loaders.Excel(url, date_field="Date")

        years = loader.get_years()
        assert years == [x for x in range(2014,2021)]

        df_comp = pd.read_excel(url, sheet_name="2014")
        df_comp.columns= [x for x in df_comp.iloc[0]]
        df_comp.drop(index=df_comp.index[0], inplace=True)
        df_comp.reset_index(drop=True, inplace=True)
        df_comp = df_comp.convert_dtypes()
        df_comp.columns = [x.strip() if isinstance(x, str) else x for x in df_comp.columns]
        df_comp = df_comp.iloc[:, 1:]

        # Load all years
        df_2014 = loader.load(year=2014, pbar=False)

        assert df_comp.equals(df_2014)

        df_comp = pd.read_excel(url, sheet_name="2015")
        df_comp.columns= [x for x in df_comp.iloc[0]]
        df_comp.drop(index=df_comp.index[0], inplace=True)
        df_comp.reset_index(drop=True, inplace=True)
        df_comp = df_comp.convert_dtypes()
        df_comp.columns = [x.strip() if isinstance(x, str) else x for x in df_comp.columns]
        df_comp = df_comp.iloc[:, 1:]

        # Load all years
        df_2015 = loader.load(year=2015, pbar=False)

        assert df_comp.equals(df_2015)

        # Note: There is no 2013 data
        df_multi = loader.load(year=[2013,2015], pbar=False)

        assert df_multi.equals(pd.concat([df_2014, df_2015], ignore_index=True))

        df = loader.load(pbar=False)
        df_last = loader.load(year=years[-1], pbar=False)

        assert df.head(len(df_multi)).equals(df_multi)
        assert df.tail(len(df_last)).reset_index(drop=True).equals(df_last.reset_index(drop=True))

        # Test loading to ensure that channel name changes are handled
        data_loaders.Excel("https://northamptonpd.com/images/ODP%20Spreadsheets/NPD_Use_of_Force_2014-2020_incident_level_data.xlsx").load()

    def test_excel_header(self, csvfile, source, last, skip, loghtml):
        url = "https://cms7files1.revize.com/sparksnv/Document_Center/Sparks%20Police/IA%20Data/2000-2022-SPD-OIS-Incidents%20(3).xlsx"

        loader = data_loaders.Excel(url)
        df = loader.load(pbar=False)

        df_comp = pd.read_excel(url)
        df_comp.columns= [x for x in df_comp.iloc[3]]
        df_comp.drop(index=df_comp.index[0:4], inplace=True)
        df_comp.reset_index(drop=True, inplace=True)
        df_comp = df_comp.convert_dtypes()
        df_comp.columns = [x.strip() if isinstance(x, str) else x for x in df_comp.columns]
        df_comp = df_comp.dropna(thresh=10)

        assert(df_comp.equals(df))


    def test_excel_xls(self, csvfile, source, last, skip, loghtml):
        url = r"http://gouda.beloitwi.gov/WebLink/0/edoc/66423/3Use%20of%20Force%202017%20-%20last%20updated%201-12-18.xls"

        try:
            df_comp = pd.read_excel(url)
        except Exception as e:
            if len(e.args) and e.args[0]=='Excel file format cannot be determined, you must specify an engine manually.':
                r = requests.get(url)
                r.raise_for_status()
                text = r.content
                file_like = BytesIO(text)
                df_comp = pd.read_excel(file_like)
            else:
                raise e
        df_comp = df_comp.convert_dtypes()
        df_comp.columns = [x.strip() if isinstance(x, str) else x for x in df_comp.columns]
        df = data_loaders.Excel(url).load()

        assert df_comp.equals(df)


    def test_excel_xls_protected(self, csvfile, source, last, skip, loghtml):
        url = "http://www.rutlandcitypolice.com/app/download/5136813/ResponseToResistance+2015-2017.xls"

        r = requests.get(url)
        r.raise_for_status()

        import os
        import msoffcrypto
        import tempfile
        # Create a file path by joining the directory name with the desired file name
        output_directory = tempfile.gettempdir()
        file_path = os.path.join(output_directory, 'temp1.xls')

        # Write the file
        with open(file_path, 'wb') as output:
            output.write(r.content)

        file_path_decrypted = os.path.join(output_directory, 'temp2.xls')
        # Try and unencrypt workbook with magic password
        fp = open(file_path, 'rb')
        wb_msoffcrypto_file = msoffcrypto.OfficeFile(fp)

        # https://stackoverflow.com/questions/22789951/xlrd-error-workbook-is-encrypted-python-3-2-3
        # https://nakedsecurity.sophos.com/2013/04/11/password-excel-velvet-sweatshop/
        wb_msoffcrypto_file.load_key(password='VelvetSweatshop')
        with open(file_path_decrypted, 'wb') as output:
            wb_msoffcrypto_file.decrypt(output)

        fp.close()

        df_comp = pd.read_excel(open(file_path_decrypted, 'rb'))

        os.remove(file_path)
        os.remove(file_path_decrypted)

        loader = data_loaders.Excel(url)
        df = loader.load(pbar=False)

        df_comp = df_comp.convert_dtypes()
        df_comp.columns = [x.strip() if isinstance(x, str) else x for x in df_comp.columns]
        assert df_comp.equals(df)

if __name__ == "__main__":
    tp = TestProduct()

    # tp.test_carto(None,None,None,None,None)
    # tp.test_arcgis(None,None,None,None,None)
    # tp.test_arcgis_geopandas(None,None,None,None,None)
    # tp.test_arcgis_pandas(None,None,None,None,None)
    # tp.test_csv(None,None,None,None,None)
    # tp.test_csv_year_filter(None,None,None,None,None)
    # tp.test_process_date_input_empty(None,None,None,None,None)
    # tp.test_process_date_too_many(None,None,None,None,None)
    # tp.test_process_dates_year_input_wrong_order(None,None,None,None,None)
    # tp.test_socrata(None,None,None,None,None)
    # tp.test_socrata_geopandas(None,None,None,None,None)
    # tp.test_socrata_pandas(None,None,None,None,None)
    # tp.test_excel(None,None,None,None,None)
    # tp.test_excel_year_sheets(None,None,None,None,None)
    # tp.test_excel_header(None,None,None,None,None)
    tp.test_excel_xls(None,None,None,None,None)
    tp.test_excel_xls_protected(None,None,None,None,None)
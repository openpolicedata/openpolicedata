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

    def test_arcgis(self, csvfile, source, last, skip, loghtml):
        lim = data_loaders._default_limit
        data_loaders._default_limit = 500
        url = "https://gis.charlottenc.gov/arcgis/rest/services/CMPD/CMPD/MapServer/16/"
        gis = data_loaders.Arcgis(url)
        df = gis.load()
        count = gis.get_count()
        
        data_loaders._default_limit = lim

        if url[-1] == "/":
            url = url[0:-1]
        last_slash = url.rindex("/")
        layer_num = url[last_slash+1:]
        base_url = url[:last_slash]
        layer_collection = data_loaders.FeatureLayerCollection(base_url)

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

        assert set(df.columns) == set(layer_query_result.columns)
        assert len(layer_query_result) == count
        df = df[layer_query_result.columns]

        assert layer_query_result.equals(df)

    def test_arcgis_geopandas(self, csvfile, source, last, skip, loghtml):
        if _has_gpd:
            url = "https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer/32/"
            date_field = "TIME_PHONEPICKUP"
            year_filter = 2020
            limit = 1000
            df = data_loaders.Arcgis(url, date_field=date_field).load(year=year_filter, limit=limit)

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

    def test_socrata_geopandas(self, csvfile, source, last, skip, loghtml):
        if _has_gpd:
            url = "data.montgomerycountymd.gov"
            data_set = "4mse-ku6q"
            date_field = "date_of_stop"
            year = 2020
            limit = 1000
            df = data_loaders.Socrata(url=url, data_set=data_set, date_field=date_field).load(year=year, limit=limit)

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
        df = loader.load(year=year)
        count = loader.get_count(year=year)

        # Reset
        data_loaders._use_gpd_force = None

        assert type(df) == pd.DataFrame
        assert len(df) == count

    def test_socrata(self, csvfile, source, last, skip, loghtml):
        lim = data_loaders._default_limit
        data_loaders._default_limit = 500
        url = "data.austintexas.gov"
        data_set = "sc8s-w4ka"
        loader = data_loaders.Socrata(url, data_set)
        df =loader.load()
        count = loader.get_count()
        
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
        df = loader.load()
        df_comp = pd.read_csv(url)
        df_comp = df_comp.astype({date_field: 'datetime64[ns]'})

        count = loader.get_count()
        assert len(df_comp) == count

        assert df_comp.equals(df)

        with pytest.raises(ValueError):
            loader.get_years()

        years = loader.get_years(force=True)

        df = df.astype({date_field: 'datetime64[ns]'})
        assert list(df[date_field].dt.year.sort_values(ascending=True).dropna().unique()) == years

        nrows = 7
        df = data_loaders.Csv(url).load(limit=nrows)
        df_comp = pd.read_csv(url, nrows=nrows)

        assert df_comp.equals(df)


    def test_csv_year_filter(self, csvfile, source, last, skip, loghtml):
        url = "https://www.denvergov.org/media/gis/DataCatalog/denver_police_officer_involved_shootings/csv/denver_police_officer_involved_shootings.csv"
        loader = data_loaders.Csv(url, date_field="INCIDENT_DATE")
        year = 2020
        df = loader.load(year=year)
        with pytest.raises(ValueError):
            count = loader.get_count(year=year)

        count = loader.get_count(year=year, force=True)
        assert len(df) == count


    def test_excel(self, csvfile, source, last, skip, loghtml):
        url = "https://www.norristown.org/DocumentCenter/View/1789/2017-2018-Use-of-Force"
        date_field = "Date"
        loader = data_loaders.Excel(url, date_field=date_field)
        df = loader.load()
        df_comp = pd.read_excel(url)
        df_comp = df_comp.convert_dtypes()
        df_comp.columns = [x.strip() if isinstance(x, str) else x for x in df_comp.columns]

        with pytest.raises(ValueError):
            count = loader.get_count()
        count = loader.get_count(force=True)
        assert len(df_comp) == count

        assert df_comp.equals(df)

        with pytest.raises(ValueError):
            loader.get_years()

        years = loader.get_years(force=True)

        df = df.astype({date_field: 'datetime64[ns]'})
        assert list(df[date_field].dt.year.sort_values(ascending=True).dropna().unique()) == years

        nrows = 7
        df = loader.load(limit=nrows)        
        df_comp = pd.read_excel(url, nrows=nrows)
        df_comp = df_comp.convert_dtypes()
        df_comp.columns = [x.strip() if isinstance(x, str) else x for x in df_comp.columns]
        assert df_comp.equals(df)


    def test_excel_year_sheets(self, csvfile, source, last, skip, loghtml):
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
        df_2014 = loader.load(year=2014)

        assert df_comp.equals(df_2014)

        df_comp = pd.read_excel(url, sheet_name="2015")
        df_comp.columns= [x for x in df_comp.iloc[0]]
        df_comp.drop(index=df_comp.index[0], inplace=True)
        df_comp.reset_index(drop=True, inplace=True)
        df_comp = df_comp.convert_dtypes()
        df_comp.columns = [x.strip() if isinstance(x, str) else x for x in df_comp.columns]
        df_comp = df_comp.iloc[:, 1:]

        # Load all years
        df_2015 = loader.load(year=2015)

        assert df_comp.equals(df_2015)

        # Note: There is no 2013 data
        df_multi = loader.load(year=[2013,2015])

        assert df_multi.equals(pd.concat([df_2014, df_2015], ignore_index=True))

        df = loader.load()
        df_last = loader.load(year=years[-1])

        assert df.head(len(df_multi)).equals(df_multi)
        assert df.tail(len(df_last)).reset_index(drop=True).equals(df_last.reset_index(drop=True))

        # Test loading to ensure that channel name changes are handled
        data_loaders.Excel("https://northamptonpd.com/images/ODP%20Spreadsheets/NPD_Use_of_Force_2014-2020_incident_level_data.xlsx").load()

    def test_excel_header(self, csvfile, source, last, skip, loghtml):
        url = "https://cms7files1.revize.com/sparksnv/Document_Center/Sparks%20Police/Officer%20Involved%20Shooting/2000-2021-SPD-OIS-Incidents.xlsx"

        loader = data_loaders.Excel(url)
        df = loader.load()

        df_comp = pd.read_excel(url)
        df_comp.columns= [x for x in df_comp.iloc[3]]
        df_comp.drop(index=df_comp.index[0:4], inplace=True)
        df_comp.reset_index(drop=True, inplace=True)
        df_comp = df_comp.convert_dtypes()
        df_comp.columns = [x.strip() if isinstance(x, str) else x for x in df_comp.columns]

        assert(df_comp.equals(df))


    def test_excel_xls(self, csvfile, source, last, skip, loghtml):
        url = "http://gouda.beloitwi.gov/WebLink/0/edoc/66423/3Use%20of%20Force%202017%20-%20last%20updated%201-12-18.xls"

        df_comp = pd.read_excel(url)
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
        df = loader.load()

        df_comp = df_comp.convert_dtypes()
        df_comp.columns = [x.strip() if isinstance(x, str) else x for x in df_comp.columns]
        assert df_comp.equals(df)

if __name__ == "__main__":
    tp = TestProduct()

    tp.test_arcgis(None,None,None,None,None)
    tp.test_arcgis_geopandas(None,None,None,None,None)
    tp.test_arcgis_pandas(None,None,None,None,None)
    tp.test_csv(None,None,None,None,None)
    tp.test_csv_year_filter(None,None,None,None,None)
    tp.test_process_date_input_empty(None,None,None,None,None)
    tp.test_process_date_too_many(None,None,None,None,None)
    tp.test_process_dates_year_input_wrong_order(None,None,None,None,None)
    tp.test_socrata(None,None,None,None,None)
    tp.test_socrata_geopandas(None,None,None,None,None)
    tp.test_socrata_pandas(None,None,None,None,None)
    tp.test_excel(None,None,None,None,None)
    tp.test_excel_year_sheets(None,None,None,None,None)
    tp.test_excel_header(None,None,None,None,None)
    tp.test_excel_xls(None,None,None,None,None)
    tp.test_excel_xls_protected(None,None,None,None,None)
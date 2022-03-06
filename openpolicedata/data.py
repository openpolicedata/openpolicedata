import os.path as path
import pandas as pd

if __name__ == '__main__':
    import data_loaders
    import _datasets
else:
    from . import data_loaders
    from . import _datasets

class Table:
    source = None
    state = None
    source_name = None
    jurisdiction = None
    table_type = None
    year = None
    description = None
    url = None

    # Data
    table = None

    # From source
    _data_type = None
    # From LUT in source
    _dataset_id = None
    _date_field = None
    _jurisdiction_field = None

    def __init__(self, source, table=None, year_filter=None, jurisdiction_filter=None):
        if not isinstance(source, pd.core.frame.DataFrame) and \
            not isinstance(source, pd.core.series.Series):
            raise TypeError("data must be an ID, DataFrame or Series")
        elif isinstance(source, pd.core.frame.DataFrame):
            if len(source) == 0:
                raise LookupError("DataFrame is empty")
            elif len(source) > 1:
                raise LookupError("DataFrame has more than 1 row")

            source = source.iloc[0]

        self.source = source
        self.table = table

        self.state = source["State"]
        self.source_name = source["SourceName"]

        if jurisdiction_filter != None:
            self.jurisdiction = jurisdiction_filter
        else:
            self.jurisdiction = source["Jurisdiction"]

        self.table_type = _datasets.TableTypes(source["TableType"])  # Convert to Enum

        if year_filter != None:
            self.year = year_filter
        else:
            self.year = source["Year"]

        self.description = source["Description"]
        self.url = source["URL"]
        self._data_type = _datasets.DataTypes(source["DataType"])  # Convert to Enum

        if "id" in source["LUT"]:
            self._dataset_id = source["LUT"]["id"]

        if "date_field" in source["LUT"]:
            self._date_field = source["LUT"]["date_field"]
        
        if "jurisdiction_field" in source["LUT"]:
            self._jurisdiction_field = source["LUT"]["jurisdiction_field"]


    def to_csv(self, outputDir=None, filename=None):
        if filename == None:
            filename = self.get_csv_filename()
        if outputDir != None:
            filename = path.join(outputDir, filename)
        if not isinstance(self.table, pd.core.frame.DataFrame):
            raise ValueError("There is no table to save to CSV")

        self.table.to_csv(filename)


    def get_csv_filename(self):
        return get_csv_filename(self.state, self.source_name, self.jurisdiction, self.table_type, self.year)


class Source:
    sources = None
    __limit = None

    def __init__(self, source_name, state=None):
        self.sources = _datasets.get(source_name=source_name, state=state)

        # Ensure that all sources are from the same state
        if len(self.sources) == 0:
            raise ValueError("No Sources Found")
        elif self.sources["State"].nunique() > 1:
            raise ValueError("Not all sources are from the same state")


    def get_tables_types(self):
        return list(self.sources["TableType"].unique())


    def get_years(self, table_type=None, force_read=False):
        if isinstance(table_type, _datasets.TableTypes):
            table_type = table_type.value

        df = self.sources
        if table_type != None:
            df = self.sources[self.sources["TableType"]==table_type]

        if len(df) == 1 and df.iloc[0]["Year"] == _datasets.MULTI:
            df = df.iloc[0]

            data_type = _datasets.DataTypes(df["DataType"])
            url = df["URL"]
            if "date_field" in df["LUT"]:
                date_field = df["LUT"]["date_field"]
            else:
                raise ValueError("No date_field is provided to identify the years")
            
            if data_type == _datasets.DataTypes.CSV:
                raise NotImplementedError("This needs to be tested before use")
                if force_read:                    
                    table = pd.read_csv(url, parse_dates=True)
                    years = table[date_field].dt.year
                    years = years.unique()
                else:
                    raise ValueError("Getting the year of a CSV files requires reading in the whole file. " +
                                    "Loading in the table may be a better option. If getYears is still desired " +
                                    " for this case, use forceRead=True")
            elif data_type == _datasets.DataTypes.GeoJSON:
                raise NotImplementedError("This needs to be tested before use")
                if force_read:
                    table = data_loaders.load_geojson(url)
                    years = table[date_field].dt.year
                    years = list(years.unique())
                else:
                    raise ValueError("Getting the year of a GeoJSON files requires reading in the whole file. " +
                                    "Loading in the table may be a better option. If getYears is still desired " +
                                    " for this case, use forceRead=True")
                    
            elif data_type == _datasets.DataTypes.ArcGIS:
                years = data_loaders.get_years_argis(url, date_field)
            elif data_type == _datasets.DataTypes.SOCRATA:
                years = data_loaders.get_years_socrata(url, df["LUT"]["id"], date_field)
            else:
                raise ValueError(f"Unknown data type: {data_type}")
        else:
            years = list(df["Year"].unique())
            
        years.sort()
        return years


    def get_jurisdictions(self, table_type=None, year=None, partial_name=None):
        if isinstance(table_type, _datasets.TableTypes):
            table_type = table_type.value

        src = self.sources
        if table_type != None:
            src = self.sources[self.sources["TableType"]==table_type]

        if year != None:
            src = src[src["Year"] == year]

        if len(src) == 1:
            src = src.iloc[0]
        else:
            raise ValueError("table_type and year inputs must filter for a single source")            

        # If year is multi, need to use self._jurisdictionField to query URL
        # Otherwise return self.jurisdiction
        if src["Jurisdiction"] == _datasets.MULTI:
            data_type = _datasets.DataTypes(src["DataType"])
            if data_type == _datasets.DataTypes.CSV:
                raise NotImplementedError(f"Unable to get jurisdictions for {data_type}")
            elif data_type == _datasets.DataTypes.GeoJSON:
                raise NotImplementedError(f"Unable to get jurisdictions for {data_type}")
            elif data_type == _datasets.DataTypes.ArcGIS:
                raise NotImplementedError(f"Unable to get jurisdictions for {data_type}")
            elif data_type == _datasets.DataTypes.SOCRATA:
                if partial_name is not None:
                    opt_filter = "agency_name LIKE '%" + partial_name + "%'"
                else:
                    opt_filter = None

                select = "DISTINCT " + src["LUT"]["jurisdiction_field"]
                if year == _datasets.MULTI:
                    year = None
                jurisdictionSet = data_loaders.load_socrata(src["URL"], src["LUT"]["id"], 
                    date_field=src["LUT"]["date_field"], year=year, opt_filter=opt_filter, select=select, output_type="set")
                return list(jurisdictionSet)
            else:
                raise ValueError(f"Unknown data type: {data_type}")
        else:
            return [src["Jurisdiction"]]
        

    def load_from_url(self, year, table_type=None, jurisdiction_filter=None):
        # year is either for selecting the correct table to load (if the year is a specific year)
        #   or for filtering the table for a specific year (if the table contains multiple years)
        #   If filtering a table, year must be a single year or the list [startYear, stopYear] to 
        #   retrieve data from startYear to stopYear
        # jurisdictionFilter filters the table for a given jurisdiction for tables that contain 
        #   multiple jurisdictions
        return self.__load(year, table_type, jurisdiction_filter, True)

    def __load(self, year, table_type, jurisdiction_filter, load_table):
        if isinstance(table_type, _datasets.TableTypes):
            table_type = table_type.value

        src = self.sources
        if table_type != None:
            src = src[self.sources["TableType"] == table_type]

        if isinstance(year, list):
            matchingYears = src["Year"] == year[0]
            for y in year[1:]:
                matchingYears = matchingYears | (src["Year"] == y)
        else:
            matchingYears = src["Year"] == year

        filter_by_year = not matchingYears.any()
        if not filter_by_year:
            # Use source for this specific year if available
            src = src[matchingYears]
        else:
            # If there are not any years corresponding to this year, check for a table
            # containing multiple years
            src = src.query("Year == '" + _datasets.MULTI + "'")

        if isinstance(src, pd.core.frame.DataFrame):
            if len(src) == 0:
                raise ValueError(f"There are no sources matching tableType {table_type} and year {year}")
            elif len(src) > 1:
                raise ValueError(f"There is more than one source matching tableType {table_type} and year {year}")
            else:
                src = src.iloc[0]

        # Load data from URL. For year or jurisdiction equal to multi, filtering can be done
        data_type = _datasets.DataTypes(src["DataType"])
        url = src["URL"]

        if filter_by_year:
            year_filter = year
        else:
            year_filter = None

        if "id" in src["LUT"]:
            dataset_id = src["LUT"]["id"]

        table_year = None
        if "date_field" in src["LUT"]:
            date_field = src["LUT"]["date_field"]
            if year_filter != None:
                table_year = year_filter
        else:
            date_field = None
        
        table_jurisdiction = None
        if "jurisdiction_field" in src["LUT"]:
            jurisdiction_field = src["LUT"]["jurisdiction_field"]
            if jurisdiction_filter != None and data_type != _datasets.DataTypes.ArcGIS:
                table_jurisdiction = jurisdiction_filter
        else:
            jurisdiction_field = None
        
        #It is assumed that each data loader method will return data with the proper data type so date type etc...
        if load_table:
            if data_type == _datasets.DataTypes.CSV:
                table = data_loaders.load_csv(url, date_field=date_field, year_filter=year_filter, 
                    jurisdiction_field=jurisdiction_field, jurisdiction_filter=jurisdiction_filter, limit=self.__limit)
            elif data_type == _datasets.DataTypes.GeoJSON:
                table = data_loaders.load_geojson(url, date_field=date_field, year_filter=year_filter, 
                    jurisdiction_field=jurisdiction_field, jurisdiction_filter=jurisdiction_filter)
            elif data_type == _datasets.DataTypes.ArcGIS:
                table = data_loaders.load_arcgis(url, date_field, year_filter, limit=self.__limit)
            elif data_type == _datasets.DataTypes.SOCRATA:
                opt_filter = None
                if jurisdiction_filter != None and jurisdiction_field != None:
                    opt_filter = jurisdiction_field + " = '" + jurisdiction_filter + "'"

                table = data_loaders.load_socrata(url, dataset_id, date_field=date_field, year=year_filter, opt_filter=opt_filter, 
                    limit=self.__limit)
            else:
                raise ValueError(f"Unknown data type: {data_type}")

            if date_field != None and len(table)>0:
                dts = table[date_field]
                dts = dts[dts.notnull()]
                if len(dts) > 0:
                    one_date = dts.iloc[0]            
                    if type(one_date) == str:
                        table = table.astype({date_field: 'datetime64[ns]'})
#                    else:
            #             if pd.to_datetime(one_date, unit="ns").year > 1980:
            #                 table = table.astype({date_field: 'datetime64[ns]'})
            #             else:
            #                 table = table.astype({date_field: 'datetime64[ms]'})
        else:
            table = None

        return Table(src, table, year_filter=table_year, jurisdiction_filter=table_jurisdiction)


    def load_from_csv(self, year, outputDir=None, table_type=None, jurisdiction_filter=None):
        # Load from default CSV file in outputDir (default to cd)

        table = self.__load(year, table_type, jurisdiction_filter, False)

        filename = table.get_csv_filename()
        if outputDir != None:
            filename = path.join(outputDir, filename)            

        table.table = pd.read_csv(filename, parse_dates=True)

        return table


def get_csv_filename(state, source_name, jurisdiction, table_type, year):
    if isinstance(table_type, _datasets.TableTypes):
        table_type = table_type.value
        
    filename = f"{state}_{source_name}"
    if source_name != jurisdiction:
        filename += f"_{jurisdiction}"
    filename += f"_{table_type}"
    if isinstance(year, list):
        filename += f"_{year[0]}_{year[-1]}"
    else:
        filename += f"_{year}"

    # Clean up filename
    filename = filename.replace(",", "_").replace(" ", "_").replace("__", "_")

    filename += ".csv"

    return filename

if __name__ == '__main__':
    src = Source("Denver")
    # print(f"Years for DPD Table are {src.get_years()}")
    # table = src.load_from_url(year = 2020)

    src = Source("Fairfax County")
    print(f"Tables for FCPD are {src.get_tables_types()}")
    print(f"Years for FCPD Arrests Table are {src.get_years(_datasets.TableTypes.ARRESTS)}")
    print(f"Jurisdictions for FCPD Arrests Table are {src.get_jurisdictions(table_type=_datasets.TableTypes.ARRESTS, year=2019)}")

    table = src.load_from_url(year=2020, table_type=_datasets.TableTypes.TRAFFIC_CITATIONS)
    table.to_csv(outputDir="./doc")
    table.to_csv(filename="test.csv")
    table_load_csv = src.load_from_csv(year=2020, table_type=_datasets.TableTypes.TRAFFIC_CITATIONS, outputDir="./doc")

    if len(table.table) != len(table_load_csv.table):
        raise ValueError("CSV table not loaded properly")

    # table = src.load_from_url(year=[2019,2020], table_type=_datasets.TableTypes.TRAFFIC_CITATIONS)  # This should cause an error
    # ffxCit2020 = "https://opendata.arcgis.com/api/v3/datasets/1a262db8328e42d79feac20ec8424b38_0/downloads/data?format=csv&spatialRefId=4326"
    # csvTable = pd.read_csv(ffxCit2020, parse_dates=True)

    # if len(table.table) != len(csvTable):
    #     raise ValueError("Example GeoJSON data was not read in improperly: Lengths are not the same")

    # if not (csvTable["OBJECTID"] == table.table["OBJECTID"]).all():
    #     raise ValueError("Example GeoJSON data was not read in improperly: Test column differs")

    src = Source("Virginia Community Policing Act")
    # print(f"Years for VCPA data are {src.get_years()}")
    jurisdictionsAll = src.get_jurisdictions()
    jurisdictions_ffx =src.get_jurisdictions(partial_name="Fairfax")
    print(f"Jurisdictions for VCPA matching Fairfax are {jurisdictions_ffx}")
    table = src.load_from_url(year=[2020,2020], jurisdiction_filter="Fairfax County Police Department")

    # year = 2020
    # src = Source("Montgomery County")
    # print(f"Years for MCPD data are {src.getYears()}")
    # table = src.getTable(year=year)

    # csvFile = "https://data.montgomerycountymd.gov/api/views/4mse-ku6q/rows.csv?accessType=DOWNLOAD"
    # csvTable = pd.read_csv(csvFile)

    # csvTable = csvTable.astype({"Date Of Stop": 'datetime64[ns]'})
    # csvTable = csvTable[csvTable["Date Of Stop"].dt.year == year]

    # if len(table.table) != len(csvTable):
    #     raise ValueError("Example Socrata data was not read in improperly: Lengths are not the same")

    print("data main function complete")

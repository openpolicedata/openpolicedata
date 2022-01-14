import numbers
import pandas as pd

import DataLoaders
import datasets

class Table:
    source = None
    state = None
    sourceName = None
    jurisdiction = None
    tableType = None
    year = None
    description = None
    url = None

    # Data
    table = None

    # From source
    _dataType = None
    # From LUT in source
    _datasetId = None
    _dateField = None
    _jurisdictionField = None

    def __init__(self, source, table, dateField=None, jurisdictionField=None):
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
        self.sourceName = source["SourceName"]

        if jurisdictionField != None and self.table[jurisdictionField].nunique() == 1:
            # Jurisdiction field only contains 1 value. Use that instead of source value
            self.jurisdiction = self.table.iloc[0][jurisdictionField]
        else:
            self.jurisdiction = source["Jurisdiction"]

        self.tableType = datasets.TableTypes(source["TableType"])  # Convert to Enum

        if dateField != None and self.table[dateField].dt.year.nunique() == 1:
            # Date field only contains dates from 1 year. Use that instead of source value
            self.year = self.table.iloc[0][dateField]
        else:
            self.year = source["Year"]

        self.description = source["Description"]
        self.url = source["URL"]
        self._dataType = datasets.DataTypes(source["DataType"])  # Convert to Enum

        if "id" in source["LUT"]:
            self._datasetId = source["LUT"]["id"]

        if "dateField" in source["LUT"]:
            self._dateField = source["LUT"]["dateField"]
        
        if "jurisdictionField" in source["LUT"]:
            self._jurisdictionField = source["LUT"]["jurisdictionField"]

    # def getJurisdictions(self, partialName=None, year=None):
    #     # If year is multi, need to use self._jurisdictionField to query URL
    #     # Otherwise return self.jurisdiction
    #     if self.year == datasets.MULTI:
    #         if self._dataType == datasets.DataTypes.CSV:
    #             raise ValueError(f"Unable to get jurisdictions for {self._dataType}")
    #         elif self._dataType == datasets.DataTypes.GeoJSON:
    #             raise ValueError(f"Unable to get jurisdictions for {self._dataType}")
    #         elif self._dataType == datasets.DataTypes.ArcGIS:
    #             raise ValueError(f"Unable to get jurisdictions for {self._dataType}")
    #         elif self._dataType == datasets.DataTypes.SOCRATA:
    #             if partialName is not None:
    #                 optFilter = "agency_name LIKE '%" + partialName + "%'"
    #             else:
    #                 optFilter = None

    #             jurisdictionSet = DataLoaders.loadSocrataTable(self.url, self._datasetId, dateField=self._dateField, year=year, 
    #                 optFilter=optFilter, select="DISTINCT agency_name", outputType="set")
    #             return list(jurisdictionSet)
    #         else:
    #             raise ValueError(f"Unknown data type: {self._dataType}")
    #     else:
    #         return [self.jurisdiction]

    def load_from_csv(self, outputDir=None, year=None, jurisdiction=None):
        # Load from default CSV file in outputDir (default to cd)
        pass

    def to_csv(self, outputDir=None, filename=None):
        # Default outputDir is cd
        # If filename is none, there should be a default one
        # Save to CSV
        pass


class Source:
    sources = None

    def __init__(self, sourceName, state=None):
        self.sources = datasets.get(sourceName=sourceName, state=state)

        # Ensure that all sources are from the same state
        if len(self.sources) == 0:
            raise ValueError("No Sources Found")
        elif self.sources["State"].nunique() > 1:
            raise ValueError("Not all sources are from the same state")


    def getTablesList(self):
        return list(self.sources["TableType"].unique())


    def getYears(self, tableType=None, forceRead=False):
        if isinstance(tableType, datasets.TableTypes):
            table = tableType.value

        df = self.sources
        if tableType != None:
            df = self.sources[self.sources["TableType"]==table]

        if len(df) == 1 and df.iloc[0]["Year"] == datasets.MULTI:
            df = df.iloc[0]

            dataType = datasets.DataTypes(df["DataType"])
            url = df["URL"]
            if "dateField" in df["LUT"]:
                dateField = df["LUT"]["dateField"]
            else:
                raise ValueError("No dateField is provided to identify the years")
            
            if dataType == datasets.DataTypes.CSV:
                raise NotImplementedError("This needs to be tested before use")
                if forceRead:                    
                    table = pd.read_csv(url, parse_dates=True)
                    years = table[dateField].dt.year
                    years = years.unique()
                else:
                    raise ValueError("Getting the year of a CSV files requires reading in the whole file. " +
                                    "Loading in the table may be a better option. If getYears is still desired " +
                                    " for this case, use forceRead=True")
            elif dataType == datasets.DataTypes.GeoJSON:
                raise NotImplementedError("This needs to be tested before use")
                if forceRead:
                    table = DataLoaders.loadGeoJSON(url)
                    years = table[dateField].dt.year
                    years = list(years.unique())
                else:
                    raise ValueError("Getting the year of a GeoJSON files requires reading in the whole file. " +
                                    "Loading in the table may be a better option. If getYears is still desired " +
                                    " for this case, use forceRead=True")
                    
            elif dataType == datasets.DataTypes.REQUESTS:
                # TODO: Paul
                raise NotImplementedError("This needs implemented")
            elif dataType == datasets.DataTypes.SOCRATA:
                dates = DataLoaders.loadSocrataTable(url, df["LUT"]["id"], 
                    select="DISTINCT " + dateField, outputType="set")
                dates = list(dates)
                dates = pd.to_datetime(dates)
                years = list(dates.year.unique())
            else:
                raise ValueError(f"Unknown data type: {dataType}")
        else:
            years = list(df["Year"].unique())
            
        years.sort()
        return years


    def getJurisdictions(self, partialName=None, year=None):
        pass


    def getTable(self, tableType=None, year=None, jurisdictionFilter=None):
        # year is either for selecting the correct table to load (if the year is a specific year)
        #   or for filtering the table for a specific year (if the table contains multiple years)
        #   If filtering a table, year must be a single year or the list [startYear, stopYear] to 
        #   retrieve data from startYear to stopYear
        # jurisdictionFilter filters the table for a given jurisdiction for tables that contain 
        #   multiple jurisdictions

        if isinstance(tableType, datasets.TableTypes):
            tableType = tableType.value

        # This will determine whether to filter the table by year or whether to load the table 
        # for a given year. If year is a list, it is always for filtering
        filterByYear = isinstance(year, list)   

        src = self.sources
        if tableType != None:
            src = src[self.sources["TableType"] == tableType]

        if year != None and not filterByYear:
            matchingYears = src["Year"] == year
            filterByYear = not matchingYears.any()
            if not filterByYear:
                # Use source for this specific year if available
                src = src[matchingYears]
            else:
                # If there are not any years corresponding to this year, check for a table
                # containing multiple years
                src = src[self.sources["Year"] == datasets.MULTI]

        if isinstance(src, pd.core.frame.DataFrame):
            if len(src) == 0:
                raise ValueError(f"There are no sources matching tableType {tableType} and year {year}")
            elif len(src) > 1:
                raise ValueError(f"There is more than one source matching tableType {tableType} and year {year}")
            else:
                src = src.iloc[0]

        # Load data from URL. For year or jurisdiction equal to multi, filtering can be done
        dataType = datasets.DataTypes(src["DataType"])
        url = src["URL"]

        if filterByYear:
            yearFilter = year
        else:
            yearFilter = None

        if "id" in src["LUT"]:
            datasetId = src["LUT"]["id"]

        if "dateField" in src["LUT"]:
            dateField = src["LUT"]["dateField"]
        else:
            dateField = None
        
        if "jurisdictionField" in src["LUT"]:
            jurisdictionField = src["LUT"]["jurisdictionField"]
        else:
            jurisdictionField = None
        
        if dataType == datasets.DataTypes.CSV:
            table = pd.read_csv(url, parse_dates=True)
            table = DataLoaders.filterDataFrame(table, dateField=dateField, yearFilter=yearFilter, 
                jurisdictionField=jurisdictionField, jurisdictionFilter=jurisdictionFilter)
        elif dataType == datasets.DataTypes.GeoJSON:
            table = DataLoaders.loadGeoJSON(url, dateField=dateField, yearFilter=yearFilter, 
                jurisdictionField=jurisdictionField, jurisdictionFilter=jurisdictionFilter)
        elif dataType == datasets.DataTypes.ArcGIS:
            table = DataLoaders.loadArcGIS(url, dateField, yearFilter)
        elif dataType == datasets.DataTypes.SOCRATA:
            optFilter = None
            if jurisdictionFilter != None and jurisdictionField != None:
                optFilter = jurisdictionField + " = '" + jurisdictionFilter + "'"

            table = DataLoaders.loadSocrataTable(url, datasetId, dateField=dateField, year=yearFilter, optFilter=optFilter)
        else:
            raise ValueError(f"Unknown data type: {dataType}")

        if dateField != None:
            table = table.astype({dateField: 'datetime64[ns]'})

        return Table(src, table, dateField, jurisdictionField)


if __name__ == '__main__':
    src = Source("Denver Police Department")
    # print(f"Years for DPD Table are {src.getYears()}")
    table = src.getTable(year = 2020)

    # src = Source("Fairfax County Police Department")
    # print(f"Tables for FCPD are {src.getTablesList()}")
    # print(f"Years for FCPD Arrests Table are {src.getYears(datasets.TableTypes.ARRESTS)}")

    # table = src.getTable(datasets.TableTypes.TRAFFIC_CITATIONS, year=2020)
    # ffxCit2020 = "https://opendata.arcgis.com/api/v3/datasets/1a262db8328e42d79feac20ec8424b38_0/downloads/data?format=csv&spatialRefId=4326"
    # csvTable = pd.read_csv(ffxCit2020, parse_dates=True)

    # if len(table.table) != len(csvTable):
    #     raise ValueError("Example GeoJSON data was not read in improperly: Lengths are not the same")

    # if not (csvTable["OBJECTID"] == table.table["OBJECTID"]).all():
    #     raise ValueError("Example GeoJSON data was not read in improperly: Test column differs")

    src = Source("Virginia Community Policing Act")
    print(f"Years for VCPA data are {src.getYears()}")
    table = src.getTable(year=[2020,2020], jurisdictionFilter="Fairfax County Police Department")

    # year = 2020
    # src = Source("Montgomery County Police Department")
    # print(f"Years for MCPD data are {src.getYears()}")
    # table = src.getTable(year=year)

    # csvFile = "https://data.montgomerycountymd.gov/api/views/4mse-ku6q/rows.csv?accessType=DOWNLOAD"
    # csvTable = pd.read_csv(csvFile)

    # csvTable = csvTable.astype({"Date Of Stop": 'datetime64[ns]'})
    # csvTable = csvTable[csvTable["Date Of Stop"].dt.year == year]

    # if len(table.table) != len(csvTable):
    #     raise ValueError("Example Socrata data was not read in improperly: Lengths are not the same")

    print("data main function complete")

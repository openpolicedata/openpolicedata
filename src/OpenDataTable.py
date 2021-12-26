import numbers
import pandas as pd

import DataLoaders
import datasets

class OpenDataTable:
    # From datasets
    id = None
    state = None
    jurisdiction = None
    tableType = None
    year = None
    description = None
    url = None

    # Data
    table = None

    # From datasets
    _dataType = None
    # From LUT in datasets
    _datasetId = None
    _dateField = None
    _jurisdictionField = None

    def __init__(self, data):
        # If data is ID find datasets row. Otherwise, it should be datasets row
        # Then populate class properties
        if isinstance(data, numbers.Number):
            data = datasets.get(id=data)
        elif not isinstance(data, pd.core.frame.DataFrame) and \
            not isinstance(data, pd.core.series.Serires):
            raise TypeError("data must be an ID, DataFrame or Series")

        if isinstance(data, pd.core.frame.DataFrame):
            if len(data) == 0:
                raise LookupError("DataFrame is empty")
            elif len(data) > 1:
                raise LookupError("DataFrame has more than 1 row")

            data = data.iloc[0]

        self.id = data["ID"]
        self.state = data["State"]
        self.jurisdiction = data["Jurisdiction"]
        self.tableType = datasets.TableTypes(data["TableType"])  # Convert to Enum
        self.year = data["Year"]
        self.description = data["Description"]
        self.url = data["URL"]
        self._dataType = datasets.DataTypes(data["DataType"])  # Convert to Enum

        if "id" in data["LUT"]:
            self._datasetId = data["LUT"]["id"]

        if "dateField" in data["LUT"]:
            self._dateField = data["LUT"]["dateField"]
        
        if "jurisdictionField" in data["LUT"]:
            self._jurisdictionField = data["LUT"]["jurisdictionField"]


    def getYears(self):
        # If year is multi, need to use self._yearsField to query URL
        # Otherwise return self.year
        pass

    def getJurisdictions(self, partialName=None, year=None):
        # If year is multi, need to use self._jurisdictionField to query URL
        # Otherwise return self.jurisdiction
        if self.year == datasets.MULTI:
            if self._dataType == datasets.DataTypes.CSV:
                raise ValueError(f"Unable to get jurisdictions for {self._dataType}")
            elif self._dataType == datasets.DataTypes.GeoJSON:
                raise ValueError(f"Unable to get jurisdictions for {self._dataType}")
            elif self._dataType == datasets.DataTypes.REQUESTS:
                raise ValueError(f"Unable to get jurisdictions for {self._dataType}")
            elif self._dataType == datasets.DataTypes.SOCRATA:
                if partialName is not None:
                    optFilter = "agency_name LIKE '%" + partialName + "%'"
                else:
                    optFilter = None

                jurisdictionSet = DataLoaders.loadSocrataTable(self.url, self._datasetId, dateField=self._dateField, year=year, 
                    optFilter=optFilter, select="DISTINCT agency_name", outputType="set")
                return list(jurisdictionSet)
            else:
                raise ValueError(f"Unknown data type: {self._dataType}")
        else:
            return [self.jurisdiction]


    def load(self, year=None, jurisdiction=None):
        # Load data from URL. For year or jurisdiction equal to multi, filtering can be done
        if self._dataType == datasets.DataTypes.CSV:
            # TODO: Paul
            pass
        elif self._dataType == datasets.DataTypes.GeoJSON:
            pass
        elif self._dataType == datasets.DataTypes.ArcGIS:
            # TODO: Paul
            pass
        elif self._dataType == datasets.DataTypes.SOCRATA:
            optFilter = None
            if jurisdiction != None and self._jurisdictionField != None:
                optFilter = self._jurisdictionField + " = '" + jurisdiction + "'"

            self.table = DataLoaders.loadSocrataTable(self.url, self._datasetId, dateField=self._dateField, year=year, optFilter=optFilter)
        else:
            raise ValueError(f"Unknown data type: {self._dataType}")

    def load_from_csv(self, outputDir=None, year=year, jurisdiction=jurisdiction):
        # Load from default CSV file in outputDir (default to cd)
        pass

    def to_csv(self, outputDir=None, filename=None):
        # Default outputDir is cd
        # If filename is none, there should be a default one
        # Save to CSV
        pass


if __name__ == '__main__':
    table = OpenDataTable(18360770769085142775) # Virginia Community Policing Act data

    # Get all jurisdicitions
    agencies = table.getJurisdictions(year=2020)

    # Get jurisdicitions with Fairfax in the name
    ffxAgencies = table.getJurisdictions(partialName="Fairfax", year=2020)

    # Load data for 2020
    dpmt = "Fairfax County Police Department"
    year = 2020
    table.load(year=2020, jurisdiction=dpmt)
    # table.table = table.table.astype({'incident_date': 'datetime64[ns]'})

    # Verify that data is correct
    csvTable = pd.read_csv("https://data.virginia.gov/api/views/segb-5y2c/rows.csv?accessType=DOWNLOAD",parse_dates=['INCIDENT DATE'])
    csvTable = csvTable[csvTable["AGENCY NAME"]==dpmt]
    csvTable = csvTable[csvTable['INCIDENT DATE'].dt.year == year]

    # Assuming that if lengths are the same, then data has been imported properly
    if len(table.table) != len(csvTable):
        raise ValueError("VCPA data was not read in improperly")
    
    # Use Montogomery County data to test big loads
    table = OpenDataTable(datasets.get(jurisdiction="Montgomery County Police Department"))

    csvTable = pd.read_csv("https://data.montgomerycountymd.gov/api/views/4mse-ku6q/rows.csv?accessType=DOWNLOAD",parse_dates=['Date Of Stop'])

    # Test loading for single year
    year = 2020
    table.load(year=year)

    csvTable = csvTable[csvTable['Date Of Stop'].dt.year == year]

    if len(table.table) != len(csvTable):
        raise ValueError("MCPD data was not read in improperly")

    print("OpenDataTable main function complete")
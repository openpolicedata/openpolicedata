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
        if isinstance(data, str):
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

    def getJurisdictions(self):
        # If year is multi, need to use self._jurisdictionField to query URL
        # Otherwise return self.jurisdiction
        pass

    def load(self, year=None, jurisdiction=None):
        # Load data from URL. For year or jurisdiction equal to multi, filtering can be done
        if self._dataType == datasets.DataTypes.CSV:
            pass
        elif self._dataType == datasets.DataTypes.GeoJSON:
            pass
        elif self._dataType == datasets.DataTypes.REQUESTS:
            pass
        elif self._dataType == datasets.DataTypes.SOCRATA:
            optFilter = None
            if jurisdiction != None:
                optFilter = self._jurisdictionField + " = '" + jurisdiction + "'"

            self.table = DataLoaders.loadSocrataTable(self.url, self._datasetId, dateField=self._dateField, year=year, optFilter=optFilter)
        else:
            raise ValueError("Unknown data type")

    def load_from_csv(self, outputDir=None, year=year, jurisdiction=jurisdiction):
        # Load from default CSV file in outputDir (default to cd)
        pass

    def to_csv(self, outputDir=None, filename=None):
        # Default outputDir is cd
        # If filename is none, there should be a default one
        # Save to CSV
        pass


# if __name__ == '__main__':

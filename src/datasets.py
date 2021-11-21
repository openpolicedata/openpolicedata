import pandas as pd
import numpy as np
import uuid

import OpenDataTable as odt

# import datasets
# df = datasets.get()  # Return a dataframe containing all datasets
# df = datasets.get(state="Virginia")  # Return a dataframe containing only datasets for Virginia

_allStates = [
    'Alabama', 
    'Alaska',
    'AmericanSamoa',
    'Arizona',
    'Arkansas',
    'California',
    'Colorado',
    'Connecticut',
    'Delaware',
    'DistrictOfColumbia',
    'Florida',
    'Georgia',
    'Guam',
    'Hawaii',
    'Idaho',
    'Illinois',
    'Indiana',
    'Iowa',
    'Kansas',
    'Kentucky',
    'Louisiana',
    'Maine',
    'Maryland',
    'Massachusetts',
    'Michigan',
    'Minnesota',
    'Mississippi',
    'Missouri',
    'Montana',
    'Nebraska',
    'Nevada',
    'NewHampshire',
    'NewJersey',
    'NewMexico',
    'NewYork',
    'NorthCarolina',
    'NorthDakota',
    'NorthernMarianaIslands',
    'Ohio',
    'Oklahoma',
    'Oregon',
    'Pennsylvania',
    'PuertoRico',
    'RhodeIsland',
    'SouthCarolina',
    'SouthDakota',
    'Tennessee',
    'Texas',
    'Utah',
    'Vermont',
    'VirginIslands',
    'Virginia',
    'Washington',
    'WestVirginia',
    'Wisconsin',
    'Wyoming'
]

# For datasets that contain all jurisdictions in a state
all = "ALL"
# For data sets that put all years in 1 dataset
multi = "MULTI"
_idLength = 8

class _DatasetBuilder:
    columns = {
        'ID' : pd.StringDtype(),
        'State' : pd.StringDtype(),
        'Jurisdiction': pd.StringDtype(),
        'TableType': pd.StringDtype(),
        'Year': np.dtype("O"),
        'Description': pd.StringDtype(),
        'DataType': pd.StringDtype(),
        'URL': pd.StringDtype(),
        'LUT': np.dtype("O"),
    }

    rowData = []


    def addData(self, state, jurisdiction, tableType, url, dataType, years=multi, description="", lutDict={}):
        if state not in _allStates:
            raise ValueError(f"Unknown state: {state}")
        
        if not isinstance(years, list):
            years = [years]

        if not isinstance(url, list):
            url = [url]

        for k, year in enumerate(years):
            self.rowData.append([0, state, jurisdiction, tableType.value, year, description, dataType.value, url[k], lutDict])

    def buildDataFrame(self):
        df = pd.DataFrame(self.rowData, columns=self.columns.keys())
        keyVals = ['State', 'Jurisdiction', 'TableType','Year']
        df.drop_duplicates(subset=keyVals, inplace=True)
        df = df.astype(self.columns)
        df.sort_values(by=keyVals, inplace=True, ignore_index=True)

        df["ID"] = pd.util.hash_pandas_object(df[["State", "Jurisdiction", "TableType", "Year"]], index=False)

        return df
    

_builder = _DatasetBuilder()

###################### Add datasets here #########################
_builder.addData(state="Virginia", jurisdiction=all, tableType=odt.TableTypes.STOPS, url="data.virginia.gov", dataType=odt.DataTypes.SOCRATA, 
    description="A data collection consisting of all traffic and investigatory stops made in Virginia as aggregated by Virginia Department of State Police",
    lutDict={id :"segb-5y2c"})
_builder.addData(state="Virginia", jurisdiction="Fairfax County Police Department",
    tableType=odt.TableTypes.TRAFFIC_WARNINGS, 
    url=["https://opendata.arcgis.com/datasets/f9c4429fb0dc440ba97a0616c99c9493_0.geojson",
        "https://opendata.arcgis.com/datasets/19307e74fb5948c1a9d0270b44ebb638_0.geojson"], 
    dataType=odt.DataTypes.GeoJSON,
    years=[2019,2020],
    description="Traffic Warnings issued by Fairfax County Police")
_builder.addData(state="Virginia", jurisdiction="Fairfax County Police Department",
    tableType=odt.TableTypes.TRAFFIC_CITATIONS, 
    url=["https://opendata.arcgis.com/datasets/67a02d6ebbdf41f089b9afda47292697_0.geojson",
        "https://opendata.arcgis.com/datasets/1a262db8328e42d79feac20ec8424b38_0.geojson"], 
    dataType=odt.DataTypes.GeoJSON,
    years=[2019,2020],
    description="Traffic Citations issued by Fairfax County Police")
_builder.addData(state="Virginia", jurisdiction="Fairfax County Police Department",
    tableType=odt.TableTypes.ARRESTS, 
    url=["https://opendata.arcgis.com/datasets/946c2420324f4151b3526698d14021cd_0.geojson",
        "https://opendata.arcgis.com/datasets/0245b41f40a9439e928f17366dfa0b62_0.geojson",
        "https://opendata.arcgis.com/datasets/26ecb857abeb45bdbb89658b1d2b6eb1_0.geojson",
        "https://opendata.arcgis.com/datasets/71900edc43ed4be79270cdf505b06209_0.geojson",
        "https://opendata.arcgis.com/datasets/c722428522bb4666a609dd282463e11e_0.geojson"], 
    dataType=odt.DataTypes.GeoJSON,
    years=[2016,2017,2018,2019,2020],
    description="Traffic Citations issued by Fairfax County Police")
_builder.addData("Maryland", jurisdiction="Montgomery County Police Department", 
    tableType=odt.TableTypes.TRAFFIC, url="data.montgomerycountymd.gov", dataType=odt.DataTypes.SOCRATA,
    description="This dataset contains traffic violation information from all electronic traffic violations issued in Montgomery County",
    lutDict={id :"4mse-ku6q"})

datasets = _builder.buildDataFrame()


def get(state=None):
    if state == None:
        return datasets
    else:
        return datasets.query("State == '" + state + "'") 


if __name__=="__main__":
    df = get()
    df = get("Virginia")
from enum import Enum
import pandas as pd
import numpy as np
import re

try:
    from .defs import TableType, DataType
except:
    from defs import TableType, DataType

# Location of table where datasets available in opd are stored
csv_file = "https://raw.github.com/openpolicedata/opd-data/main/opd_source_table.csv"

def _build(csv_file):
    # Check columns
    columns = {
        'State' : pd.StringDtype(),
        'SourceName' : pd.StringDtype(),
        'Agency': pd.StringDtype(),
        'TableType': pd.StringDtype(),
        'Year': np.dtype("O"),
        'Description': pd.StringDtype(),
        'DataType': pd.StringDtype(),
        'URL': pd.StringDtype(),
        'date_field': pd.StringDtype(),
        'dataset_id': pd.StringDtype(),
        'agency_field': pd.StringDtype()
    }
    df = pd.read_csv(csv_file, dtype=columns)

    if "Jurisdiction" in df:
        df.rename(columns={
            "Jurisdiction" : "Agency",
            "jurisdiction_field" : "agency_field"
        }, inplace=True)

    # Convert years to int
    df["Year"] = [int(x) if x.isdigit() else x for x in df["Year"]]
    df["SourceName"] = df["SourceName"].str.replace("Police Department", "")
    df["Agency"] = df["Agency"].str.replace("Police Department", "")

    for col in df.columns:
        df[col] = [x.strip() if type(x)==str else x for x in df[col]]

    # ArcGIS datasets should have a URL ending in either /FeatureServer/# or /MapServer/#
    # Where # is a layer #
    urls = df["URL"]
    p = re.compile(r"(MapServer|FeatureServer)/\d+")
    for i,url in enumerate(urls):
        if df.iloc[i]["DataType"] == DataType.ArcGIS.value:
            result = p.search(url)
            urls[i] = url[:result.span()[1]]

    df["URL"] = urls

    keyVals = ['State', 'SourceName', 'Agency', 'TableType','Year']
    df.drop_duplicates(subset=keyVals, inplace=True)
    # df.sort_values(by=keyVals, inplace=True, ignore_index=True)

    return df


datasets = _build(csv_file)


# Datasets that had issues that need added in the future
# _builder.add_data(state="North Carolina", agency="Charlotte-Mecklenburg",
#     table_type=TableType.TRAFFIC, 
#     url=["https://gis.charlottenc.gov/arcgis/rest/services/CMPD/CMPD/MapServer/14/"], 
#     data_type=DataType.ArcGIS,
#     description="Traffic Stops",
#     lut_dict={"date_field" : "Month_of_Stop"})
# _builder.add_data(state="Vermont", agency="Burlington",
#     tableType=TableType.USE_OF_FORCE, 
#     url=["https://data.burlingtonvt.gov/explore/dataset/bpd-use-of-force/"], 
#     data_type=DataType.UNKNOWN,
#     description="Use-of-Force incidents",
#     lut_dict={"date_field" : "call_time"})
# _builder.add_data(state="Vermont", agency="Burlington",
#     tableType=TableType.TRAFFIC, 
#     url=["https://data.burlingtonvt.gov/explore/dataset/bpd-traffic-stops/"], 
#     data_type=DataType.UNKNOWN,
#     description="Traffic Stops",
#     lut_dict={"date_field" : "call_time"})
# _builder.add_data(state="Vermont", agency="Burlington",
#     tableType=TableType.ARRESTS, 
#     url=["https://data.burlingtonvt.gov/explore/dataset/arrests/"], 
#     data_type=DataType.UNKNOWN,
#     description="Arrests",
#     lut_dict={"date_field" : "arrest_date"})
# _builder.add_data(state="Vermont", agency="Burlington",
#     tableType=TableType.ARRAIGNMENT, 
#     url=["https://data.burlingtonvt.gov/explore/dataset/arraignment-and-bail-data/"], 
#     data_type=DataType.UNKNOWN,
#     description="Case level data set on arraignment and bail",
#     lut_dict={"date_field" : "arraignment_date"})
# _builder.add_data(state="California", source_name="California Department of Justice", agency=MULTI,
#     tableType=TableType.DEATHES_IN_CUSTODY, 
#     url=["https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2021-07/DeathInCustody_2005-2020_20210603.xlsx"], 
#     data_type=DataType.EXCEL,
#     escription="State and local law enforcement agencies and correctional facilities report information on deaths that occur in custody or during the process of arrest in compliance with Section 12525 of the California Government Code",
#     lut_dict={"date_field" : "date_of_death_yyyy"})


def datasets_query(source_name=None, state=None, agency=None, table_type=None):
    """Query for available datasets.
    Request a DataFrame containing available datasets based on input filters.
    Returns all datasets if no filters applied.
    
    Parameters
    ----------
    source_name : str
        OPTIONAL name of source to filter by source name
    state : str
        OPTIONAL name of state to filter by state
    agency : str
        OPTIONAL name of agency to filter by agency
    table_type : str or TableType enum
        OPTIONAL name of table type to filter by type of data

    RETURNS
    -------
    Dataframe containing datasets that match any filters applied
    """
    query = ""
    if state != None:
        query += "State == '" + state + "' and "

    if source_name != None:
        query += "SourceName == '" + source_name + "' and "

    if agency != None:
        query += "Agency == '" + agency + "' and " 

    if table_type != None:
        if isinstance(table_type, TableType):
            table_type = table_type.value
        query += "TableType == '" + table_type + "' and "

    if len(query) == 0:
        return datasets.copy()
    else:
        return datasets.query(query[0:-5]) 


if __name__=="__main__":
    df = datasets_query()
    df = datasets_query("Virginia")

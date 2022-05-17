from enum import Enum
import pandas as pd
import numpy as np
import re

# These are the types of data currently available in opd.
# They all have corresponding data loaders in data_loaders.py
# When new data loaders are added, this list should be updated.
class DataTypes(Enum):
    CSV = "CSV"
    # EXCEL = "Excel"
    ArcGIS = "ArcGIS"
    SOCRATA = "Socrata"

# These are the types of tables currently available in opd.
# Add to this list when datasets do not correspond to the below data types
class TableTypes(Enum):
    ARRESTS = "ARRESTS"
    ARRAIGNMENT = "ARRAIGNMENT"
    CALLS_FOR_SERVICE = "CALLS FOR SERVICE"
    CITATIONS = "CITATIONS"
    COMPLAINTS = "COMPLAINTS"
    DEATHES_IN_CUSTODY = "DEATHES IN CUSTODY"
    EMPLOYEE = "EMPLOYEE"
    FIELD_CONTACTS = "FIELD CONTACTS"
    PEDESTRIAN = "PEDESTRIAN STOPS"
    PEDESTRIAN_ARRESTS = "PEDESTRIAN ARRESTS"
    PEDESTRIAN_CITATIONS = "PEDESTRIAN CITATIONS"
    PEDESTRIAN_WARNINGS = "PEDESTRIAN WARNINGS"
    SHOOTINGS = "OFFICER-INVOLVED SHOOTINGS"
    SHOOTINGS_CIVILIANS = "OFFICER-INVOLVED SHOOTINGS - CIVILIANS"
    SHOOTINGS_OFFICERS = "OFFICER-INVOLVED SHOOTINGS - OFFICERS"
    SHOOTINGS_INCIDENTS = "OFFICER-INVOLVED SHOOTINGS - INCIDENTS"
    SHOW_OF_FORCE = "SHOW OF FORCE"
    STOPS = "STOPS"
    TRAFFIC = "TRAFFIC STOPS"
    TRAFFIC_ARRESTS = "TRAFFIC ARRESTS"
    TRAFFIC_CITATIONS = "TRAFFIC CITATIONS"
    TRAFFIC_WARNINGS = "TRAFFIC WARNINGS"
    USE_OF_FORCE = "USE OF FORCE"
    USE_OF_FORCE_CIVILIANS = "USE OF FORCE - CIVILIANS"
    USE_OF_FORCE_OFFICERS = "USE OF FORCE - OFFICERS"
    USE_OF_FORCE_INCIDENTS = "USE OF FORCE - INCIDENTS"
    USE_OF_FORCE_CIVILIANS_OFFICERS = "USE OF FORCE - CIVILIANS/OFFICERS"
    VEHICLE_PURSUITS = "VEHICLE PURSUITS"

# Constants used in dataset parameters
MULTI = "MULTI"    # For data sets that put multiple years or agencies in 1 dataset
NA = "NONE"         # None = not applicable (pandas converts "N/A" to NaN)

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
        if df.iloc[i]["DataType"] == DataTypes.ArcGIS.value:
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
#     table_type=TableTypes.TRAFFIC, 
#     url=["https://gis.charlottenc.gov/arcgis/rest/services/CMPD/CMPD/MapServer/14/"], 
#     data_type=DataTypes.ArcGIS,
#     description="Traffic Stops",
#     lut_dict={"date_field" : "Month_of_Stop"})
# _builder.add_data(state="Vermont", agency="Burlington",
#     tableType=TableTypes.USE_OF_FORCE, 
#     url=["https://data.burlingtonvt.gov/explore/dataset/bpd-use-of-force/"], 
#     data_type=DataTypes.UNKNOWN,
#     description="Use-of-Force incidents",
#     lut_dict={"date_field" : "call_time"})
# _builder.add_data(state="Vermont", agency="Burlington",
#     tableType=TableTypes.TRAFFIC, 
#     url=["https://data.burlingtonvt.gov/explore/dataset/bpd-traffic-stops/"], 
#     data_type=DataTypes.UNKNOWN,
#     description="Traffic Stops",
#     lut_dict={"date_field" : "call_time"})
# _builder.add_data(state="Vermont", agency="Burlington",
#     tableType=TableTypes.ARRESTS, 
#     url=["https://data.burlingtonvt.gov/explore/dataset/arrests/"], 
#     data_type=DataTypes.UNKNOWN,
#     description="Arrests",
#     lut_dict={"date_field" : "arrest_date"})
# _builder.add_data(state="Vermont", agency="Burlington",
#     tableType=TableTypes.ARRAIGNMENT, 
#     url=["https://data.burlingtonvt.gov/explore/dataset/arraignment-and-bail-data/"], 
#     data_type=DataTypes.UNKNOWN,
#     description="Case level data set on arraignment and bail",
#     lut_dict={"date_field" : "arraignment_date"})
# _builder.add_data(state="California", source_name="California Department of Justice", agency=MULTI,
#     tableType=TableTypes.DEATHES_IN_CUSTODY, 
#     url=["https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2021-07/DeathInCustody_2005-2020_20210603.xlsx"], 
#     data_type=DataTypes.EXCEL,
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
    table_type : str or TableTypes enum
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
        if isinstance(table_type, TableTypes):
            table_type = table_type.value
        query += "TableType == '" + table_type + "' and "

    if len(query) == 0:
        return datasets.copy()
    else:
        return datasets.query(query[0:-5]) 


if __name__=="__main__":
    df = datasets_query()
    df = datasets_query("Virginia")

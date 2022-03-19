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
    GeoJSON = "GeoJSON"
    ArcGIS = "ArcGIS"
    SOCRATA = "Socrata"

# These are the types of tables currently available in opd.
# Add to this list when datasets do not correspond to the below data types
class TableTypes(Enum):
    ARRESTS = "ARRESTS"
    TRAFFIC = "TRAFFIC STOPS"
    STOPS = "STOPS"
    TRAFFIC_WARNINGS = "TRAFFIC WARNINGS"
    TRAFFIC_CITATIONS = "TRAFFIC CITATIONS"
    COMPLAINTS = "COMPLAINTS"
    EMPLOYEE = "EMPLOYEE"
    USE_OF_FORCE = "USE OF FORCE"
    ARRAIGNMENT = "ARRAIGNMENT"
    DEATHES_IN_CUSTODY = "DEATHES IN CUSTODY"
    CALLS_FOR_SERVICE = "CALLS FOR SERVICE"
    SHOOTINGS = "OFFICER-INVOLVED SHOOTINGS"

# Constants used in dataset parameters
MULTI = "MULTI"    # For data sets that put multiple years or jurisdictions in 1 dataset
NA = "None"         # None = not applicable (pandas converts "N/A" to NaN)

def _build():
    csv_file = "https://raw.github.com/openpolicedata/opd-data/main/opd_source_table.csv"

    # Check columns
    columns = {
        'State' : pd.StringDtype(),
        'SourceName' : pd.StringDtype(),
        'Jurisdiction': pd.StringDtype(),
        'TableType': pd.StringDtype(),
        'Year': np.dtype("O"),
        'Description': pd.StringDtype(),
        'DataType': pd.StringDtype(),
        'URL': pd.StringDtype(),
        'date_field': pd.StringDtype(),
        'dataset_id': pd.StringDtype(),
        'jurisdiction_field': pd.StringDtype()
    }
    df = pd.read_csv(csv_file, dtype=columns)

    # Convert years to int
    df["Year"] = [int(x) if x.isdigit() else x for x in df["Year"]]
    df["SourceName"] = df["SourceName"].str.replace("Police Department", "")
    df["Jurisdiction"] = df["Jurisdiction"].str.replace("Police Department", "")

    for col in df.columns:
        df[col] = [x.strip() if type(x)==str else x for x in df[col]]

    # ArcGIS datasets should have a URL ending in either /FeatureServer/# or /MapServer/#
    # Where # is a layer #
    urls = df["URL"]
    p = re.compile("(MapServer|FeatureServer)/\d+")
    for i,url in enumerate(urls):
        if df.iloc[i]["DataType"] == DataTypes.ArcGIS.value:
            result = p.search(url)
            urls[i] = url[:result.span()[1]]

    df["URL"] = urls

    keyVals = ['State', 'SourceName', 'Jurisdiction', 'TableType','Year']
    df.drop_duplicates(subset=keyVals, inplace=True)
    df.sort_values(by=keyVals, inplace=True, ignore_index=True)

    return df


datasets = _build()


# Datasets that had issues that need readded in the future
# _builder.add_data(state="North Carolina", jurisdiction="Charlotte-Mecklenburg",
#     table_type=TableTypes.TRAFFIC, 
#     url=["https://gis.charlottenc.gov/arcgis/rest/services/CMPD/CMPD/MapServer/14/"], 
#     data_type=DataTypes.ArcGIS,
#     description="Traffic Stops",
#     lut_dict={"date_field" : "Month_of_Stop"})
# _builder.add_data(state="Vermont", jurisdiction="Burlington",
#     tableType=TableTypes.USE_OF_FORCE, 
#     url=["https://data.burlingtonvt.gov/explore/dataset/bpd-use-of-force/"], 
#     data_type=DataTypes.UNKNOWN,
#     description="Use-of-Force incidents",
#     lut_dict={"date_field" : "call_time"})
# _builder.add_data(state="Vermont", jurisdiction="Burlington",
#     tableType=TableTypes.TRAFFIC, 
#     url=["https://data.burlingtonvt.gov/explore/dataset/bpd-traffic-stops/"], 
#     data_type=DataTypes.UNKNOWN,
#     description="Traffic Stops",
#     lut_dict={"date_field" : "call_time"})
# _builder.add_data(state="Vermont", jurisdiction="Burlington",
#     tableType=TableTypes.ARRESTS, 
#     url=["https://data.burlingtonvt.gov/explore/dataset/arrests/"], 
#     data_type=DataTypes.UNKNOWN,
#     description="Arrests",
#     lut_dict={"date_field" : "arrest_date"})
# _builder.add_data(state="Vermont", jurisdiction="Burlington",
#     tableType=TableTypes.ARRAIGNMENT, 
#     url=["https://data.burlingtonvt.gov/explore/dataset/arraignment-and-bail-data/"], 
#     data_type=DataTypes.UNKNOWN,
#     description="Case level data set on arraignment and bail",
#     lut_dict={"date_field" : "arraignment_date"})
# _builder.add_data(state="California", source_name="California Department of Justice", jurisdiction=MULTI,
#     tableType=TableTypes.DEATHES_IN_CUSTODY, 
#     url=["https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2021-07/DeathInCustody_2005-2020_20210603.xlsx"], 
#     data_type=DataTypes.EXCEL,
#     escription="State and local law enforcement agencies and correctional facilities report information on deaths that occur in custody or during the process of arrest in compliance with Section 12525 of the California Government Code",
#     lut_dict={"date_field" : "date_of_death_yyyy"})
# include_comport = False  # These dataset links are unreliable 2/10/2022
# if include_comport:
#     _builder.add_data(state="Maryland", jurisdiction="Baltimore",
#         table_type=TableTypes.COMPLAINTS, 
#         url=["https://www.projectcomport.org/department/4/complaints.csv"], 
#         data_type=DataTypes.CSV,
#         description="The Baltimore Police Department’s Office of Professional Responsibility tracks and investigates both internal complaints and citizen complaints regarding officer interactions in order to better serve the people of Baltimore.",
#         lut_dict={"date_field" : "occurredDate"})
#     _builder.add_data(state="Maryland", jurisdiction="Baltimore",
#         table_type=TableTypes.USE_OF_FORCE, 
#         url=["https://www.projectcomport.org/department/4/uof.csv"], 
#         data_type=DataTypes.CSV,
#         description="Officers must immediately report any use of force incident, and all incidents undergo a thorough review process to ensure that the force used was reasonable, necessary, and proportional.",
#         lut_dict={"date_field" : "occurredDate"})
#     _builder.add_data(state="Maryland", jurisdiction="Baltimore",
#         table_type=TableTypes.SHOOTINGS, 
#         url=["https://www.projectcomport.org/department/4/ois.csv"], 
#         data_type=DataTypes.CSV,
#         description="",
#         lut_dict={"date_field" : "occurredDate"})
#     _builder.add_data(state="Indiana", jurisdiction="Indianapolis",
#         table_type=TableTypes.COMPLAINTS, 
#         url=["https://www.projectcomport.org/department/1/complaints.csv"], 
#         data_type=DataTypes.CSV,
#         description="The Citizens' Police Complaint Office (CPCO) gathers this data as part of accepting and investigating resident complaints about interactions with IMPD officers. More information is available in the CPCO FAQ (http://www.indy.gov/eGov/City/DPS/CPCO/Pages/faq.aspx).",
#         lut_dict={"date_field" : "occurredDate"})
#     _builder.add_data(state="Indiana", jurisdiction="Indianapolis",
#         table_type=TableTypes.USE_OF_FORCE, 
#         url=["https://www.projectcomport.org/department/1/uof.csv"], 
#         data_type=DataTypes.CSV,
#         description="The Indianapolis Metropolitan Police Department (IMPD) gathers this data as part of its professional standards practices.",
#         lut_dict={"date_field" : "occurredDate"})
#     _builder.add_data(state="Indiana", jurisdiction="Indianapolis",
#         table_type=TableTypes.SHOOTINGS, 
#         url=["https://www.projectcomport.org/department/1/ois.csv"], 
#         data_type=DataTypes.CSV,
#         description="The Indianapolis Metropolitan Police Department (IMPD) gathers this data as part of its professional standards practices.",
#         lut_dict={"date_field" : "occurredDate"})
#     _builder.add_data(state="Kansas", jurisdiction="Wichita",
#         table_type=TableTypes.COMPLAINTS, 
#         url=["https://www.projectcomport.org/department/7/complaints.csv"], 
#         data_type=DataTypes.CSV,
#         description="",
#         lut_dict={"date_field" : "receivedDate"})
#     _builder.add_data(state="Kansas", jurisdiction="Wichita",
#         table_type=TableTypes.USE_OF_FORCE, 
#         url=["https://www.projectcomport.org/department/7/uof.csv"], 
#         data_type=DataTypes.CSV,
#         description="The Wichita Police Department tracks all incidents of force used in a situation during the line of duty as part of its office of Professional Standards. ",
#         lut_dict={"date_field" : "occurredDate"})


def datasets_query(source_name=None, state=None, jurisdiction=None, table_type=None):
    """Query for available datasets.
    Request a DataFrame containing available datasets based on input filters.
    Returns all datasets if no filters applied.
    
    Parameters
    ----------
    source_name : str
        OPTIONAL name of source to filter by source name
    state : str
        OPTIONAL name of state to filter by state
    jurisdiction : str
        OPTIONAL name of jurisdiction to filter by jurisdiction
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

    if jurisdiction != None:
        query += "Jurisdiction == '" + jurisdiction + "' and " 

    if table_type != None:
        if isinstance(table_type, TableTypes):
            table_type = table_type.value
        query += "TableType == '" + table_type + "' and "

    if len(query) == 0:
        return datasets
    else:
        return datasets.query(query[0:-5]) 


if __name__=="__main__":
    df = datasets_query()
    df = datasets_query("Virginia")

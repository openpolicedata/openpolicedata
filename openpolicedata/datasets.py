import pandas as pd
import numpy as np
import re
import warnings

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
        'agency_field': pd.StringDtype(),
        'readme': pd.StringDtype(),
        'min_version': pd.StringDtype()
    }

    try:
        df = pd.read_csv(csv_file, dtype=columns)
    except:
        warnings.warn(f"Unable to load CSV file from {csv_file}. " +
            "This may be due to a bad internet connection or bad filename/URL.")
        return None

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

def query(source_name=None, state=None, agency=None, table_type=None):
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
    df = query()
    df = query("Virginia")

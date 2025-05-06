from __future__ import annotations  # This should not be necessary once Python 3.7 is no longer supported
import pandas as pd
import numpy as np
import re
from typing import Optional, Union
import warnings

from . import defs, dataset_id
from .deprecated._pandas import DeprecationHandlerDataFrame
from .deprecated.source_table_compat import check_compat_source_table

# Location of table where datasets available in opd are stored
csv_file = "https://raw.github.com/openpolicedata/opd-data/main/opd_source_table.csv"

_column_types = {
        'State' : pd.StringDtype(),
        'SourceName' : pd.StringDtype(),
        'Agency': pd.StringDtype(),
        'TableType': pd.StringDtype(),
        'Year': np.dtype("O"),
        'Description': pd.StringDtype(),
        'DataType': pd.StringDtype(),
        'URL': pd.StringDtype(),
        'date_field': pd.StringDtype(),
        'agency_field': pd.StringDtype(),
        'readme': pd.StringDtype(),
        'min_version': pd.StringDtype(),
        'py_min_version':pd.StringDtype()
    }


def _build(csv_file, error=False):
    warnings.simplefilter('default', DeprecationWarning)

    if isinstance(csv_file, pd.DataFrame):
        df = csv_file.copy()
    else:
        loaded, df, _ = check_compat_source_table(column_types=_column_types)

        if not loaded:
            try:
                df = pd.read_csv(csv_file, dtype=_column_types)
            except:
                if error:
                    raise
                warnings.warn(f"Unable to load CSV file from {csv_file}. " +
                    "This may be due to a bad internet connection or bad filename/URL.")
                return None

    if "Jurisdiction" in df:
        df.rename(columns={
            "Jurisdiction" : "Agency",
            "jurisdiction_field" : "agency_field"
        }, inplace=True)

    # Convert years to int
    df["Year"] = [int(x) if isinstance(x,str) and x.isdigit() else x for x in df["Year"]]
    df["Year"] = df["Year"].apply(lambda x: defs.MULTI if x=="MULTI" else x)
    df["SourceName"] = df["SourceName"].str.replace("Police Department", "")
    df["Agency"] = df["Agency"].str.replace("Police Department", "").apply(lambda x: defs.MULTI if x=="MULTI" else x)

    df['dataset_id'] = dataset_id.parse(df['dataset_id'])

    for col in df.columns:
        df[col] = [x.strip() if isinstance(x, str) else x for x in df[col]]

    # ArcGIS datasets should have a URL ending in either /FeatureServer/# or /MapServer/#
    # Where # is a layer #
    urls = list(df["URL"])
    p = re.compile(r"(MapServer|FeatureServer)/\d+")
    for i,url in enumerate(urls):
        if df.iloc[i]["DataType"] == defs.DataType.ArcGIS:
            result = p.search(url)
            urls[i] = url[:result.span()[1]]

    df["URL"] = urls

    key_vals = ['State', 'SourceName', 'Agency', 'TableType','Year', 'coverage_start', 'coverage_end']
    df.drop_duplicates(subset=key_vals, inplace=True, ignore_index=True)

    if "coverage_start" in df:
        p = re.compile(r"\d{1,2}/\d{1,2}/\d{4}")
        df["coverage_start"] = df["coverage_start"].apply(lambda x: pd.to_datetime(x) if \
                                                          not isinstance(x,pd.Timestamp) and pd.notnull(x) and p.search(x) else x)
        df["coverage_end"] = df["coverage_end"].apply(lambda x: pd.to_datetime(x) if \
                                                      not isinstance(x,pd.Timestamp) and pd.notnull(x) and p.search(x) else x)

    return DeprecationHandlerDataFrame(df)


datasets = _build(csv_file)


def reload(csvfile: Union[str,pd.DataFrame] = csv_file):
    """Reload default datasets CSV file or load datasets CSV from local file. Useful if datasets file may have been updated.

    Parameters
    ----------
    csvfile : str, pd.DataFrame, optional
        OPTIONAL CSV file location or pandas DataFrame, by default the default OPD CSV file will be loaded from GitHub
    """

    from .import datasets
    datasets.datasets = _build(csvfile, error=True)
    

def query(
    source_name: Optional[str] = None, 
    state: Optional[str] = None, 
    agency: Optional[str] = None,
    table_type: Union[str,defs.TableType,None] = None
) -> pd.DataFrame:
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
    query_str = ""
    if state != None:
        query_str += "State == '" + state + "' and "

    if source_name != None:
        query_str += "SourceName == '" + source_name + "' and "

    if agency != None:
        query_str += "Agency == '" + agency + "' and " 

    if table_type != None:
        query_str += "TableType == '" + table_type + "' and "

    if len(query_str) == 0:
        return datasets.copy()
    else:
        return datasets.query(query_str[0:-5]).copy()

def num_unique() -> int:
    """_summary_

    Returns:
        int: _description_
    """
    return len(query().drop_duplicates(["State","SourceName","Agency","TableType"]))

def num_sources(full_states_only: bool = False) -> int:
    """_summary_

    Args:
        full_states_only (bool, optional): _description_. Defaults to False.

    Returns:
        int: _description_
    """
    d = query().drop_duplicates(subset=["State","SourceName","Agency"])

    if full_states_only:
        return ((d["State"]==d["SourceName"]) & (d["Agency"]==defs.MULTI)).sum()
    else:
        return len(d)

def summary_by_state(by: Optional[str] = None) -> pd.DataFrame:
    df = query()
    df_unique = df.drop_duplicates(["State","SourceName","Agency","TableType"])
    s = df_unique.groupby("State").size()
    out = pd.DataFrame(s,columns=["Total"])

    by_year = type(by)==str and by.lower() == "year"
    by_table = type(by)==str and by.lower() == "table"
    if not by_year and not by_table and by!=None:
        raise ValueError("by must be None, 'year', or 'state'")
    if by_year:
        s = df.drop_duplicates(["State","SourceName","Agency","TableType","Year"]).groupby(["State","Year"]).size().unstack()
        s = s.fillna(0).convert_dtypes(convert_integer=True)
        s = s[s.columns[::-1]]
        s.rename(columns={"NONE":"N/A",defs.MULTI:"MULTI-YEAR"},inplace=True)
        out = pd.concat([out, s],axis=1)
    elif by_table:
        s = df_unique.groupby(["State","TableType"]).size().unstack().fillna(0).convert_dtypes(convert_integer=True)
        out = pd.concat([out, s],axis=1)

    out.sort_values(by="Total",inplace=True, ascending=False)

    # Group related tables
    k = 0
    empty_row = ["" for _ in out.columns]
    while k < len(out):
        df_state = df[(df["State"]==out.index[k]) & (df["SourceName"]==out.index[k]) & (df["Agency"]==defs.MULTI)]
        s = df_state.drop_duplicates(["State","SourceName","Agency","TableType"]).groupby("State").size()
        row_state = pd.DataFrame(s,columns=["Total"])

        if len(row_state) > 0:
            if by_year:
                s = df_state.drop_duplicates(["State","SourceName","Agency","TableType","Year"]).groupby(["State","Year"]).size().unstack()
                s = s.fillna(0).convert_dtypes(convert_integer=True)
                s = s[s.columns[::-1]]
                s.rename(columns={"NONE":"N/A",defs.MULTI:"MULTI-YEAR"},inplace=True)
                row_state = pd.concat([row_state, s],axis=1)
            elif by_table:
                s = df_state.drop_duplicates(["State","SourceName","Agency","TableType"]).groupby(["State","TableType"]).size().unstack().fillna(0).convert_dtypes(convert_integer=True)
                row_state = pd.concat([row_state, s],axis=1)

            out_start = out.iloc[0:k]
            out_end = out.iloc[k+1:]
            # This indexing keeps this a DataFrame
            out_cur = out.iloc[k:k+1].convert_dtypes()

            row_single = out_cur.sub(row_state, fill_value=0)
            row_single = row_single.astype({x:"int" for x in row_single.columns})
            row_state.index = ["  All State Agencies"]
            row_single.index = ["  Individual Agency"]

            out_cur = pd.concat([
                pd.DataFrame([empty_row],columns=out.columns,index=out_cur.index),
                row_state, row_single
                ]).fillna(0)
            
            out = pd.concat([out_start,out_cur,out_end],axis=0)
            k = len(out_start) + len(out_cur) 
        else:
            k+=1

    out.index.name = "State"
    return out

def summary_by_table_type(by_year: bool = False) -> pd.DataFrame:
    df = query()
    s = df.drop_duplicates(["State","SourceName","Agency","TableType"]).groupby("TableType").size()
    out = pd.DataFrame(s,columns=["Total"])
    if by_year:
        s = df.drop_duplicates(["State","SourceName","Agency","TableType","Year"]).groupby(["TableType","Year"]).size().unstack()
        s = s.fillna(0).convert_dtypes(convert_integer=True)
        s = s[s.columns[::-1]]
        s.rename(columns={"NONE":"N/A",defs.MULTI:"MULTI-YEAR"},inplace=True)
        out = pd.concat([out, s],axis=1)

    out.sort_values(by="Total",inplace=True, ascending=False)
    definitions = []
    for x in out.index:
        try:
            definitions.append(defs.TableType(x).description)
        except:
            warnings.warn(f"{x} is not a known table type in defs.TableType")
            definitions.append("")

    out["Definition"] = definitions

    # Group related tables
    groups = ["STOPS", "CITATIONS","ARRESTS","WARNINGS","OFFICER-INVOLVED SHOOTINGS","USE OF FORCE",
              "CRASHES", "COMPLAINTS"]
    k = 0
    empty_row = ["" for _ in out.columns]
    while k < len(out):
        cur_group = [x for x in groups if x in out.index[k]]
        if len(cur_group)==0:
            k+=1
            continue
        elif len(cur_group)>1:
            raise ValueError("Multiple groups found")
        
        cur_group = cur_group[0]

        matches = [x for x in out.index if cur_group in x]
        multi_table = any(["INCIDENT" in x for x in matches]) or \
            any(["OFFICER" in x for x in matches]) or any(["CIVILIAN" in x for x in matches])

        out_start = out.iloc[0:k]
        out_end = out.iloc[k:].drop(index=matches)
        out_cur = out.loc[matches]

        if multi_table: 
            index = [cur_group+"-RELATED", "  Single Table  ", "    "+cur_group, "  Multi-Table  "]
            index.extend(["    "+x+"    " for x in matches if x!=cur_group])
            out_cur = pd.concat([
                pd.DataFrame([empty_row,empty_row],columns=out.columns),
                pd.DataFrame([out_cur.loc[cur_group]],columns=out.columns),
                pd.DataFrame([empty_row],columns=out.columns),
                out_cur.loc[[x for x in matches if x!=cur_group]]
                ],ignore_index=True)  
            out_cur.index = index  
        else:
            out_cur.rename(index={cur_group : cur_group+" (All)"}, inplace=True)
            out_cur.rename(index={x:x+" (Only)" for x in out_cur.index if "(All)" not in x}, inplace=True)
            out_cur.rename(index={x:"  "+x+"  " for x in out_cur.index}, inplace=True)
            # Add empty row for spacing
            out_cur  = pd.concat([pd.DataFrame([empty_row],index=[cur_group+"-RELATED"],columns=out.columns),out_cur],axis=0)
            # Find all indices for group
        
        out = pd.concat([out_start,out_cur,out_end],axis=0)
        k = len(out_start) + len(out_cur)

    out.index.name = "TableType"
    return out

def get_table_types(contains=None):
    df = query()
    table_types = df["TableType"].unique()
    if contains is not None:
        table_types = [x for x in table_types if contains.lower() in x.lower()]
    table_types.sort()
    return table_types

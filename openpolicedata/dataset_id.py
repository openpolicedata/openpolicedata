'''
This module handles non-null values in the dataset_id column

Most values will be strings indicating a dataset ID or Excel sheet name

More complicated cases where a year's worth of data is spread across multiple datasets will typically be 
json strings that are converted to dictionaries or lists of dictionaries by the parse function below.
The dictionary option is just a simplification of a length 1 list of dictionaries. Each dictionary can
have the following keys:
1. url (str or list of str): 1 or more partial URLs to be appended to the main URL. This is only necessary if multiple files 
from the same base URL are to be accessed.
2. file (str or list of str): The name of 1 or more files in a zip file. This is only necessary if there is more than 1 file in the zip file.
3. sheets (list of str): The name(s) of sheets in an Excel file

For a given dictionary, the defined sheets will be loaded from each file defined by all url and file parameters.

All keys are optional. [{'file','spreadsheet1.xlsx', 'sheets', ['Sheet1','Sheet2']}]
indicates that the file spreadsheet1.xlsx is to be extracted from a zip file and sheets Sheet1 and Sheet2
are to be loaded from the Excel file. [{'file',['spreadsheet1.xlsx','spreadsheet2.xlsx'], 'sheets', ['Sheet1','Sheet2']}]
indicates that the files spreadsheet1.xlsx and spreadsheet2.xlsx are to be extracted from a zip file and sheets Sheet1 and Sheet2
are to be loaded from each Excel file. [{'file',['spreadsheet1.xlsx','spreadsheet2.xlsx']}]
indicates that the files spreadsheet1.xlsx and spreadsheet2.xlsx are to be extracted from a zip file and the first sheet will be loaded
from each.

If both are defined, the number of files and urls must match and for each url/file pair, the defined sheets (if any) will be loaded.

Tables from all spreadsheets defined the dataset id in the format described above are appended together.

It is also possible to join tables together. In this case, dataset_id should be a list of list(s) of dictionaries where each list of
dictionaries is defined as described above with the exception that they should have an addtional field "on" that indicates which 
field to join on. Each list of dictionaries will be joined together. In the following example the Survey1 sheet from Spreadsheet1.xlsx will be
merged with the the Survey2 sheet from Spreadsheet2.xlsx using the Case Number and Case # fields, respectively.
[[{"sheets": "Survey1", "files": "Spreadsheet1.xlsx", "on":"Case Number"}], [{"sheets": "Survey2", "files": "Spreadsheet2.xlsx", "on":"Case #"}]]
'''

import json
import pandas as pd
import re

def notnull(x):
    return isinstance(x, list) or pd.notnull(x)

def isnull(x):
    return not notnull(x)

def parse_id(x):
    return json.loads(x) if isinstance(x,str) and x.startswith(('[','{')) else x

def parse(s):
    return s.apply(parse_id)

def expand(id):
    if not isinstance(id, list) and not isinstance(id, dict):
        # This is either null or a string
        return id

    if not isinstance(id, list):
        id = [id]

    out = []
    for x in id:
        if isinstance(x, list):
            out.append(expand(x))
            continue
        
        files = x['files'] if 'files' in x else [None]
        urls = x['urls'] if 'urls' in x else [None]
        sheets = x['sheets'] if 'sheets' in x else None
        on = x['on'] if 'on' in x else None

        files = [files] if isinstance(files, str) else files
        urls = [urls] if isinstance(urls, str) else urls
        sheets = [sheets] if isinstance(sheets, str) else sheets

        # All arrays should be length 1 or otherwise the same length
        n = max(len(files), len(urls))
        assert len(files) in [1,n] and len(urls) in [1,n]

        # Repeat length 1 arrays to be length n
        files = [files[0] for _ in range(n)] if len(files)==1 else files
        urls = [urls[0] for _ in range(n)] if len(urls)==1 else urls

        for u, f in zip(urls, files):
            d = {}
            if u:
                d['url'] = u.strip()
            if f:
                d['file'] = f.strip()
            if sheets:
                d['sheets'] = sheets
            if on:
                d['on'] = on

            out.append(d)

    return out


def is_combined_dataset(dataset):
    if isinstance(dataset, list):
        if isinstance(dataset[0], list):
            return True
        else:
            firstfile = dataset[0]['file'] if 'file' in dataset[0] else None
            for x in dataset:
                if 'url' in x:
                    return True
                
                file = x['file'] if 'file' in x else None
                if file!=firstfile:  # There is more than 1 file case
                    return True

    return False


def parse_excel_dataset(is_zip, id):
    file = None
    sheets = None
    if isinstance(id, list):
        assert len(id)==1
        id = id[0]

    if (isinstance(id, dict) and len(id)>0):
        if 'sheets' in id:
            sheets = id['sheets']
            sheets = sheets if isinstance(sheets, list) else [sheets]
        file = id['file'] if 'file' in id else None
    elif isinstance(id, str):
        if re.match(r'^[“”"].+[“”"]$', id):
            # Sheet name was put in quotes due to it being a number to prevent Excel from dropping any zeros from the front
            id = id[1:-1]
        if is_zip:
            file = id
        else:
            sheets = [id]

    return sheets, file
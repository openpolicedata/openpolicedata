from io import BytesIO
import json
import pytest
import re
import requests
import sys

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders, dataset_id
import pandas as pd

@pytest.mark.parametrize("url, dataset", [
    ("https://wallkillpd.org/document-center/data/vehicle-a-pedestrian-stops/2016-vehicle-a-pedestrian-stops", 
    '{"urls": ["147-2016-2nd-quarter-vehicle-a-pedestrian-stops/file.html", "148-2016-3rd-quarter-vehicle-a-pedestrian-stops/file.html", "167-2016-4th-quarter-vehicle-pedestrian-stops/file.html"]}'), 
    ("http://www.bremertonwa.gov/DocumentCenter/View", 
    '{"sheets": "*arrest*", "urls": ["4713/January-2017-XLSX", "4873/February-2017-XLSX", "4872/March-2017-XLSX", "https://raw.githubusercontent.com/openpolicedata/opd-datasets/main/data/Washington_Bremerton_ARRESTS_April_2017.csv", "5026/May-2017-XLSX", "5153/June-2017-XLSX", "5440/July-2017-XLSX", "5441/August-2017-XLSX", "5477/September-2017-XLSX", "5548/October-2017-XLSX", "5608/November-2017-XLSX", "5607/December-2017-XLSX"]}'),
    ("https://mpdc.dc.gov/sites/default/files/dc/sites/mpdc/publication/attachments", 
    '[{"urls": ["New and Closed Lawsuits CY 2023 as of 7.20.2023.xlsx"], "sheets": ["Open YTD", "Closed YTD"]}, {"urls": ["New and Closed Lawsuits and Claims 2023 July-December External.xlsx"], "sheets": ["New Lawsuits", "Closed Lawsuits", "New Claims", "Closed Claims"]}]')
    ]
    )
def test_combined(url, dataset):
    dataset_dict = dataset_id.parse_id(dataset)
    exp_datasets = dataset_id.expand(dataset_dict)
    assert dataset_id.is_combined_dataset(exp_datasets)
    loader = data_loaders.CombinedDataset(data_loaders.Excel, url, exp_datasets, pbar=False)

    assert loader.isfile()

    dataset_dict_gt = json.loads(dataset)
    if isinstance(dataset_dict_gt, dict) and set(dataset_dict_gt.keys())=={'urls'}:
        exp_datasets_gt = [{'url':x} for x in dataset_dict_gt['urls']]
    elif isinstance(dataset_dict_gt, dict) and set(dataset_dict_gt.keys())=={'urls','sheets'}:
        exp_datasets_gt = [{'url':x, 'sheets':[dataset_dict_gt['sheets']]} for x in dataset_dict_gt['urls']]
    elif isinstance(dataset_dict_gt, list) and set(dataset_dict_gt[0].keys())=={'urls','sheets'} and len(dataset_dict_gt[0]['urls'])==1:
        exp_datasets_gt = [{'url':x['urls'][0], 'sheets':x['sheets']} for x in dataset_dict_gt]
    else:
        raise NotImplementedError()

    dfs = []
    for ds in exp_datasets_gt:
        headers = {'User-agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A'}
        if 'url' in ds and ds['url'].endswith('.csv'):
            dfcur = pd.read_csv(ds['url'])
            cols = [x for x in dfcur.columns if not x.startswith('Unnamed')]
            dfcur = dfcur[cols]
            dfs.append(dfcur)
        else:
            url_cur = url + '/' + ds['url'] if 'url' in ds else url
            r = requests.get(url_cur, stream=True, headers=headers)
            r.raise_for_status()
            file_like = BytesIO(r.content)
            sheets = ds['sheets'] if 'sheets' in ds else [0]
            for s in sheets:
                if isinstance(s, str):
                    s = s.strip()
                    if '*' in s:
                        all_sheets = pd.ExcelFile(file_like).sheet_names
                        p = s.replace('*','.*')
                        s = [x for x in all_sheets if re.search(p,x)]
                        assert len(s)==1
                        s = s[0]

                dfcur = pd.read_excel(file_like, sheet_name=s)

                cols = [x for x in dfcur.columns if not x.startswith('Unnamed')]
                dfcur = dfcur[cols]
                dfs.append(dfcur)
        
    df_true = pd.concat(dfs, ignore_index=True).convert_dtypes()

    count = loader.get_count(force=True)
    df = loader.load().convert_dtypes()

    assert len(df_true) == count
    
    pd.testing.assert_frame_equal(df, df_true)

    offset = 3000
    nrows = 20
    df = loader.load(offset=offset).convert_dtypes()
    pd.testing.assert_frame_equal(df, df_true.iloc[offset:].convert_dtypes())

    df = loader.load(nrows=nrows).convert_dtypes()
    pd.testing.assert_frame_equal(df, df_true.head(nrows).convert_dtypes())

    df = loader.load(offset=offset, nrows=nrows).convert_dtypes()
    pd.testing.assert_frame_equal(df, df_true.iloc[offset:].head(nrows).convert_dtypes())
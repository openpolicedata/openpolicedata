import os
import pandas as pd

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
import pytest

from openpolicedata import data
from datetime import datetime

import pathlib
import sys
sys.path.append(pathlib.Path(__file__).parent.resolve())
from test_utils import user_request_skip, check_load_for_datasets

@pytest.fixture(scope='module')
def excel_datasets(datasets, is_excel):
	return datasets[is_excel]

@pytest.fixture(scope='module')
def sheet_specified(excel_datasets):
	# Get datasets with sheetnames in dataset_id
	# TODO: Split into large and small datasets???
	def has_sheet(x):
		id  = x['dataset_id']
		tf = isinstance(id,list) or pd.notnull(id)
		if tf:
			is_zip = x['URL'].endswith('.zip')
			if is_zip:
				tf = not isinstance(id,str) and (isinstance(id,dict) and (len(id)>1 or 'files' not in id))
			else:
				if isinstance(id, str):
					tf = True
				elif isinstance(id, dict):
					tf = 'sheets' in id
				elif isinstance(id, list) and all(isinstance(x,dict) and 'sheets' in x for x in id):
					tf = True
				else:
					raise NotImplementedError()
		return tf
	return excel_datasets.apply(has_sheet, axis=1)

def test_all_used(sheet_specified):
	raise NotImplementedError('xor booleans')

def test_load_sheet_specified(excel_datasets, sheet_specified, source, start_idx, skip, query={}):
	def test(dataset, table):
		if pd.notnull(dataset['date_field']):
			assert table.table[dataset['date_field']].dt.year.min()>=dataset['coverage_start'].year
			max_year = table.table[dataset['date_field']].dt.year.max()
			assert 0<=dataset['coverage_end'].year - max_year<2

	check_load_for_datasets(excel_datasets[sheet_specified],  skip, start_idx, source, query, testfcn=test)
	


def test_load_has_year_sheets(excel_datasets, source, start_idx, skip, query={}):
	for i in range(len(excel_datasets)):
		if user_request_skip(excel_datasets, i, skip, start_idx, source, query) or not sheets[i]['has_year_sheets']:
			continue

		srcName = excel_datasets.iloc[i]["SourceName"]
		src = data.Source(srcName, state=excel_datasets.iloc[i]["State"], agency=excel_datasets.iloc[i]["Agency"])

		table_print = excel_datasets.iloc[i]["TableType"]
		now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
		print(f"{now} Testing {i+1} of {len(excel_datasets)}: {srcName} {table_print} table")

		excel = src._Source__get_loader(excel_datasets.iloc[i]["DataType"], excel_datasets.iloc[i]["URL"], excel_datasets.iloc[i]["query"],
				dataset_id=excel_datasets.iloc[i]["dataset_id"])
		sheets, has_year_sheets = excel._Excel__get_sheets()

		if has_year_sheets:
			# Ensure that load works
			src.load(excel_datasets.iloc[i]["TableType"], excel_datasets.iloc[i]["Year"], pbar=False)
		else:
			excel._Excel__check_sheet(sheets)

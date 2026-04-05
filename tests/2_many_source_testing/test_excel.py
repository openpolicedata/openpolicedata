import numpy as np
from urllib.parse import urlparse

import pandas as pd

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
import pytest

from openpolicedata import defs

import pathlib
import sys
sys.path.append(pathlib.Path(__file__).parent.resolve())
from test_utils import check_load_for_datasets, shuffle

@pytest.fixture(scope='module')
def excel_datasets(datasets, is_excel):
	return datasets[is_excel]


@pytest.fixture(scope='module')
def is_zip(excel_datasets):
	def is_zip_with_files(x):
		id  = x['dataset_id']
		return (isinstance(id,list) or pd.notnull(id)) and x['URL'].endswith('.zip') and \
			(isinstance(id,str) or (isinstance(id,dict) and (len(id)==1 and 'files' in id)))

	return excel_datasets.apply(is_zip_with_files, axis=1)


@pytest.fixture(scope='module')
def sheet_specified(excel_datasets, is_zip):
	# Get datasets with sheetnames in dataset_id
	# TODO: Split into large and small datasets???
	def has_sheet(x):
		id  = x['dataset_id']
		tf = isinstance(id,list) or pd.notnull(id)
		if tf:
			if isinstance(id, str):
				tf = True
			elif isinstance(id, dict):
				tf = 'sheets' in id
			elif isinstance(id, list) and all(isinstance(x,dict) and 'sheets' in x for x in id):
				tf = True
			else:
				raise NotImplementedError()
		return tf
	return (~is_zip) & excel_datasets.apply(has_sheet, axis=1)


@pytest.fixture(scope='module')
def sheet_unspecified(sheet_specified, is_zip):
	return (~sheet_specified) & (~is_zip)

def test_all_datasets_tested(sheet_specified, sheet_unspecified, is_zip):
	x = (sheet_specified + sheet_unspecified + is_zip)!=1
	numleft = x.sum()
	assert numleft==0, f'{numleft} datasets not used or used more than once'

def check_date(dataset, table, date):
		if pd.notnull(dataset['date_field']):
			assert table.table[dataset['date_field']].dt.year.min()>=dataset['coverage_start'].year
			max_year = table.table[dataset['date_field']].dt.year.max()
			# Skipping datasets that are known to have out-of-range date values
			if dataset['URL'] not in ['https://www.oaklandca.gov/files/assets/city/v/1/police-commission/police/documents/opd-policies-and-resources/uof/for-prrs-2022-use-of-force.xlsx']:
				if dataset['Year']==defs.MULTI:
					assert 0<=max_year-dataset['coverage_end'].year<=1
				else:
					assert dataset['coverage_end'].year==max_year

def test_load_sheet_specified(excel_datasets, sheet_specified, source, start_idx, skip, query={}):
	check_load_for_datasets(excel_datasets[sheet_specified],  skip, start_idx, source, query, testfcn=check_date)


def test_load_sheet_unspecified(excel_datasets, sheet_unspecified, source, start_idx, skip, query={}):
	check_load_for_datasets(excel_datasets[sheet_unspecified],  skip, start_idx, source, query, testfcn=check_date)


@pytest.mark.slow(reason="Loading zip file is slow to run and will be run last.")
def test_zip_load(excel_datasets, is_zip, source, start_idx, skip, query={}, num_zips_check=5):

	df_ori = pd.read_csv('https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2024-07/UseofForce_ORI-Agency_Names_2023.csv')
	
	datasets = excel_datasets[is_zip]
	datasets = datasets[~datasets['SourceName'].isin(skip)] if skip else datasets
	datasets = datasets[datasets.index>=start_idx]
	datasets = datasets[datasets['SourceName']==source] if source else datasets
	zip_netloc = datasets['URL'].apply(lambda x: urlparse(x).netloc)
	zip_sites = zip_netloc.unique()
	zip_2_run = []
	for k in zip_sites:
		idx = datasets[zip_netloc==k].index.tolist()
		shuffle(idx)
		idx = idx[:num_zips_check] if len(idx)>num_zips_check else idx
		zip_2_run.extend(idx)

	def test(dataset, table):
		check_date(dataset, table)
		
		if table.urls['source_url'] == 'https://openjustice.doj.ca.gov/data':
			ori = table.table['AGENCY_ORI'].unique()
			if table.agency==defs.MULTI:
				assert len(ori)>1
			else:
				assert len(ori)==1
				if ori[0]=='CA0349902':
					# This ORI is not found but is California Hwy Patrol
					return
				df_sel = df_ori.loc[df_ori['ORI']==ori[0],'AGENCY_NAME']
				assert len(df_sel)==1
				name = df_sel.iloc[0].replace('PD','Police Department')\
					.replace('SO','County Sheriff’s Department')\
					.replace('SD','County Sheriff’s Department')\
					.title()\
					.replace("’S","’s").replace('’',"'")
				if name.startswith('Csu'):
					assert 'California State University' in datasets.iloc[i]['AgencyFull']
				else:
					assert datasets.iloc[i]['AgencyFull'].replace('Office','Department').replace('’',"'")==name

	check_load_for_datasets(datasets.loc[zip_2_run],  skip, start_idx, source, query, testfcn=test)
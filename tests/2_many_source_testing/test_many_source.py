import pandas as pd
import pytest

import openpolicedata as opd
from tests.test_utils import check_for_dataset

def test_all_datasets_tested(is_excel, is_api):
    x = (is_excel+is_api)!=1
    numleft = x.sum()
    assert numleft==0, f'{numleft} datasets not used or used more than once'


def test_get_agencies_not_multi(datasets):
    datasets = datasets[datasets['Agency']!=opd.defs.MULTI]
    for i in range(len(datasets)):
        srcName = datasets.iloc[i]["SourceName"]
        state = datasets.iloc[i]["State"]
        table_type = datasets.iloc[i]['TableType']

        print(f"Testing {i+1} of {len(datasets)}: {srcName} {table_type} table")

        src = opd.data.Source(srcName, state=state, agency=datasets.iloc[i]["Agency"])

        # Handle cases where URL is required to disambiguate requested dataset
        ds_filter = src.filter(table_type, datasets.iloc[i]["Year"])
        url = datasets.iloc[i]['URL'] if len(ds_filter)>1 else None
        id = datasets.iloc[i]['dataset_id'] if len(ds_filter)>1 else None

        agency = src.get_agencies(table_type, year=datasets.iloc[i]["Year"], url=url, id=id)

        assert len(agency)==1
        assert agency[0]==datasets.iloc[i]["Agency"]

def test_get_agencies_file(datasets):
    source = 'Illinois'
    table = 'TRAFFIC STOPS'
    if not check_for_dataset(source, table):
        return
    
    src = opd.data.Source(source)
    with pytest.raises(ValueError):
        src.get_agencies(table)

multi_states = ['Connecticut','New York','Virginia']
multi_tables = ['TRAFFIC STOPS','TRAFFIC CITATIONS','STOPS']
multi_years = [opd.defs.MULTI,opd.defs.MULTI,opd.defs.MULTI]
multi_partial = ["Hartford",'Buffalo',"Arlington"]

def test_multi_agency_list(datasets):  # Ensure that multi datasets list above covers all possibilities for test_get_agencies_name_match
    for i in range(len(datasets)):
        if datasets.iloc[i]["DataType"] not in ['CSV','Excel'] and datasets.iloc[i]["Agency"] == opd.defs.MULTI:
            assert any([datasets.iloc[i]['State']==x and datasets.iloc[i]['TableType']==y and datasets.iloc[i]['Year']==z for 
                        x,y,z in zip(multi_states, multi_tables,multi_years)])
            

@pytest.mark.parametrize('state,table_type,year,partial',[(x,y,z,a) for x,y,z,a in zip(multi_states, multi_tables,multi_years,multi_partial)])
def test_get_agencies_name_match(datasets, remaining_datasets, state, table_type, year, partial):
    def dataset_not_found(x):
        # Return true if dataset matches any in dataframe
        return ((x['State']!=state) | (x['SourceName']!=state) | (x['TableType']!=table_type) | (x['Year']!=year)).all()
    
    # Check if requested dataset has already been processed (i.e. is in remaining_datasets)
    if dataset_not_found(remaining_datasets):
        # New York dataset processing does not test get_agencies. It will be tested if it is available to process (in datasets)
        if dataset_not_found(datasets) or (state, table_type, year) != ('New York', 'TRAFFIC CITATIONS', opd.defs.MULTI):
            return
        
    if check_for_dataset(state, table_type):
        src = opd.data.Source(state)
        try:
            agencies = src.get_agencies(partial_name=partial, table_type=table_type, year=year)
        except opd.exceptions.OPD_MinVersionError:
            return
        except Exception as e:
            raise
            
        assert len(agencies) > 1
        assert len(agencies)==len(set(agencies))
        assert all(partial.upper() in x.upper() for x in agencies)


def test_get_agencies_multi_file_force(remaining_datasets, start_idx):
    # TODO: Instead of loading every file, can verifying that the agency column is in each table be done more quickly?
    # TODO: Do not run all Stanford datasets
    datasets = remaining_datasets[remaining_datasets['Agency']==opd.defs.MULTI]
    datasets = datasets[datasets['DataType'].isin(['CSV','Excel'])]
    datasets = datasets.drop_duplicates(subset='URL')
    for i in range(max(0, start_idx-1), len(datasets)):
        srcName = datasets.iloc[i]["SourceName"]
        state = datasets.iloc[i]["State"]
        table_type = datasets.iloc[i]['TableType']

        print(f"Testing {i+1} of {len(datasets)}: {srcName} {table_type} table")

        src = opd.data.Source(srcName, state=state, agency=datasets.iloc[i]["Agency"])

        # Handle cases where URL is required to disambiguate requested dataset
        ds_filter = src.filter(table_type, datasets.iloc[i]["Year"])
        url = datasets.iloc[i]['URL'] if len(ds_filter)>1 else None
        id = datasets.iloc[i]['dataset_id'] if len(ds_filter)>1 else None

        agencies = src.get_agencies(table_type, year=datasets.iloc[i]["Year"], url=url, id=id, force=True)

        assert len(agencies)>1
        
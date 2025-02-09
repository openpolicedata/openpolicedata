import pandas as pd
import pytest

import openpolicedata as opd
from openpolicedata.dataset_id import parse_id, expand, is_combined_dataset, parse_excel_dataset

def test_parse_null():
    assert pd.isnull(parse_id(None))

def test_parse_str():
    string = 'test'
    assert string == parse_id(string)

def test_parse_json(req_csvfile):
    df = pd.read_csv(req_csvfile)

    ids = df['dataset_id'][df['dataset_id'].notnull()]
    ids = ids[ids.str.startswith('[') | ids.str.startswith('{')]

    assert all(isinstance(p:=parse_id(x), list) or isinstance(p, dict) for x in ids)

def test_expand_null():
    assert pd.isnull(expand(None))

def test_expand_str():
    string = 'test'
    assert string == expand(string)

def test_expand_json(all_datasets):
    ids = all_datasets['dataset_id'][all_datasets['dataset_id'].notnull()]
    ids = ids[ids.str.startswith('[') | ids.str.startswith('{')]

    assert all(isinstance(expand(x), list) for x in ids)

def test_expand_json_files():
    json = '{"files":["RIPA Stop Data 2018/RIPA Stop Data _ Los Angeles 2018 Q3.xlsx", "RIPA Stop Data 2018/RIPA Stop Data _ Los Angeles 2018 Q4.xlsx"]}'
    d = expand(parse_id(json))
    assert len(d)==2
    assert all(list(x.keys())==['file'] for x in d)
    assert all(isinstance(x['file'], str) for x in d)

@pytest.mark.parametrize('json', ['{"sheets": ["Open YTD", "Closed YTD"]}',
                                  '{"sheets": ["Open YTD"]}',
                                  '{"sheets": "Open YTD"}'])
def test_expand_json_sheets(json):
    d = expand(parse_id(json))
    assert len(d)==1
    assert set(d[0].keys())=={'sheets'}
    assert isinstance(d[0]['sheets'], list)
    assert len(d[0]['sheets'])==json.count(',')+1

@pytest.mark.parametrize('json', ['{"sheets": "Survey1", "files": "2022 UseOfForce_Step3_AgencyInformation.xlsx"}',
                                  '{"sheets": ["Survey1"], "files": "2022 UseOfForce_Step3_AgencyInformation.xlsx"}',
                                  '{"sheets": ["Survey1","Sheet2"], "files": "2022 UseOfForce_Step3_AgencyInformation.xlsx"}'])
def test_expand_json_sheets_file(json):
    d = expand(parse_id(json))
    assert len(d)==1
    assert all(set(x.keys())=={'sheets','file'} for x in d)
    assert all(isinstance(x['file'], str) for x in d)
    assert all(isinstance(x['sheets'], list) for x in d)
    assert all(len(x['sheets'])==json.count(',') for x in d)

@pytest.mark.parametrize('json', ['{"sheets": "Survey1", "files": ["2022 UseOfForce_Step3_AgencyInformation.xlsx","file2"]}',
                                  '{"sheets": ["Survey1"], "files": ["2022 UseOfForce_Step3_AgencyInformation.xlsx","file2"]}',
                                  '{"sheets": ["Survey1","Sheet2"], "files": ["2022 UseOfForce_Step3_AgencyInformation.xlsx","file2"]}'])
def test_expand_json_sheets_multiple_files(json):
    d = expand(parse_id(json))
    assert len(d)==2
    assert all(set(x.keys())=={'sheets','file'} for x in d)
    assert all(isinstance(x['file'], str) for x in d)
    assert all(isinstance(x['sheets'], list) for x in d)
    assert all(len(x['sheets'])==json.count(',')-1 for x in d)

def test_expand_json_list_sheets_multiple_files():
    json = '[{"sheets": "Survey1", "files": ["file1.xlsx","file2"]}, {"sheets": ["Survey1","Table2"], "files": "file4"}, {"files": "file5"}]'
    d = expand(parse_id(json))
    assert len(d)==4
    assert all(set(x.keys())=={'sheets','file'} for x in d[:-1])
    assert set(d[-1].keys())=={'file'}
    assert all(isinstance(x['file'], str) for x in d)
    assert all(isinstance(x['sheets'], list) for x in d[:-1])
    assert len(d[0]['sheets'])==1
    assert len(d[1]['sheets'])==1
    assert len(d[2]['sheets'])==2

def test_expand_json_urls():
    json = '{"urls": ["Q1-2023-Stop-Data-For-Website.xlsx", "Q2-2023-Stop-Data-Cleaned.xlsx", "Q3-2023-Stop-Data-for-Website.xlsx", "Q4-2023-Stop-Data-Cleaned.xlsx"]}'
    d = expand(parse_id(json))
    assert len(d)==4
    assert all(list(x.keys())==['url'] for x in d)
    assert all(isinstance(x['url'], str) for x in d)

@pytest.mark.parametrize('json', ['{"sheets": "Survey1", "urls": ["2022 UseOfForce_Step3_AgencyInformation.xlsx","file2"]}',
                                  '{"sheets": ["Survey1"], "urls": ["2022 UseOfForce_Step3_AgencyInformation.xlsx","file2"]}',
                                  '{"sheets": ["Survey1","Sheet2"], "urls": ["2022 UseOfForce_Step3_AgencyInformation.xlsx","file2"]}'])
def test_expand_json_sheets_multiple_urls(json):
    d = expand(parse_id(json))
    assert len(d)==2
    assert all(set(x.keys())=={'sheets','url'} for x in d)
    assert all(isinstance(x['url'], str) for x in d)
    assert all(isinstance(x['sheets'], list) for x in d)
    assert all(len(x['sheets'])==json.count(',')-1 for x in d)

def test_expand_json_list_sheets_multiple_urls():
    json = '[{"sheets": "Survey1", "urls": ["file1.xlsx","file2"]}, {"sheets": ["Survey1","Table2"], "urls": "file4"}, {"urls": "file5"}]'
    d = expand(parse_id(json))
    assert len(d)==4
    assert all(set(x.keys())=={'sheets','url'} for x in d[:-1])
    assert set(d[-1].keys())=={'url'}
    assert all(isinstance(x['url'], str) for x in d)
    assert all(isinstance(x['sheets'], list) for x in d[:-1])
    assert len(d[0]['sheets'])==1
    assert len(d[1]['sheets'])==1
    assert len(d[2]['sheets'])==2

@pytest.mark.parametrize('ds', [None, 'test', '{"sheets": "s1"}', '{"sheets": ["s1","s2"]}',
                                '{"files":"file1"}', '{"files":"file1", "sheets":["s1","s2"]}',
                                '[{"files":"file1"}, {"files":"file1", "sheets":["s1","s2"]}, {"files":["file1"], "sheets":"s3"}]'])
def test_is_combined_false(ds):
    d = expand(parse_id(ds))  
    assert not is_combined_dataset(d)

@pytest.mark.parametrize('ds', ['{"sheets": "s1", "urls":"url1"}', 
                                '{"sheets": "s1", "files":["file1","file2"]}', 
                                '[{"sheets": "s1"}, {"files":"file2", "sheets":["s1","s2"]}]',
                                '[{"files":"file1", "sheets": "s1"}, {"files":"file2", "sheets":["s1","s2"]}]',
                                '[{"sheets": "s1"}, {"urls":"url1", "sheets":["s1","s2"]}]'])
def test_is_combined_true(ds):
    d = expand(parse_id(ds))  
    assert is_combined_dataset(d)

@pytest.mark.parametrize('is_zip',[False, True])
def test_parse_excel_dataset_null(is_zip):
    sheet, file_in_zip = parse_excel_dataset(is_zip, None)
    assert pd.isnull(sheet)
    assert pd.isnull(file_in_zip)

@pytest.mark.parametrize('is_zip',[False, True])
def test_parse_excel_dataset_multiple_items_error(is_zip):
    with pytest.raises(AssertionError):
        parse_excel_dataset(is_zip, [[], []])


default_name = 'test'
@pytest.mark.parametrize('input',[default_name,
                                  {'sheets':default_name},
                                  {'sheets':[default_name]},
                                  [{'sheets':[default_name]}]])
def test_parse_excel_dataset_sheet(input):
    sheet, file_in_zip = parse_excel_dataset(False, input)
    assert len(sheet)==1
    assert sheet[0]==default_name
    assert pd.isnull(file_in_zip)

def test_parse_excel_dataset_multiple_sheets():
    input = {'sheets':['Sheet1','Sheet2']}
    sheet, file_in_zip = parse_excel_dataset(False, input)
    assert len(sheet)==2
    assert sheet==input['sheets']
    assert pd.isnull(file_in_zip)


@pytest.mark.parametrize('input',['"test"', '“test”'])
def test_parse_excel_dataset_sheet_quoted_string(input):
    sheet, file_in_zip = parse_excel_dataset(False, input)
    assert len(sheet)==1
    assert input[1:-1]==sheet[0]
    assert pd.isnull(file_in_zip)

def test_parse_excel_dataset_file_string():
    input = 'test'
    sheet, file_in_zip = parse_excel_dataset(True, input)
    assert input==file_in_zip
    assert pd.isnull(sheet)

@pytest.mark.parametrize('input',[default_name,
                                  {'file':default_name},
                                  [{'file':default_name}]])
def test_parse_excel_dataset_file(input):
    sheet, file_in_zip = parse_excel_dataset(True, input)

    assert file_in_zip==default_name
    assert pd.isnull(sheet)

@pytest.mark.parametrize('is_zip_file',[True, False])
@pytest.mark.parametrize('input',[{'file':default_name, 'sheets':'sheet1'},
                                  {'file':default_name, 'sheets':['sheet1']},
                                  {'file':default_name, 'sheets':['sheet1','sheet2']}])
def test_parse_excel_dataset_file_and_sheet(is_zip_file, input):
    sheet, file_in_zip = parse_excel_dataset(is_zip_file, input)

    assert file_in_zip==input['file']
    assert sheet==(input['sheets'] if isinstance(input['sheets'], list) else [input['sheets']])
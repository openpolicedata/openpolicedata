from datetime import datetime
import glob
import time
import pandas as pd
import os
import re
import shapely
import subprocess
import warnings
import urllib

import sys

import random
sys.path.append('../openpolicedata')
import openpolicedata as opd


# if the --use-changed-rows is specified in the test 
# make a table in the comments  with the columns --use-changed-rows and --csvfile with the true and false combinations for the rows
# | --use-changed-rows  | use_csvfile | Results                                                                                       |
# |---------------------|-------------|-----------------------------------------------------------------------------------------------|
# | True                | True        | Throw an error because it is ambiguous                                                        |
# | True                | False       | Use the added rows in the local ../opd-data/opd_source_table.csv that have not been committed |
# | False               | True        | Use the user specified csv file for the opd_source_table                                      |
# | False               | False       | Use default github opd_source_table.csv                                                       |
# 
def get_datasets(csvfile=None,use_changed_rows=False):
    csvfile = m if csvfile and os.path.exists((m:=csvfile.replace('\\','/'))) else csvfile

    if use_changed_rows:
        # Use the added rows in the local ../opd-data/opd_source_table.csv that have not been committed
        if csvfile:
            # Check if git repo
            cmd =f'git -C {os.path.dirname(csvfile)} remote -v'
            result = subprocess.run(cmd, stdout=subprocess.PIPE).stdout.decode('utf-8')
            if "opd-data.git" not in result:
                 raise FileNotFoundError(f"{csvfile} does not appear to be an Git repository for opd-data")
            csv_path = os.path.dirname(csvfile)
        else:
            csv_path = os.path.join('..','opd-data')
        assert os.path.exists(csv_path)
        added_lines_datasets = get_changed_rows(csv_path, 'opd_source_table.csv')
        opd.datasets.reload(added_lines_datasets)
    elif not use_changed_rows and csvfile:
        assert os.path.exists(csvfile)
        # Use the user specified csv file for the opd_source_table
        opd.datasets.reload(csvfile)

    return opd.datasets.query()


def get_outage_datasets(datasets):
    # Hard-coding location for now as being in opd-data folder in same folder as openpolicedata folder
    outages_file = os.path.join('..', 'opd-data', 'outages.csv')
    assert os.path.exists(outages_file)

    outages = pd.read_csv(outages_file)
    outages["Year"] = [int(x) if isinstance(x,str) and x.isdigit() else x for x in outages["Year"]]

    outage_cols = ["State","SourceName","Agency","TableType","Year",'AgencyFull','source_url','URL','dataset_id']
    matches = pd.Series(False, datasets.index)
    for _, row in outages[outage_cols].iterrows():
        x = ((row == datasets[outage_cols]) | (pd.isnull(row) & pd.isnull(datasets[outage_cols]))).all(axis=1)
        if not x.any():
            # URL may have changed
            new_cols = outage_cols.copy()
            new_cols.remove('URL')
            x = ((row[new_cols] == datasets[new_cols]) | (pd.isnull(row[new_cols]) & pd.isnull(datasets[new_cols]))).all(axis=1)
        assert x.any()
        matches = matches | x

    return datasets[matches]


def get_line_numbers(result):
    lines= result.split("\n")

    added_lines_file_number = []
    current_new_file_line_number = None
    for line in lines:
        # Detect hunk headers
        if line.startswith('@@'):
            # Extract new file line numbers
            _, new_file_range, _ = line.split(' ')[1:4]
            start_line = int(new_file_range.split(',')[0][1:])
            current_new_file_line_number = start_line - 1  # Adjust for 0-indexing and increment before use
        elif line.startswith('+') and not line.startswith('+++'):
            # Increment before adding because the line is part of the new file
            current_new_file_line_number += 1
            added_lines_file_number.append(current_new_file_line_number)
        elif line.startswith('-') or line.startswith(' '):
            # For unchanged or removed lines, only increment if it's not a removal from the new file
            if not line.startswith('-'):
                current_new_file_line_number += 1

    # convert the added_lines_file_number to dataframe indexes by subtracting 2 from each element and call the variable added_lines_dataframe_index
    added_lines_dataframe_index = [x-2 for x in added_lines_file_number]

    return added_lines_dataframe_index


# Function to get changed rows
def get_changed_rows(repo_dir, file_name):
    cmd = f"git -C {repo_dir} diff -- {file_name}"
    result = subprocess.check_output(cmd, shell=True).decode('utf-8')

    added_lines_dataframe_index = get_line_numbers(result)
    
    # extract the added lines from the csv file        
    csv_file = os.path.join(repo_dir, file_name)
       
    datasets = pd.read_csv(csv_file)
    
    #return only the datasets that are in the added_lines_dataframe_index
    added_lines_datasets = datasets.iloc[added_lines_dataframe_index]
    
    return added_lines_datasets


def shuffle(lst):
    rand_state = random.getstate()
    random.seed(datetime.now().month*31 + datetime.now().day) # Using a seed that varies but can be guessed if there is an error
    random.shuffle(lst)
    random.setstate(rand_state)


already_warned = [False]
def update_outages(outages_file, dataset, is_outage, e=None):
    if not os.path.exists(outages_file):
        if not already_warned[0]:
            warnings.warn(f'Outages file not found at {outages_file}')
            already_warned[0] = True
        return
    
    outages = pd.read_csv(outages_file)
    outages["Year"] = [int(x) if isinstance(x,str) and x.isdigit() else x for x in outages["Year"]]

    outage_cols = ["State","SourceName","Agency","TableType","Year",'AgencyFull','source_url','URL','dataset_id']
    match = ((outages[outage_cols] == dataset[outage_cols]) | (pd.isnull(outages[outage_cols]) & pd.isnull(dataset[outage_cols]))).all(axis=1)
    if is_outage:
        if match.any():
            outages.loc[match, 'Last Outage Confirmation'] = datetime.now().strftime('%Y-%m-%d')
        else:
            new_outage = dataset[outage_cols]
            new_outage['Error'] = str(e.args[1:])
            new_outage['Date Outage Started'] = datetime.now().strftime('%Y-%m-%d')
            new_outage['Last Outage Confirmation'] = new_outage['Date Outage Started']

            outages = pd.concat([outages, new_outage.to_frame().T])
    elif not match.any():
        new_cols = outage_cols.copy()
        new_cols.remove('URL')
        match = ((outages[new_cols] == dataset[new_cols]) | (pd.isnull(outages[new_cols]) & pd.isnull(dataset[new_cols]))).all(axis=1)
        if match.any():
             outages = outages.drop(index=outages.loc[match].index)
        else:
            return
    else:
        outages = outages.drop(index=outages.loc[match].index)

    outages.to_csv(outages_file, index=False)
    

def user_request_skip(datasets, i, skip, start_idx, source, query={}, skip_zip=False):
	# Skip sources that the user requested to skip
	if skip and datasets.iloc[i]["SourceName"] in skip:
		return True
	# User requested to start at start_idx
	if i<start_idx-1:
		return True
	if source != None and datasets.iloc[i]["SourceName"] != source:
		return True
	if skip_zip and datasets.iloc[i]["URL"].lower().endswith('.zip'):
		return True
	
	match = True
	for k,v in query.items():
		if datasets.iloc[i][k]!=v:
			match = False
			break
	
	return not match

def match_dataframes(df1, df2):
    df2 = df2.convert_dtypes()
    df1 = df1.convert_dtypes()

    for c in df1.columns:
        if df1[c].dtype != df2[c].dtype:
            df2[c] = df2[c].astype(df1[c].dtype)
        if df1[c].dtype=='object':
            # Ensure each value has same type
            app_vals = df2[c].tolist()
            true_vals = df1[c].tolist()
            for k in range(len(true_vals)):
                if type(true_vals[k]) != type(app_vals[k]):
                    # Convert to same type
                    if pd.isnull(app_vals[k]) and pd.isnull(true_vals[k]):
                        app_vals[k] = true_vals[k]
                    # elif isinstance(true_vals[k], dict) and isinstance(app_vals[k], str):
                    #     app_vals[k] = json.loads(app_vals[k].replace("'",'"'))
                    else:
                        app_vals[k] = type(true_vals[k])(app_vals[k])

            df2[c] = app_vals
            df2[c] = df2[c].astype(df1[c].dtype)

        if df1[c].dtype != df2[c].dtype:
            df2[c] = df2[c].astype(df1[c].dtype)

    return df1, df2

def pop_dataset(dataset):
	srcName = dataset["SourceName"]
	state = dataset["State"]
	table = dataset["TableType"]
	agency = dataset["Agency"]

	return srcName, state, table, agency

def rest_url(datasets, i, last_url, t=0.1):
	url = datasets.iloc[i]['URL']
	if urllib.urlparse(url).netloc == urllib.urlparse(last_url).netloc:
		# Adding a pause here to prevent issues with requesting from site too frequently
		time.sleep(t)

	return url

def is_file(datasets, i):
	return datasets.iloc[i]["DataType"] in ["CSV","Excel",'HTML']


def load_data(src, table_type, year, agency, nrows, url, id, caught_exceptions_warn):
	table = None
	try:
		table = src.load(table_type, year, 
						agency=agency, pbar=False, 
						nrows=nrows, 
						url=url, id=id)
	except (opd.exceptions.OPD_DataUnavailableError, opd.exceptions.OPD_SocrataHTTPError) as e:
		caught_exceptions_warn.append(e)
	except:
		raise

	return table

def check_result(df, gt, row, convert_to_date=True):
    assert len(gt)>0, 'Ground truth is empty. Unintentionally filtered for empty table'
    if convert_to_date and pd.notnull(row['date_field']):
        df[row['date_field']] = opd.datetime_parser.to_datetime(df[row['date_field']])

        if not isinstance(df[row['date_field']].dtype, pd.PeriodDtype):
            df[row['date_field']] = df[row['date_field']].dt.tz_localize(None)

    df = df.dropna(axis=1, how='all')
    gt = gt.dropna(axis=1, how='all')
    assert gt.shape==df.shape
    df = df[gt.columns]  #Ensure columns are in correct order

    df = df.apply(lambda x: x.apply(lambda y: str(y) if isinstance(y,dict) else y))
    gt = gt.apply(lambda x: x.apply(lambda y: str(y) if isinstance(y,dict) else y))

    df = df.sort_values(by=df.columns.tolist()).reset_index(drop=True)
    gt = gt.sort_values(by=df.columns.tolist()).reset_index(drop=True)

    if 'geometry' in df:  # Force NaN points to be equal
         df['geometry'] = df['geometry'].apply(lambda x: shapely.geometry.point.Point(-1000, -1000) if pd.notnull(x) and pd.isnull(x.x) else x)
         gt['geometry'] = gt['geometry'].apply(lambda x: shapely.geometry.point.Point(-1000, -1000) if pd.notnull(x) and pd.isnull(x.x) else x)

    # Sorting is done to account for rows that are sorted by date but where rows with same date are in different order
    pd.testing.assert_frame_equal(df, gt, check_dtype=False)
    time.sleep(0.1) # Just so we don't cause issues at the URL

def check_load_for_datasets(datasets, skip, start_idx, source, query, nrows=None, testfcn=None, datefcn=None):
    caught_exceptions = []
    caught_exceptions_warn = []
    base_sleep_time = 0.1
    outages_file = os.path.join("..","opd-data","outages.csv")
    warn_errors = (opd.exceptions.OPD_DataUnavailableError, opd.exceptions.OPD_SocrataHTTPError, opd.exceptions.OPD_FutureError)
    last_source = None
    for i in range(len(datasets)):
        if user_request_skip(datasets, i, skip, start_idx, source, query):
            continue
        
        srcName = datasets.iloc[i]["SourceName"]
        src = opd.Source(srcName, state=datasets.iloc[i]["State"], agency=datasets.iloc[i]["Agency"])
        
        table_type = datasets.iloc[i]["TableType"]
        now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
        print(f"{now} Testing {i+1} of {len(datasets)}: {srcName} {table_type} {datasets.iloc[i]['Year']} table")

		# Handle cases where URL is required to disambiguate requested dataset
        ds_filter = src.filter(table_type, datasets.iloc[i]["Year"])
        url = datasets.iloc[i]['URL'] if len(ds_filter)>1 else None
        id = datasets.iloc[i]['dataset_id'] if len(ds_filter)>1 else None

        date = datefcn(datasets.iloc[i], src, table_type) if datefcn else datasets.iloc[i]["Year"]

        try:
            table = src.load(table_type, date, pbar=False, nrows=nrows, 
					url=url, id=id)
        except opd.exceptions.OPD_MinVersionError as e:
            e.prepend(f"Iteration {i}", srcName, table_type, datasets.iloc[i]["Year"])
            caught_exceptions_warn.append(e)
            continue
        except warn_errors as e:
            e.prepend(f"Iteration {i}", srcName, table_type, datasets.iloc[i]["Year"])
            update_outages(outages_file, datasets.iloc[i], True, e)
            caught_exceptions_warn.append(e)
            continue
        except (opd.exceptions.OPD_TooManyRequestsError, opd.exceptions.OPD_arcgisAuthInfoError) as e:
			# Catch exceptions related to URLs not functioning
            e.prepend(f"Iteration {i}", srcName, table_type, datasets.iloc[i]["Year"])
            update_outages(outages_file, datasets.iloc[i], True, e)
            caught_exceptions.append(e)
            continue
        except:
            raise
        
        if len(table.table)==0: 
            update_outages(outages_file, datasets.iloc[i], True, ValueError('Table has 0 rows'))
            continue
        
        update_outages(outages_file, datasets.iloc[i], False)

        if pd.notnull(datasets.iloc[i]['query']):
            for k,v in opd.data_loaders.data_loader.str2json(datasets.iloc[i]['query']).items():
                assert (table.table[k]==v).all()
                
        if pd.notnull(datasets.iloc[i]['date_field']):
            assert datasets.iloc[i]['date_field'] in table.table
            
        if pd.notnull(datasets.iloc[i]['agency_field']):
            assert datasets.iloc[i]['agency_field'] in table.table

        if testfcn:
            testfcn(datasets.iloc[i], table, date)

        # Adding a pause here to prevent issues with requesting from site too frequently
        if last_source!=srcName:
            last_source = srcName
            sleep_time = base_sleep_time
        else:
            time.sleep(sleep_time)
            sleep_time+=base_sleep_time

    if len(caught_exceptions)==1:
        raise caught_exceptions[0]
    elif len(caught_exceptions)>0:
        msg = f"{len(caught_exceptions)} URL errors encountered:\n"
        for e in caught_exceptions:
            msg += "\t" + e.args[0] + "\n"
        raise opd.exceptions.OPD_MultipleErrors(msg)

    for e in caught_exceptions_warn:
        warnings.warn(str(e))

def get_remaining_datasets(datasets):
    # Parse .py test files to determine which datasets are thoroughly tested in 
    # dataset-specific tests and don't need to be retested in general tests
    files = []
    for f in ['1_unit_data_loaders', '1_unit_data_source_loading']:
        files.extend(glob.glob(os.path.join('tests', f, '**','*.py'), recursive=True))

    matches = []
    exclude = ['test_socrata_general.py', 'test_loader_arcgis.py']
    for f in files:
        if os.path.basename(f) in exclude:
             continue
        with open(f, 'r', encoding="utf8") as fid:
            for line in fid:
                m = re.search(r'^source\s*=\s*[\"\'](.+)[\'\"]', line)
                if m:
                    source = m.group(1)
                    year = None
                    url = None
                    for k in range(3):
                        line = fid.readline()
                        m = re.search(r'^table\s*=\s*(.+)\s*', line)
                        if m:
                            table = m.group(1)
                            if '.' in table:
                                e = table[table.rfind('.')+1:].strip()
                                table = getattr(opd.defs.TableType, e)
                            else:
                                 table[1:-1]
                            continue
                        m = re.search(r'^year\s*=\s*(\d+)\s*', line)
                        if m:
                            year = int(m.group(1)) if m.group(1).isdigit() else m.group(1)
                            continue
                        m = re.search(r'^url\s*=\s*[\"\'](.+)[\"\']\s*', line)
                        if m:
                            url = m.group(1)
                            continue
					
                    dmatch = (datasets['SourceName']==source) & (datasets['TableType']==table)
                    if dmatch.sum()!=1 and url:
                        dmatch &= datasets['URL'].str.contains(url)
                    if dmatch.sum()!=1 and year:
                        dmatch &= (datasets['Year']==year)
                             
                    if dmatch.sum()>1:
                        raise NotImplementedError()
                    elif dmatch.sum()==1:
                        matches.append(dmatch[dmatch].index[0])
                    break
            else:
                 raise NotImplementedError()

    return datasets.drop(index=matches)

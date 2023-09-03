# import pytest

# from copy import deepcopy
# from io import StringIO
# import logging
# import pandas as pd
# import os
# import random
# import time

# if __name__ == "__main__":
# 	import sys
# 	sys.path.append('../openpolicedata')
# from openpolicedata import datasets, data, preproc
# from openpolicedata import defs
# from openpolicedata._preproc_utils import DataMapping
# from openpolicedata.exceptions import BadCategoryDict

# date_col = "incident_date"
# agency_col = "agency_name"
# race_col = "race"
# eth_col = "ethnicity"
# age_col = "age"
# gender_col = "gender"
# role_col = "role"
# age_group_col = "age_group"
# time_col = "time"
# bad_data = "BAD DATA"
# bad_time = "25:71"

# @pytest.fixture
# def table():
#     random.seed(0)
#     rows = 1000
#     time_format = '%Y-%m-%d'
#     stime = time.mktime(time.strptime("2022-01-01", time_format))
#     etime = time.mktime(time.strptime("2022-12-31", time_format))
#     races = ['BLACK OR AFRICAN AMERICAN', 'WHITE', 'UNKNOWN',
#        'ASIAN OR NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER',
#        'AMERICAN INDIAN OR ALASKA NATIVE']
#     eth = ['NOT HISPANIC OR LATINO', 'HISPANIC OR LATINO', 'UNKNOWN']
#     df = pd.DataFrame(
#         {
#             date_col: [time.strftime(time_format, time.localtime(stime + random.random() * (etime - stime))) for _ in range(rows)],
#             time_col: [time.strftime("%H:%M", time.localtime(stime + random.random() * (etime - stime))) if k>4 else bad_time for k in range(rows)],
#             race_col: [random.choice(races) for _ in range(rows)],
#             eth_col: [random.choice(eth) for _ in range(rows)],
#             gender_col: [random.choice(["MALE","FEMALE"]) for _ in range(rows)],
#             agency_col: [random.choice(["ABC Police Department","DEF Police Department","GHI Police Department"]) for _ in range(rows)],
#             age_col: [random.randint(15, 99) for _ in range(rows)],
#             age_group_col: [random.choice(["15-25", "26-35", '36-80']) for _ in range(rows)]
#         }
#     )

#     source = pd.Series(
#         {
#             "State":"Virginia",
#             "SourceName": "Virginia",
#             "TableType":"STOPS",
#             "Description":"",
#             "URL":"",
#             "DataType":"CSV",
#             "dataset_id":"",
#             "date_field":date_col,
#             "agency_field":agency_col,
#             "source_url":"",
#             "readme":""
#         }
#     )

#     table = data.Table(source, df, 2022, "MULTI")

#     return table

# @pytest.fixture
# def table_w_role(table):
#     new_table = deepcopy(table)
#     new_table.table[role_col] = [random.choice(["OFFICER", "Subject"]) for _ in range(len(table.table))]
#     new_table.table_type = defs.TableType.SHOOTINGS
#     return new_table

# @pytest.fixture
# def std_table(table):
#     new_table = deepcopy(table)
#     new_table.standardize()
#     return new_table

# @pytest.fixture
# def std_table_w_role(table_w_role):
#     new_table = deepcopy(table_w_role)
#     new_table.standardize()
#     return new_table


# @pytest.fixture
# def log_filename():
#     filename = "test.log"
#     yield filename
#     if os.path.exists(filename):
#         os.remove(filename)


# def test_no_transform_map(csvfile, source, last, skip, loghtml, table):
#     assert table.get_transform_map() is None


# def test_transform_map(csvfile, source, last, skip, loghtml, std_table):
#     assert isinstance(std_table.get_transform_map(), list)
#     assert isinstance(std_table.get_transform_map()[0], DataMapping)


# @pytest.mark.parametrize("col", [defs.columns.DATE, defs.columns.TIME, defs.columns.DATETIME,
#                                  defs.columns.RACE_ONLY_SUBJECT, defs.columns.ETHNICITY_SUBJECT,
#                                  defs.columns.RACE_SUBJECT, defs.columns.AGE_SUBJECT,
#                                  defs.columns.AGE_RANGE_SUBJECT, defs.columns.GENDER_SUBJECT,
#                                  defs.columns.AGENCY])
# def test(csvfile, source, last, skip, loghtml, std_table, col):
#     assert col in std_table.table


# @pytest.mark.parametrize("col", [defs.columns.RACE_ONLY_OFFICER_SUBJECT, defs.columns.ETHNICITY_OFFICER_SUBJECT,
#                                  defs.columns.RACE_OFFICER_SUBJECT, defs.columns.AGE_OFFICER_SUBJECT,
#                                  defs.columns.AGE_RANGE_OFFICER_SUBJECT, defs.columns.GENDER_OFFICER_SUBJECT,
#                                  defs.columns.SUBJECT_OR_OFFICER])
# def test_w_role(csvfile, source, last, skip, loghtml, std_table_w_role, col):    
#     assert col in std_table_w_role.table


# def test_get_race_cats_expand(csvfile, source, last, skip, loghtml):
#     assert defs.get_race_cats() != defs.get_race_cats(expand=True)
    

# def test_race_cats(csvfile, source, last, skip, loghtml, table, std_table):
#     race_cats = defs.get_race_cats()
#     keys = defs.get_race_keys()
#     white = race_cats[keys.WHITE]
#     race_cats[keys.WHITE] = "TEST"
#     table.standardize(race_cats=race_cats)

#     orig = std_table.table[defs.columns.RACE_ONLY_SUBJECT]
#     renamed = table.table[defs.columns.RACE_ONLY_SUBJECT]

#     assert ((orig==white) == (renamed=="TEST")).all()


# def test_race_cats_bad_input(csvfile, source, last, skip, loghtml, table):
#     cats = defs.get_race_cats()
#     cats["BAD"] = 'ERROR'
#     with pytest.raises(BadCategoryDict):
#         table.standardize(race_cats=cats)

# def test_eth_cats_bad_input(csvfile, source, last, skip, loghtml, table):
#     cats = defs.get_eth_cats()
#     cats["BAD"] = 'ERROR'
#     with pytest.raises(BadCategoryDict):
#         table.standardize(eth_cats=cats)


# def test_gender_cats_bad_input(csvfile, source, last, skip, loghtml, table):
#     cats = defs.get_gender_cats()
#     cats["BAD"] = 'ERROR'
#     with pytest.raises(BadCategoryDict):
#         table.standardize(gender_cats=cats)


# def test_eth_cats(csvfile, source, last, skip, loghtml, table, std_table):
#     eth_cats = defs.get_eth_cats()
#     keys = defs.get_race_keys()
#     latino = eth_cats[keys.LATINO]
#     eth_cats[keys.LATINO] = "TEST"
#     table.standardize(eth_cats=eth_cats)

#     orig = std_table.table[defs.columns.ETHNICITY_SUBJECT]
#     renamed = table.table[defs.columns.ETHNICITY_SUBJECT]

#     assert ((orig==latino) == (renamed=="TEST")).all()


# def test_gender_cats(csvfile, source, last, skip, loghtml, table, std_table):
#     cats = defs.get_gender_cats()
#     keys = defs.get_gender_keys()
#     orig_label = cats[keys.MALE]
#     cats[keys.MALE] = "TEST"
#     table.standardize(gender_cats=cats)

#     orig = std_table.table[defs.columns.GENDER_SUBJECT]
#     renamed = table.table[defs.columns.GENDER_SUBJECT]

#     assert ((orig==orig_label) == (renamed=="TEST")).all()

# @pytest.mark.parametrize("old_column, new_column",
#                          [(race_col, defs.columns.RACE_ONLY_OFFICER_SUBJECT),
#                           (gender_col, defs.columns.GENDER_OFFICER_SUBJECT),
#                           (eth_col, defs.columns.ETHNICITY_OFFICER_SUBJECT),
#                           (role_col, defs.columns.SUBJECT_OR_OFFICER),
#                           (age_group_col, defs.columns.AGE_RANGE_OFFICER_SUBJECT)
#                           ])
# def test_no_id_keep(csvfile, source, last, skip, loghtml, table_w_role, old_column, new_column):
#     orig_label = bad_data
#     table_w_role.table.loc[:5, old_column] = orig_label

#     table_w_role.standardize()

#     assert (table_w_role.table.loc[:5, new_column] == orig_label).all()

# @pytest.mark.parametrize("old_column, new_column",
#                          [(race_col, defs.columns.RACE_ONLY_OFFICER_SUBJECT),
#                           (gender_col, defs.columns.GENDER_OFFICER_SUBJECT),
#                           (eth_col, defs.columns.ETHNICITY_OFFICER_SUBJECT),
#                           (role_col, defs.columns.SUBJECT_OR_OFFICER),
#                           (age_group_col, defs.columns.AGE_RANGE_OFFICER_SUBJECT)
#                           ])
# def test_no_id_null(csvfile, source, last, skip, loghtml, table_w_role, old_column, new_column):
#     orig_label = bad_data
#     table_w_role.table.loc[:5, old_column] = orig_label

#     table_w_role.standardize(no_id='null')

#     assert (table_w_role.table.loc[:5, new_column] == "").all()


# @pytest.mark.parametrize("old_column", [race_col, gender_col, eth_col, role_col, age_group_col])
# def test_no_id_error(csvfile, source, last, skip, loghtml, table_w_role, old_column):
#     orig_label = bad_data
#     table_w_role.table.loc[:5, old_column] = orig_label

#     with pytest.raises(ValueError, match="Unknown"):
#         table_w_role.standardize(no_id="error")


# def test_agg_cat(csvfile, source, last, skip, loghtml, table, std_table):
#     orig_label = "East African"
#     table.table.loc[:5, race_col] = orig_label

#     table.standardize(agg_race_cat=True)

#     assert (table.table.loc[:5, defs.columns.RACE_ONLY_SUBJECT] == defs.get_race_cats()[defs.get_race_keys().BLACK]).all()

# def test_keep_raw(csvfile, source, last, skip, loghtml, std_table):
#     assert any([x.startswith(preproc._OLD_COLUMN_INDICATOR+"_") for x in std_table.table.columns])


# def test_not_keep_raw(csvfile, source, last, skip, loghtml, table):
#     table.standardize(keep_raw=False)
#     assert not any([x.startswith(preproc._OLD_COLUMN_INDICATOR+"_") for x in table.table.columns])

# @pytest.mark.parametrize("column", [defs.columns.DATE, defs.columns.RACE_OFFICER])
# def test_known_col_not_in_table(csvfile, source, last, skip, loghtml, table, column):
#     with pytest.raises(ValueError, match="Known column .+ is not in the DataFrame"):
#         table.standardize(known_cols={column:"TEST"})

# def test_known_col_bad_key(csvfile, source, last, skip, loghtml, table):
#     with pytest.raises(BadCategoryDict):
#         table.standardize(known_cols={"BAD":"TEST"})

# @pytest.mark.parametrize("old_column, new_column", 
#                          [(date_col, defs.columns.DATE),
#                           (race_col, defs.columns.RACE_SUBJECT),
#                           (race_col, defs.columns.RACE_OFFICER),
#                           (race_col, defs.columns.RACE_OFFICER_SUBJECT),
#                           (age_col, defs.columns.AGE_OFFICER),
#                           (age_col, defs.columns.AGE_OFFICER_SUBJECT),
#                           (age_col, defs.columns.AGE_SUBJECT),
#                           (gender_col, defs.columns.GENDER_OFFICER),
#                           (gender_col, defs.columns.GENDER_OFFICER_SUBJECT),
#                           (gender_col, defs.columns.GENDER_SUBJECT),
#                           (agency_col, defs.columns.AGENCY),
#                           (eth_col, defs.columns.ETHNICITY_OFFICER),
#                           (eth_col, defs.columns.ETHNICITY_OFFICER_SUBJECT),
#                           (eth_col, defs.columns.ETHNICITY_SUBJECT),
#                           (age_group_col, defs.columns.AGE_RANGE_OFFICER),
#                           (age_group_col, defs.columns.AGE_RANGE_OFFICER_SUBJECT),
#                           (age_group_col, defs.columns.AGE_RANGE_SUBJECT),
#                           (time_col, defs.columns.TIME)
#                          ]
#                         )
# def test_known_col(csvfile, source, last, skip, loghtml, table, old_column, new_column):
#     assert old_column in table.table
#     table.table["TEST"] = table.table[old_column]
#     table.standardize(known_cols={new_column:"TEST"})
#     assert new_column in table.table
#     assert "RAW_TEST" in table.table

# def test_known_col_role(csvfile, source, last, skip, loghtml, table_w_role):
#     assert role_col in table_w_role.table
#     table_w_role.table["TEST"] = table_w_role.table[role_col]
#     table_w_role.standardize(known_cols={defs.columns.SUBJECT_OR_OFFICER:"TEST"})
#     assert defs.columns.SUBJECT_OR_OFFICER in table_w_role.table
#     assert "RAW_TEST" in table_w_role.table

# def test_known_col_exists_multiple(csvfile, source, last, skip, loghtml, table):
#     assert race_col in table.table
#     table.table["TEST1"] = table.table[race_col].copy()
#     table.table["TEST2"] = table.table[race_col].copy()
#     table.standardize(known_cols={defs.columns.RACE_OFFICER:"TEST1",defs.columns.RACE_SUBJECT:"TEST2"})
#     assert defs.columns.RACE_OFFICER in table.table
#     assert "RAW_TEST1" in table.table
#     assert defs.columns.RACE_SUBJECT in table.table
#     assert "RAW_TEST2" in table.table

# def test_not_verbose(csvfile, source, last, skip, loghtml, table):
#     # Capture output to ensure that it's printed
#     logger = logging.getLogger("opd-std")
#     log_stream = StringIO()
#     sh = logging.StreamHandler(log_stream)
#     logger.addHandler(sh)
#     try:
#         table.standardize()
#     except:
#         raise
#     finally:
#         logger.removeHandler(sh)

#     assert len(log_stream.getvalue()) == 0

# def test_verbose(csvfile, source, last, skip, loghtml, table):
#     # Capture output to ensure that it's printed
#     logger = logging.getLogger("opd-std")
#     log_stream = StringIO()
#     sh = logging.StreamHandler(log_stream)
#     logger.addHandler(sh)
#     try:
#         table.standardize(verbose=True)
#     except:
#         raise
#     finally:
#         logger.removeHandler(sh)

#     assert len(log_stream.getvalue()) > 0

# def test_verbose_to_file(csvfile, source, last, skip, loghtml, table, log_filename):
#     table.standardize(verbose=log_filename)

#     assert os.path.exists(log_filename)
#     assert os.path.getsize(log_filename) > 100
#     # Ensure that file handler was removed
#     assert len(logging.getLogger("opd-std").handlers)==1

# def test_verbose_to_file_cleanup_with_error(csvfile, source, last, skip, loghtml, table, log_filename):
#     table.table.loc[:5, gender_col] = "TEST"

#     with pytest.raises(ValueError, match="Unknown"):
#         table.standardize(no_id="error", verbose=log_filename)

#     assert os.path.exists(log_filename)
#     assert os.path.getsize(log_filename) > 100
#     # Ensure that file handler was removed
#     assert len(logging.getLogger("opd-std").handlers)==1


# def test_race_eth_combo_merge(csvfile, source, last, skip, loghtml, std_table):
#     assert defs.columns.RACE_ONLY_SUBJECT in std_table.table
#     assert defs.columns.RACE_SUBJECT in std_table.table
#     assert all([isinstance(x,str) for x in std_table.table[defs.columns.RACE_SUBJECT].unique()])

# def test_race_eth_combo_concat(csvfile, source, last, skip, loghtml, table):
#     table.standardize(race_eth_combo="concat")

#     r = defs.get_race_cats()
#     e = defs.get_eth_cats()

#     assert defs.columns.RACE_ONLY_SUBJECT in table.table
#     assert defs.columns.RACE_SUBJECT in table.table
#     assert f"{r[defs.get_race_keys().BLACK]} {e[defs.get_eth_keys().LATINO]}" in table.table[defs.columns.RACE_SUBJECT].unique()


# def test_race_eth_combo_false(csvfile, source, last, skip, loghtml, table):
#     table.standardize(race_eth_combo=False)
#     assert defs.columns.RACE_ONLY_SUBJECT not in table.table

# def test_merge_datetime_true(csvfile, source, last, skip, loghtml, std_table):
#     assert defs.columns.DATETIME in std_table.table

# def test_merge_datetime_false(csvfile, source, last, skip, loghtml, table):
#     table.standardize(merge_date_time=False)
#     assert defs.columns.DATETIME not in table.table

# def test_empty_time_nat(csvfile, source, last, skip, loghtml, table, std_table):
#     idx = table.table[time_col]==bad_time
#     assert idx.sum()>0
#     assert defs.columns.DATETIME in std_table.table
#     assert std_table.table[defs.columns.DATETIME][idx].isnull().all()

# def test_empty_time_ignore(csvfile, source, last, skip, loghtml, table):
#     idx = table.table[time_col]==bad_time
#     table.standardize(empty_time='ignore')
#     assert idx.sum()>0
#     assert defs.columns.DATETIME in table.table
#     assert (table.table[defs.columns.DATETIME][idx]==table.table[defs.columns.DATE][idx]).all()

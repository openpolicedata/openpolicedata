import pytest

from copy import deepcopy
from io import StringIO
import logging
import pandas as pd
import os
import random
import time

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import data, preproc
from openpolicedata import defs
from openpolicedata import Column, TableType
from openpolicedata._preproc_utils import DataMapping
from openpolicedata.exceptions import BadCategoryDict

# TODO: Add tests to ensure that _converters produce consistent outputs

date_col = "incident_date"
agency_col = "agency_name"
race_col = "race"
eth_col = "ethnicity"
age_col = "age"
gender_col = "gender"
role_col = "role"
age_group_col = "age_group"
time_col = "time"
bad_data = "BAD DATA"
bad_time = "25:71"

def standardize(t, *args, **kwargs):
    # Do copy so fixtures are not changed
    t = deepcopy(t)
    t.standardize(*args, **kwargs)
    return t

@pytest.fixture(scope="module")
def table():
    random.seed(0)
    rows = 1000
    time_format = '%Y-%m-%d'
    stime = time.mktime(time.strptime("2022-01-01", time_format))
    etime = time.mktime(time.strptime("2022-12-31", time_format))
    races = ['BLACK OR AFRICAN AMERICAN', 'WHITE', 'UNKNOWN',
       'ASIAN OR NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER',
       'AMERICAN INDIAN OR ALASKA NATIVE']
    eth = ['NOT HISPANIC OR LATINO', 'HISPANIC OR LATINO', 'UNKNOWN']
    df = pd.DataFrame(
        {
            date_col: [time.strftime(time_format, time.localtime(stime + random.random() * (etime - stime))) for _ in range(rows)],
            time_col: [time.strftime("%H:%M", time.localtime(stime + random.random() * (etime - stime))) if k>4 else bad_time for k in range(rows)],
            race_col: [random.choice(races) for _ in range(rows)],
            eth_col: [random.choice(eth) for _ in range(rows)],
            gender_col: [random.choice(["MALE","FEMALE"]) for _ in range(rows)],
            agency_col: [random.choice(["ABC Police Department","DEF Police Department","GHI Police Department"]) for _ in range(rows)],
            age_col: [random.randint(15, 99) for _ in range(rows)],
            age_group_col: [random.choice(["15-25", "26-35", '36-80']) for _ in range(rows)]
        }
    )

    source = pd.Series(
        {
            "State":"Virginia",
            "SourceName": "Virginia",
            "TableType":"STOPS",
            "Description":"",
            "URL":"",
            "DataType":"CSV",
            "dataset_id":"",
            "date_field":date_col,
            "agency_field":agency_col,
            "source_url":"",
            "readme":""
        }
    )

    table = data.Table(source, df, 2022, "MULTI")

    return table

@pytest.fixture(scope="module")
def table_w_role(table):
    new_table = deepcopy(table)
    new_table.table[role_col] = [random.choice(["OFFICER", "Subject"]) for _ in range(len(table.table))]
    new_table.table_type = TableType.SHOOTINGS
    return new_table

@pytest.fixture(scope="module")
def std_table(table):
    new_table = deepcopy(table)
    new_table.standardize()
    return new_table

@pytest.fixture(scope="module")
def std_table_w_role(table_w_role):
    new_table = deepcopy(table_w_role)
    new_table.standardize()
    return new_table


@pytest.fixture(scope="module")
def log_filename():
    filename = "test.log"
    yield filename
    if os.path.exists(filename):
        os.remove(filename)


def test_no_transform_map(table):
    assert table.get_transform_map() is None


def test_transform_map(std_table):
    assert isinstance(std_table.get_transform_map(), list)
    assert isinstance(std_table.get_transform_map()[0], DataMapping)


@pytest.mark.parametrize("col", [Column.DATE, Column.TIME, Column.DATETIME,
                                 Column.RACE_ETHNICITY_SUBJECT, Column.ETHNICITY_SUBJECT,
                                 Column.RACE_SUBJECT, Column.RE_GROUP_SUBJECT, Column.AGE_SUBJECT,
                                 Column.AGE_RANGE_SUBJECT, Column.GENDER_SUBJECT,
                                 Column.AGENCY])
def test_col_in_table(std_table, col):
    assert col in std_table.table


@pytest.mark.parametrize("col", [Column.RACE_ETHNICITY_OFFICER_SUBJECT, Column.ETHNICITY_OFFICER_SUBJECT,
                                 Column.RACE_OFFICER_SUBJECT, Column.RE_GROUP_OFFICER_SUBJECT, Column.AGE_OFFICER_SUBJECT,
                                 Column.AGE_RANGE_OFFICER_SUBJECT, Column.GENDER_OFFICER_SUBJECT,
                                 Column.SUBJECT_OR_OFFICER])
def test_col_in_table_w_role(std_table_w_role, col):    
    assert col in std_table_w_role.table


def test_get_race_cats_expand():
    assert defs.get_race_cats() != defs.get_race_cats(expand=True)
    

def test_race_cats(table, std_table):
    race_cats = defs.get_race_cats()
    keys = defs.get_race_keys()
    white = race_cats[keys.WHITE]
    race_cats[keys.WHITE] = "TEST"
    table = standardize(table, race_cats=race_cats)

    orig = std_table.table[Column.RACE_SUBJECT]
    renamed = table.table[Column.RACE_SUBJECT]

    assert ((orig==white) == (renamed=="TEST")).all()


def test_race_cats_bad_input(table):
    cats = defs.get_race_cats()
    cats["BAD"] = 'ERROR'
    with pytest.raises(BadCategoryDict):
        table = standardize(table, race_cats=cats)

def test_eth_cats_bad_input(table):
    cats = defs.get_eth_cats()
    cats["BAD"] = 'ERROR'
    with pytest.raises(BadCategoryDict):
        table = standardize(table, eth_cats=cats)


def test_gender_cats_bad_input(table):
    cats = defs.get_gender_cats()
    cats["BAD"] = 'ERROR'
    with pytest.raises(BadCategoryDict):
        table = standardize(table, gender_cats=cats)


def test_eth_cats(table, std_table):
    eth_cats = defs.get_eth_cats()
    keys = defs.get_race_keys()
    latino = eth_cats[keys.LATINO]
    eth_cats[keys.LATINO] = "TEST"
    table = standardize(table, eth_cats=eth_cats)

    orig = std_table.table[Column.ETHNICITY_SUBJECT]
    renamed = table.table[Column.ETHNICITY_SUBJECT]

    assert ((orig==latino) == (renamed=="TEST")).all()


def test_gender_cats(table, std_table):
    cats = defs.get_gender_cats()
    keys = defs.get_gender_keys()
    orig_label = cats[keys.MALE]
    cats[keys.MALE] = "TEST"
    table = standardize(table, gender_cats=cats)

    orig = std_table.table[Column.GENDER_SUBJECT]
    renamed = table.table[Column.GENDER_SUBJECT]

    assert ((orig==orig_label) == (renamed=="TEST")).all()

@pytest.mark.parametrize("old_column, new_column",
                         [(race_col, Column.RACE_OFFICER_SUBJECT),
                          (gender_col, Column.GENDER_OFFICER_SUBJECT),
                          (eth_col, Column.ETHNICITY_OFFICER_SUBJECT),
                          (role_col, Column.SUBJECT_OR_OFFICER),
                          (age_group_col, Column.AGE_RANGE_OFFICER_SUBJECT)
                          ])
def test_no_id_keep(table_w_role, old_column, new_column):
    orig_label = bad_data
    table_w_role.table.loc[:5, old_column] = orig_label

    table_w_role = standardize(table_w_role)

    assert (table_w_role.table.loc[:5, new_column] == orig_label).all()

@pytest.mark.parametrize("old_column, new_column",
                         [(race_col, Column.RACE_OFFICER_SUBJECT),
                          (gender_col, Column.GENDER_OFFICER_SUBJECT),
                          (eth_col, Column.ETHNICITY_OFFICER_SUBJECT),
                          (role_col, Column.SUBJECT_OR_OFFICER),
                          (age_group_col, Column.AGE_RANGE_OFFICER_SUBJECT)
                          ])
def test_no_id_null(table_w_role, old_column, new_column):
    orig_label = bad_data
    table_w_role.table.loc[:5, old_column] = orig_label

    table_w_role = standardize(table_w_role, no_id='null')

    assert (table_w_role.table.loc[:5, new_column] == "").all()


@pytest.mark.parametrize("old_column", [race_col, gender_col, eth_col, role_col, age_group_col])
def test_no_id_error(table_w_role, old_column):
    orig_label = bad_data
    table_w_role.table.loc[:5, old_column] = orig_label

    with pytest.raises(ValueError, match="Unknown"):
        table_w_role = standardize(table_w_role, no_id="error")


def test_agg_cat(table, std_table):
    orig_label = "East African"
    table.table.loc[:5, race_col] = orig_label

    table = standardize(table, agg_race_cat=True)

    assert (table.table.loc[:5, Column.RACE_SUBJECT] == defs.get_race_cats()[defs.get_race_keys().BLACK]).all()

def test_keep_raw(std_table):
    assert any([x.startswith(preproc._OLD_COLUMN_INDICATOR+"_") for x in std_table.table.columns])


def test_not_keep_raw(table):
    table = standardize(table, keep_raw=False)
    assert not any([x.startswith(preproc._OLD_COLUMN_INDICATOR+"_") for x in table.table.columns])

@pytest.mark.parametrize("column", [Column.DATE, Column.RACE_OFFICER])
def test_known_col_not_in_table(table, column):
    with pytest.raises(ValueError, match="Known column .+ is not in the DataFrame"):
        table = standardize(table, known_cols={column:"TEST"})

def test_known_col_bad_key(table):
    with pytest.raises(BadCategoryDict):
        table = standardize(table, known_cols={"BAD":"TEST"})

@pytest.mark.parametrize("old_column, new_column", 
                         [(date_col, Column.DATE),
                          (race_col, Column.RACE_SUBJECT),
                          (race_col, Column.RACE_OFFICER),
                          (race_col, Column.RACE_OFFICER_SUBJECT),
                          (age_col, Column.AGE_OFFICER),
                          (age_col, Column.AGE_OFFICER_SUBJECT),
                          (age_col, Column.AGE_SUBJECT),
                          (gender_col, Column.GENDER_OFFICER),
                          (gender_col, Column.GENDER_OFFICER_SUBJECT),
                          (gender_col, Column.GENDER_SUBJECT),
                          (agency_col, Column.AGENCY),
                          (eth_col, Column.ETHNICITY_OFFICER),
                          (eth_col, Column.ETHNICITY_OFFICER_SUBJECT),
                          (eth_col, Column.ETHNICITY_SUBJECT),
                          (age_group_col, Column.AGE_RANGE_OFFICER),
                          (age_group_col, Column.AGE_RANGE_OFFICER_SUBJECT),
                          (age_group_col, Column.AGE_RANGE_SUBJECT),
                          (time_col, Column.TIME)
                         ]
                        )
def test_known_col(table, old_column, new_column):
    assert old_column in table.table
    table.table["TEST"] = table.table[old_column]
    table = standardize(table, known_cols={new_column:"TEST"})
    assert new_column in table.table
    assert "RAW_TEST" in table.table

def test_known_col_role(table_w_role):
    assert role_col in table_w_role.table
    table_w_role.table["TEST"] = table_w_role.table[role_col]
    table_w_role = standardize(table_w_role, known_cols={Column.SUBJECT_OR_OFFICER:"TEST"})
    assert Column.SUBJECT_OR_OFFICER in table_w_role.table
    assert "RAW_TEST" in table_w_role.table

def test_known_col_exists_multiple(table):
    assert race_col in table.table
    table.table["TEST1"] = table.table[race_col].copy()
    table.table["TEST2"] = table.table[race_col].copy()
    table = standardize(table, known_cols={Column.RACE_OFFICER:"TEST1",Column.RACE_SUBJECT:"TEST2"})
    assert Column.RACE_OFFICER in table.table
    assert "RAW_TEST1" in table.table
    assert Column.RACE_SUBJECT in table.table
    assert "RAW_TEST2" in table.table

def test_not_verbose(table):
    # Capture output to ensure that it's printed
    logger = logging.getLogger("opd-std")
    log_stream = StringIO()
    sh = logging.StreamHandler(log_stream)
    logger.addHandler(sh)
    try:
        table = standardize(table)
    except:
        raise
    finally:
        logger.removeHandler(sh)

    assert len(log_stream.getvalue()) == 0

def test_verbose(table):
    # Capture output to ensure that it's printed
    logger = logging.getLogger("opd-std")
    log_stream = StringIO()
    sh = logging.StreamHandler(log_stream)
    logger.addHandler(sh)
    try:
        table = standardize(table, verbose=True)
    except:
        raise
    finally:
        logger.removeHandler(sh)

    assert len(log_stream.getvalue()) > 0

def test_verbose_to_file(table, log_filename):
    table = standardize(table, verbose=log_filename)

    assert os.path.exists(log_filename)
    assert os.path.getsize(log_filename) > 100
    # Ensure that file handler was removed
    assert len(logging.getLogger("opd-std").handlers)==1

def test_verbose_to_file_cleanup_with_error(table, log_filename):
    table.table.loc[:5, gender_col] = "TEST"

    with pytest.raises(ValueError, match="Unknown"):
        table = standardize(table, no_id="error", verbose=log_filename)

    assert os.path.exists(log_filename)
    assert os.path.getsize(log_filename) > 100
    # Ensure that file handler was removed
    assert len(logging.getLogger("opd-std").handlers)==1


def test_race_eth_combo_merge(std_table):
    assert Column.RACE_SUBJECT in std_table.table
    assert Column.RACE_ETHNICITY_SUBJECT in std_table.table
    assert Column.RE_GROUP_SUBJECT in std_table.table
    assert all([isinstance(x,str) for x in std_table.table[Column.RACE_ETHNICITY_SUBJECT].unique()])

def test_race_eth_combo_concat(table):
    table = standardize(table, race_eth_combo="concat")

    r = defs.get_race_cats()
    e = defs.get_eth_cats()

    assert Column.RACE_SUBJECT in table.table
    assert Column.RACE_ETHNICITY_SUBJECT in table.table
    assert f"{r[defs.get_race_keys().BLACK]} {e[defs.get_eth_keys().LATINO]}" in table.table[Column.RACE_ETHNICITY_SUBJECT].unique()


def test_race_eth_combo_false(table):
    table = standardize(table, race_eth_combo=False)
    assert Column.RACE_ETHNICITY_SUBJECT not in table.table

def test_merge_datetime_true(std_table):
    assert Column.DATETIME in std_table.table

def test_merge_datetime_false(table):
    table = standardize(table, merge_date_time=False)
    assert Column.DATETIME not in table.table

def test_empty_time_nat(table, std_table):
    idx = table.table[time_col]==bad_time
    assert idx.sum()>0
    assert Column.DATETIME in std_table.table
    assert std_table.table[Column.DATETIME][idx].isnull().all()

def test_empty_time_ignore(table):
    idx = table.table[time_col]==bad_time
    table = standardize(table, empty_time='ignore')
    assert idx.sum()>0
    assert Column.DATETIME in table.table
    assert (table.table[Column.DATETIME][idx]==table.table[Column.DATE][idx]).all()

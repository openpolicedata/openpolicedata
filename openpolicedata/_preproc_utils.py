from enum import Enum
from numbers import Number
import numpy as np
import pandas as pd
import re

from .utils import camel_case_split
from . import defs

class DataMapping:
    def __init__(self, orig_column_name=None, new_column_name = None, data_maps=None, orig_column=None):
        self.orig_column_name = orig_column_name
        self.new_column_name = new_column_name
        self.data_maps = data_maps
        self.orig_value_counts = orig_column.value_counts().head() if orig_column is not None else None

    def __repr__(self, ) -> str:
        return ',\n'.join("%s: %s" % item for item in vars(self).items())
    
    def __eq__(self, other): 
        if not isinstance(other, DataMapping):
            return False
        
        tf_data_maps = self.data_maps == other.data_maps
        if not tf_data_maps:
            if (self.data_maps is None and other.data_maps is not None) or \
                (self.data_maps is not None and other.data_maps is None):
                return False
            if len(self.data_maps)==len(other.data_maps):
                for k in self.data_maps.keys():
                    if k not in other.data_maps and \
                        not (pd.isna(k) and any([pd.isna(x) for x in other.data_maps.keys()])):
                        return False
                    if pd.isnull(k):
                        kother = [x for x in other.data_maps.keys() if pd.isnull(x)][0]
                    else:
                        kother = k
                    if self.data_maps[k] != other.data_maps[kother]:
                        return False
                
                tf_data_maps = True
            elif set(self.data_maps.keys()).symmetric_difference(set(other.data_maps.keys())) == {np.nan}:
                # Adding nan to data maps later
                if (np.nan in other.data_maps and other.data_maps[np.nan]=="UNSPECIFIED") or \
                    (np.nan in self.data_maps and self.data_maps[np.nan]=="UNSPECIFIED"):
                    tf_data_maps = True


        tf_vals = self.orig_value_counts is None and other.orig_value_counts is None
        if not tf_vals and self.orig_value_counts is not None and other.orig_value_counts is not None:
            tf_vals = self.orig_value_counts.equals(other.orig_value_counts)
            if not tf_vals and other.orig_value_counts.index.dtype=="int64" and all([x.isdigit() for x in self.orig_value_counts.index if isinstance(x,str)]):
                # Found a case where indices were numeric strings but read back in as numbers
                other.orig_value_counts.index = [str(x) for x in other.orig_value_counts.index]
                tf_vals = self.orig_value_counts.equals(other.orig_value_counts)
            if not tf_vals and len(self.orig_value_counts)==len(other.orig_value_counts) and \
                all(self.orig_value_counts.values == other.orig_value_counts.values):
                tf_vals = True
                for x,y in zip(self.orig_value_counts.index, other.orig_value_counts.index):
                    # Difference of trailing 0 is OK
                    if x!=y:
                        if isinstance(x,str) and isinstance(y,str) and (x=="0"+y or y=="0"+x):
                            continue
                        elif isinstance(x,str) and x.isdigit() and isinstance(y,Number) and float(x)==y:
                            continue
                        elif isinstance(y,str) and y.isdigit() and isinstance(x,Number) and float(y)==x:
                            continue
                        elif isinstance(y,str) and isinstance(x, pd._libs.tslibs.timestamps.Timestamp) and \
                            (y==x.strftime("%Y-%m-%d %H:%M:%S") or (y==x.strftime("%Y-%m-%d") and x.hour==0 and x.minute==0)):
                            continue
                        elif isinstance(x,str) and isinstance(y, pd._libs.tslibs.timestamps.Timestamp) and \
                            (x==y.strftime("%Y-%m-%d %H:%M:%S") or (x==y.strftime("%Y-%m-%d") and y.hour==0 and y.minute==0)):
                            continue
                        tf_vals = False

        new_name_equal = self.new_column_name == other.new_column_name or self.new_column_name=="RAW_"+other.new_column_name
                        
        tf = self.orig_column_name == other.orig_column_name and \
            new_name_equal and \
            tf_data_maps and tf_vals

        return tf


def check_column(col_name, col_types):
    if isinstance(col_types, str):
        col_types = [col_types]

    for col_type in col_types:
        col_type = col_type.lower()

        # Check for unambiguous column names
        if col_name.lower()==col_type:
            return True

        # Check if column name is a descriptive term + race
        # sus has been used as shorthand for suspect
        # ofc has been used as shorthand for officer
        desc_terms = ["citizen","subject","suspect", "sus", "civilian", "complainant", "person", "cit",
                      "offender", "officer", "ofc", "deputy", "off"]

        if any([col_name.lower()==x+col_type or col_name.lower()==col_type+x for x in desc_terms]):
            return True
        
        words = set(re.split(r"[^A-Za-z]+", col_name.lower()))
        if any([{x,col_type}.issubset(words) for x in desc_terms]):
            return True

        words = set(camel_case_split(col_name))
        if any([{x,col_type}.issubset(words) for x in desc_terms]):
            return True
    
    return False
    

class _case:
    def __init__(self, src, table_type, old_name, new_name, year=None):
        self.src = src
        self.table_type = table_type
        self.year = year
        self.old_name = old_name if type(old_name)==list else [old_name]
        self.new_name = new_name if type(new_name)==list else [new_name]

    def __repr__(self, ) -> str:
        return ',\n'.join("%s: %s" % item for item in vars(self).items())

    def equals(self, src, table_type, year):
        tf = src==self.src and table_type==self.table_type
        if self.year!=None:
            if isinstance(self.year, Number):
                tf = tf and (year==self.year)
            else:
                tf = tf and (year in self.year)

        return tf
    
    def findcols(self, columns):
        for k in range(len(self.old_name)):
            if self.old_name[k] not in columns:
                return False
        return True


class MultType(Enum):
    SINGLE = 0
    DICT = 1
    DEMO_COL = 2
    COUNTS = 3
    DELIMITED = 4
    WITH_NAME = 5

class _MultData:
    type = MultType.SINGLE
    delim_race = None
    delim_age = None
    delim_gender = None
    delim_eth = None
    item_race = None
    item_age = None
    item_gender = None
    item_eth = None

NOT_REQUIRED_TABLES_FOR_DATE = [defs.TableType.COMPLAINTS_OFFICERS, defs.TableType.COMPLAINTS_SUBJECTS,
                                defs.TableType.USE_OF_FORCE_SUBJECTS_OFFICERS, defs.TableType.USE_OF_FORCE_SUBJECTS, 
                                defs.TableType.USE_OF_FORCE_OFFICERS, 
                                defs.TableType.SHOOTINGS_SUBJECTS, defs.TableType.SHOOTINGS_OFFICERS,
                                defs.TableType.CRASHES_SUBJECTS, defs.TableType.CRASHES_VEHICLES,
                                defs.TableType.COMPLAINTS_ALLEGATIONS, defs.TableType.COMPLAINTS_PENALTIES]

RACE_TABLES_TO_EXCLUDE = [
    ("Milwaukee", defs.TableType.COMPLAINTS),
    ("Santa Rosa", defs.TableType.USE_OF_FORCE),
    ("New York City", defs.TableType.CRASHES_SUBJECTS),
    ("San Diego", defs.TableType.CRASHES_SUBJECTS),
    ("Montgomery County", defs.TableType.COMPLAINTS),
    ("Seattle", defs.TableType.COMPLAINTS),
    ("Albany", defs.TableType.COMPLAINTS),
    ("Dallas", defs.TableType.ARRESTS),
    ("Denver", defs.TableType.STOPS),
    ("Lincoln", defs.TableType.VEHICLE_PURSUITS),
    ("Los Angeles", defs.TableType.STOPS),
    ("South Bend", defs.TableType.USE_OF_FORCE),
    ("South Bend", defs.TableType.COMPLAINTS),
    ("State Police", defs.TableType.SHOOTINGS),
    ("Menlo Park",defs.TableType.STOPS),
    ("Richmond",defs.TableType.CITATIONS),
    ("Gilbert",defs.TableType.STOPS),
    ("Anaheim",defs.TableType.TRAFFIC),
    ("San Bernardino",defs.TableType.TRAFFIC),
    ("Cambridge",defs.TableType.CITATIONS),
    ("Saint Petersburg", defs.TableType.TRAFFIC),
    ("Idaho Falls",defs.TableType.STOPS),
    ("Fort Wayne", defs.TableType.TRAFFIC),
    ("Baltimore", defs.TableType.STOPS),
    ("Lubbock", defs.TableType.STOPS),
    ("Tacoma", defs.TableType.STOPS),
    ("State Patrol", defs.TableType.TRAFFIC),
    ("Greensboro", defs.TableType.USE_OF_FORCE_OFFICERS),
    ("Minneapolis", defs.TableType.LAWSUITS),
    ]
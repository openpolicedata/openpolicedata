import logging
import pandas as pd
import re
from collections import Counter, defaultdict
import numpy as np
from numbers import Number
import warnings

from . import datetime_parser
from . import defs
from . import _converters as convert
from ._converters import  _p_age_range
from ._preproc_utils import _MultData, check_column, DataMapping, _case, MultType
from .utils import camel_case_split, split_words

_skip_tables = ["calls for service"]
_OLD_COLUMN_INDICATOR = "RAW"

logger = logging.getLogger("opd-std")

class _ColMapDict():
    def __init__(self):
        self.cur_dict = {}
        self.orig_dict = {}

    def __repr__(self):
        return f"cur_dict: {self.cur_dict}\norig_dict: {self.orig_dict}"
    
    def __str__(self):
        return f"cur_dict: {self.cur_dict}\norig_dict: {self.orig_dict}"

    def __setitem__(self, key, value):
        self.cur_dict[key] = value
        self.orig_dict[key] = value

    def __getitem__(self,key):
        return self.cur_dict[key]
    
    def __contains__(self, __key: object) -> bool:
        return __key in self.cur_dict
    
    def keys(self):
        return self.cur_dict.keys()
    
    def values(self):
        return self.cur_dict.values()
    
    def items(self):
        return self.cur_dict.items()
    
    def pop(self, key):
        self.orig_dict.pop(key)
        return self.cur_dict.pop(key)
    
    def update_current(self, key, value):
        self.cur_dict[key] = value

    def get_original(self, key):
        return self.orig_dict[key]
    
    def replace(self, old_key, new_key):
        self.cur_dict[new_key] = self.cur_dict.pop(old_key)
        self.orig_dict[new_key] = self.orig_dict.pop(old_key)
        

def standardize(df, table_type, year, 
    known_cols={}, 
    source_name=None, 
    keep_raw=True,
    agg_race_cat=False,
    race_cats=defs.get_race_cats(),
    eth_cats=defs.get_eth_cats(),
    gender_cats=defs.get_gender_cats(),
    no_id="pass",
    race_eth_combo="merge",
    merge_date_time=True,
    empty_time="NaT"): 

    if table_type.lower() in _skip_tables:
        print(f"Standardization is not currently applied to {table_type} table.")
        return df, None
    
    logger.info(f"Original columns:\n{df.columns}")
    
    std = Standardizer(df, table_type, year, known_cols, source_name, keep_raw, agg_race_cat, race_cats, eth_cats, gender_cats, no_id)

    std.id_columns()
    std.standardize_date()
    std.standardize_time()
    if merge_date_time:
        std.merge_date_time(empty_time=empty_time)
    std.standardize_off_or_civ()
    std.check_for_multiple()
    std.standardize_race()
    std.standardize_ethnicity()
    std.combine_race_ethnicity(race_eth_combo)
    
    # standardize_age needs to go before standardize_age_range as it can detect if what was ID'ed as 
    # an age column is actually an age range
    std.standardize_age()
    std.standardize_age_range()
    std.standardize_gender()
    std.standardize_agency()
    std.cleanup()
    std.sort_columns()

    # Print column changes and then full maps
    logger.info(f"Identified columns:")
    for map in std.data_maps:
        logger.info(f"\t{map.orig_column_name}: {map.new_column_name}")

    logger.info("\nData mappings:")
    for map in std.data_maps:
        logger.info(map)
        logger.info("\n")
    
    return std.df, std.data_maps

def _count_values(col, known_delim=None):
    if known_delim is None:
        delims = [",", "|", ";", "/","\n"]
    else:
        delims = [known_delim]
    max_count = -1
    for d in delims:
        num_vals = col.apply(lambda x: len(x.split(d)) if type(x)==str else 1)
        count = (num_vals>1).sum()
        if count > max_count:
            max_count = count
            max_num_vals = num_vals
            delim = d

    # Put nans in for cases where there could be a single value where there should be multiple
    max_num_vals[col.isnull()] = np.nan
    max_num_vals[col == ""] = np.nan
    # In Florida, PD's use Marsy's law to try to shield themselves from releasing certain data
    is_exempt = col.apply(lambda x: "Marsy" in x and "Exempt" in x if isinstance(x,str) else False)
    max_num_vals[is_exempt] = np.nan

    return max_num_vals, delim, max_count, is_exempt


def _find_gender_col_type_advanced(df, source_name, col_names, types, col_map, civilian_col_name, officer_col_name):
    if ("subject_sex" in col_names or "officer_sex" in col_names) and \
        any([x.startswith("raw_") for x in col_names]):
        # Stanford data
        old_col_names = col_names
        old_types = types
        col_names = []
        types = []
        for x,y in zip(old_col_names, old_types):
            if not x.startswith("raw_"):
                col_names.append(x)
                types.append(y)
            else:
                logger.info(f"Removing raw channel {x} from potential gender column names")
    else:
        col_names, types = _set_type_based_on_order(col_names, types, df, source_name, officer_col_name, civilian_col_name)

    return col_names, types


def _find_race_col_type_advanced(df, source_name, col_names, types, col_map, civilian_col_name, officer_col_name):
    if ("subject_race" in col_names or "officer_race" in col_names) and \
        any([x.startswith("raw_") for x in col_names]):
        # Stanford data
        old_col_names = col_names
        old_types = types
        col_names = []
        types = []
        for x,y in zip(old_col_names, old_types):
            if not x.startswith("raw_"):
                col_names.append(x)
                types.append(y)
    elif len(col_names)==2 and all([x in ['tcole_race_ethnicity', 'standardized_race'] for x in col_names]):
        # Austin data
        logger.info(f"Austin data has 2 subject race columns {col_names}. tcole_race_ethnicity will be used to standardize.")
        col_names = ['tcole_race_ethnicity']
        types = types[0:1]
    elif len(col_names)==2 and source_name in ["Los Angeles"] and any([x.endswith("_cd") for x in col_names]):
        # The one with cd is a coded-version of the other
        keep = [k for k,x in enumerate(col_names) if not x.endswith("_cd")]
        logger.info(f"Removing potential race columns {[x for x in col_names if x.endswith('_cd')]} and keeping {col_names[keep[0]:keep[0]+1]}")
        col_names = col_names[keep[0]:keep[0]+1]
        types = types[keep[0]:keep[0]+1]
    else:
        orig_race_cols = col_names
        orig_type = types
        col_names = []
        types = []
        vals = []
        for k, col in enumerate(orig_race_cols):
            new_col = convert.convert(convert._create_race_lut, df[col], source_name)
            
            found = False
            for j, t in enumerate(types):
                if t == orig_type[k]:
                    is_equal = new_col == vals[j]
                    found = is_equal.all()
                    if not found:
                        # Check if only values that are not equal are Asian and AAPI
                        is_aapi = vals[j][~is_equal].apply(lambda x: x in [defs._race_cats_basic[defs._race_keys.AAPI], defs._race_cats_basic[defs._race_keys.ASIAN]]) & \
                            new_col[~is_equal].apply(lambda x: x in [defs._race_cats_basic[defs._race_keys.AAPI], defs._race_cats_basic[defs._race_keys.ASIAN]])
                        if (is_aapi.sum() + (len(is_equal)-len(is_aapi))) / len(is_equal) > 0.999:  # These are very similar. Assuming differences are typos
                            found = True

                    if found:
                        # Check if one is an abbreviation and the other is a description. Description may be more informative.
                        if (df[col_names[j]].apply(lambda x: 1 if isinstance(x,Number) else len(x))<=1).all():
                            # Replace
                            col_names[j] = col
                            vals[j] = new_col
                        break

            if not found:
                col_names.append(col)
                types.append(orig_type[k])
                vals.append(new_col)

        col_names, types = _set_type_based_on_order(col_names, types, df, source_name, officer_col_name, civilian_col_name)

    return col_names, types

def _set_type_based_on_order(col_names, types, df, source_name, officer_col_name, civilian_col_name):
    if len(set(types)) != len(types) and source_name=="Norman":  # There are still matching types
        # Find matching types
        all_types = list(set(types))
        for t in all_types:
            cols = [x for k,x in enumerate(col_names) if types[k]==t]
            if len(cols)>1:
                type = None
                for x in df.columns:
                    if "employ" in x.lower() or "off" in x.lower():
                        type = officer_col_name
                    elif "cit" in x.lower():
                        type = civilian_col_name

                    if x in cols and type!=None:
                        types[[k for k,y in enumerate(col_names) if x==y][0]] = type

    return col_names, types


class Standardizer:
    def __init__(self, df, table_type, year, 
            known_cols={}, 
            source_name=None, 
            keep_raw=True,
            agg_race_cat=False,
            race_cats=defs.get_race_cats(),
            eth_cats=defs.get_eth_cats(),
            gender_cats=defs.get_gender_cats(),
            no_id="pass"
        ) -> None:

        self.df = df
        self.table_type = table_type
        self.year = year
        self.known_cols = defaultdict(lambda: None, known_cols)
        self.source_name = source_name
        self.keep_raw=keep_raw
        self.agg_race_cat=agg_race_cat
        self.race_cats=race_cats
        self.eth_cats=eth_cats
        self.gender_cats = gender_cats
        self.no_id = no_id

        self.col_map = _ColMapDict()
        self.data_maps = []

        self.mult_civilian = _MultData()
        self.mult_officer = _MultData()
        self.mult_both = _MultData()

    def _pattern_search(self, select_cols, patterns, match_substr, run_all=False):
        matches = []
        for p in patterns:
            if p[0].lower() == "equals":
                matches = [x for x in select_cols if x.lower() == p[1].lower()]
            elif p[0].lower() == "not equal":
                matches = [x for x in select_cols if x.lower() != p[1].lower()]
            elif p[0].lower() == "does not contain":
                if type(p[1]) == str:
                    matches = [x for x in select_cols if p[1].lower() not in x.lower()]
                else:
                    matches = [x for x in select_cols if p[1][0].lower() not in x.lower()]
                    for k in range(1,len(p[1])):
                        matches = [x for x in matches if p[1][k].lower() not in x.lower()]
            elif p[0].lower() == "contains":
                matches = [x for x in select_cols if p[1].lower() in x.lower()]
            elif p[0].lower() == "format":
                if type(match_substr) != str:
                    raise TypeError("match_substr should be a string")
                guess = match_substr[0].lower()
                pattern = p[1]
                
                def find_matches(columns, guess, match_substr, idx, pattern):
                    matches = [x for x in columns if x.lower() == pattern.format(guess).lower()]
                    if len(matches)>0:
                        return matches

                    for k,val in enumerate(match_substr[idx:]):
                        new_guess = guess + val
                        matches = find_matches(columns, new_guess, match_substr, idx+k+1, pattern)
                        if len(matches)>0:
                            return matches

                    return []

                matches = find_matches(self.df.columns, guess, match_substr, 1, pattern)
            else:
                raise ValueError("Unknown pattern type")

            if not run_all and len(matches) > 0 and len(matches)!=len(select_cols):
                break
            elif run_all:
                select_cols = matches

        return matches


    def _remove_excluded(self, match_cols, exclude_col_names, match_substr):
        match_cols_out = match_cols.copy()
        for e in exclude_col_names:
            if type(e) == tuple:
                match_cols_out = self._pattern_search(match_cols_out, [e], match_substr[0], run_all=True)
            elif e in match_cols_out:
                match_cols_out.remove(e)

        return match_cols_out


    def _find_col_matches(self, id, match_substr, 
        known_col_name=None, 
        only_table_types=None,
        exclude_table_types=[], 
        not_required_table_types="ALL", 
        exclude_col_names=[], 
        secondary_patterns=[], 
        validator=None,
        always_validate=False,
        validate_args=[],
        std_col_name = None,
        search_data=False,
        tables_to_exclude=[]):

        logger.info(f"Searching for {id} column")
        if self.table_type in exclude_table_types or \
            (only_table_types != None and self.table_type not in only_table_types):
            if std_col_name in self.df.columns:
                logger.info(f"\tSetting {id} column as {std_col_name}")
                return [std_col_name]
            else:
                logger.info(f"\tTable type {self.table_type} is excluded from tables to search for {id} column. Skipping...")
                return []

        officer_terms = ["officer","deputy", "empl"]
        if known_col_name != None:
            if known_col_name not in self.df.columns:
                raise ValueError(f"Known column {known_col_name} is not in the DataFrame")
            logger.info(f"\tSetting {id} column as {known_col_name}")
            return [known_col_name]
        else:
            if isinstance(match_substr, str):
                match_substr = [match_substr]

            match_cols = []
            civilian_terms = ["citizen","subject","suspect","civilian", "cit", "offender"]
            civilian_found = False
            officer_found = False
            for s in match_substr:
                new_matches = [x for x in self.df.columns if s.lower() in x.lower()]
                new_matches = [x for x in new_matches if x not in match_cols]
                new_matches = self._remove_excluded(new_matches, exclude_col_names, match_substr)
                if len(new_matches)>0 and len(split_words(s))==1:
                    m = 0
                    while m < len(new_matches):
                        words = split_words(new_matches[m])
                        if len(words)>1 and s.lower() != new_matches[m].lower(): # Skip cases where column name is all lower or upper case
                            # Require match to be found in individual words to avoid cases where substring is found in part of a longer word
                            if not any([s.lower()==x.lower() for x in words]):
                                new_matches.pop(m)
                            else:
                                m+=1
                        else:
                            m+=1

                if len(new_matches)>0:
                    if not officer_found and not civilian_found:
                        match_cols.extend(new_matches)
                    elif officer_found:
                        # Only keep columns with subject terms
                        match_cols.extend([x for x in new_matches if any([y in x.lower() for y in civilian_terms])])
                    elif civilian_found:
                        # Only keep columns with officer terms
                        match_cols.extend([x for x in new_matches if any([y in x.lower() for y in officer_terms])])

                    all_raw = all([x.startswith("raw_") for x in match_cols])

                    # There are cases where there should be multiple matches for both officer and community member
                    # columns. On occasion, they are labeled with different terms 
                    officer_found = False
                    civilian_found = False
                    for x in match_cols:
                        x = split_words(x)
                        if len(x)==1:
                            officer_found = officer_found or any([y in x[0].lower() for y in officer_terms])
                            civilian_found = civilian_found or any([y in x[0].lower() for y in civilian_terms])
                        else:
                            # Require match to full word
                            officer_found = officer_found or any([any([y == z.lower() for y in officer_terms]) for z in x])
                            civilian_found = civilian_found or any([any([y == z.lower() for y in civilian_terms]) for z in x])

                    if civilian_found == officer_found and not all_raw:  # civilian_found == officer_found: Both found or not found
                        break

            if len(match_cols)==0 and len(match_substr)>0:
                match_cols = self._pattern_search(self.df.columns, secondary_patterns, match_substr[0])
                match_cols = self._remove_excluded(match_cols, exclude_col_names, match_substr)
                
            if len(match_cols)>1:
                new_matches = self._pattern_search(match_cols, secondary_patterns, match_substr[0])
                if len(new_matches)>0:
                    match_cols = new_matches
                    match_cols = self._remove_excluded(match_cols, exclude_col_names, match_substr)
                elif len(match_cols)==0 and len(secondary_patterns)>0:
                    raise NotImplementedError()

            multi_check = True
            if len(match_cols) == 2:
                # Check if one column is an officer column and one is not
                multi_check = sum([any([y.lower() in x.lower() for y in officer_terms]) for x in match_cols])!=1

            if always_validate or (multi_check and validator != None and len(match_cols) > 1):
                logger.info(f"Testing data in column(s) {match_cols} as potential {id} column")
                match_cols = validator(self.df, match_cols, *validate_args)
                logger.info(f"Validated data in column(s) {match_cols} as potential {id} column")

            if search_data and len(match_cols)==0:
                logger.info(f"Testing data in all columns as potential {id} column")
                match_cols = validator(self.df, self.df.columns, *validate_args)
                logger.info(f"Validated data in column(s) {match_cols} as potential {id} column")
                
            match_cols_out = self._remove_excluded(match_cols, exclude_col_names, match_substr)
            if match_cols_out!=match_cols:
                logger.info(f"After applying rules {exclude_col_names}, potential {id} columns reduced from {match_cols} to {match_cols_out}")
                match_cols = match_cols_out

            if len(match_cols)==0 and not_required_table_types != "ALL" and self.table_type not in not_required_table_types and \
                (self.source_name, self.table_type) not in tables_to_exclude:
                raise ValueError(f"Column not found with substring {match_substr}")

            return match_cols


    def id_columns(self):
        # Find the date columns
        match_cols = self._find_col_matches("date", "date", known_col_name=self.known_cols[defs.columns.DATE], 
            std_col_name=defs.columns.DATE,
            secondary_patterns = [("equals","date"),("contains","time"),("does not contain", "officer")],
            exclude_col_names=[("does not contain", ["as_of","last_reported","objectid"])], # Terms associated with dates not of interest
            # Calls for services often has multiple date/times with descriptive names for what it corresponds to.
            # Don't generalize by standardizing
            exclude_table_types=[defs.TableType.EMPLOYEE, defs.TableType.CALLS_FOR_SERVICE], 
            validator=datetime_parser.validate_date,
            always_validate=True,
            tables_to_exclude=[("Winooski", defs.TableType.TRAFFIC),
                               ("Austin", defs.TableType.TRAFFIC_ARRESTS),
                               ("Norwich", defs.TableType.USE_OF_FORCE),
                               ("Northampton", defs.TableType.USE_OF_FORCE)],
            search_data=True)

        if len(match_cols) > 1:
            # Not relabeling multiple dates because they can have useful info in the column name
            logger.info(f"Multiple {defs.columns.DATE} columns found. None will be standardized.")
        elif len(match_cols) == 1:
            logger.info(f"Column {match_cols[0]} identified as a {defs.columns.DATE} column")
            self.col_map[defs.columns.DATE] = match_cols[0]
        
        secondary_patterns = []
        validator_args = []
        exclude_col_names = ["rankattimeofincident", ("does not contain","total")]
        if defs.columns.DATE in self.col_map:
            # Create a pattern from the format of the date column name
            # That might also be the pattern of the time column
            success = False
            if "date" in self.col_map[defs.columns.DATE].lower():
                match = re.search('date', self.col_map[defs.columns.DATE], re.IGNORECASE)
                if match != None:
                    if match[0].islower():
                        secondary_patterns = [("equals", self.col_map[defs.columns.DATE].replace(match[0], "time"))]
                    elif match[0].isupper():
                        secondary_patterns = [("equals", self.col_map[defs.columns.DATE].replace(match[0], "TIME"))]
                    elif match[0].istitle():
                        secondary_patterns = [("equals",self.col_map[defs.columns.DATE].replace(match[0], "Time"))]
                    else:
                        raise NotImplementedError()
                    success = True

            if not success:
                segs = split_words(self.col_map[defs.columns.DATE])
                if len(segs)>1:
                    date_str = defs.columns.DATE.lower()
                    pattern = ""
                    date_found = False
                    curloc = 0
                    for k,seg in enumerate(segs):
                        if not date_found and seg[0].lower() == date_str[0]:
                            # Check if rest of seg consists of values in date_str
                            found = True
                            idx = 1
                            for val in seg[1:].lower():
                                while idx < len(date_str) and val != date_str[idx]:
                                    idx+=1

                                if idx==len(date_str):
                                    found = False
                                    break
                                else:
                                    idx+=1
                            if found:
                                pattern+="{}"
                                date_found = True
                            else:
                                pattern+=seg
                        else:
                            pattern+=seg

                        if k < len(segs)-1:
                            curloc+=len(seg)
                            while self.col_map[defs.columns.DATE][curloc:curloc+len(segs[k+1])]!= segs[k+1]:
                                pattern+=self.col_map[defs.columns.DATE][curloc]
                                curloc+=1
                    
                    if date_found:
                        secondary_patterns = [("format", pattern)]

            if defs.columns.DATE in self.col_map:
                # Don't select the date column as the time column too
                exclude_col_names.append(self.col_map[defs.columns.DATE])
                date_data = self.df[self.col_map[defs.columns.DATE]]
                if self.col_map[defs.columns.DATE].lower() == "year":
                    month_idx = [k for k,x in enumerate(self.df.columns) if x.lower() == "month"]
                    if len(month_idx) > 0:
                        date_cols = [self.col_map[defs.columns.DATE], self.df.columns[month_idx[0]]]

                        day_idx = [k for k,x in enumerate(self.df.columns) if x.lower() == "day"]
                        if len(day_idx) > 0:
                            date_cols.append(self.df.columns[day_idx[0]])
                            date_data = self.df[date_cols]
                        else:
                            date_data = self.df[date_cols].copy()
                            date_data["day"] = [1 for _ in range(len(self.df))]
                                        
                date_data = datetime_parser.parse_date_to_datetime(date_data)
                validator_args.append(date_data)
            
        match_cols = self._find_col_matches("time", ["time", "tm", "toa"], 
            std_col_name=defs.columns.TIME,
            secondary_patterns=secondary_patterns, 
            validator=datetime_parser.validate_time,
            validate_args=validator_args,
            exclude_col_names=exclude_col_names,
            exclude_table_types=[defs.TableType.CALLS_FOR_SERVICE],
            always_validate=True)

        if len(match_cols) > 1:
            # Not relabeling multiple times because they can have useful info in the column name
            logger.info(f"Multiple {defs.columns.TIME} columns found. None will be standardized.")
        elif len(match_cols) == 1:
            logger.info(f"Column {match_cols[0]} identified as a {defs.columns.TIME} column")
            self.col_map[defs.columns.TIME] = match_cols[0]
            
        # Incidents tables shouldn't have demographics data
        if self.table_type not in [defs.TableType.SHOOTINGS_INCIDENTS, defs.TableType.USE_OF_FORCE_INCIDENTS]:    
            def role_validator(df, match_cols_test):
                match_cols = []
                for col_name in match_cols_test:
                    try:
                        col = df[col_name]
                        # Function for validating a column indicates whether the person described is a civilian or officer
                        new_col = convert.convert(convert.convert_off_or_civ, col)
                        vals = new_col.unique()

                        if not any([isinstance(x,str) for x in vals]) or (defs._roles.SUBJECT not in vals and defs._roles.OFFICER not in vals):
                            continue

                        if defs._roles.SUBJECT not in vals or defs._roles.OFFICER not in vals:
                            # California data, RAE = Race and Ethnicity
                            # Check if it's possible this does not indicate subject vs officer
                            race_match_cols = self._find_col_matches("race", ["race", "descent","rae_full","citizen_demographics","officer_demographics","ethnicity"],
                                validator=_race_validator, validate_args=[self.source_name])

                            off_type = False
                            civ_type = False
                            is_officer_table = self.table_type == defs.TableType.EMPLOYEE.value or \
                                ("- OFFICERS" in self.table_type and "SUBJECTS" not in self.table_type)
                            for k in range(len(race_match_cols)):
                                if "off" in race_match_cols[k].lower() or "deputy" in race_match_cols[k].lower() or \
                                    (is_officer_table and "suspect" not in race_match_cols[k].lower() and "supsect" not in race_match_cols[k].lower()):
                                    off_type = True
                                else:
                                    civ_type = True
                                
                            if civ_type and off_type:
                                continue

                        match_cols.append(col_name)
                    except:
                        pass

                return match_cols

            match_cols = self._find_col_matches("Is Person Officer or Subject", ["Civilian_Officer","ROLE"], 
                only_table_types = [defs.TableType.USE_OF_FORCE, defs.TableType.USE_OF_FORCE_SUBJECTS_OFFICERS, defs.TableType.SHOOTINGS],
                exclude_col_names=[("not equal","SubjectRole")],  # Subject role would be role of subject not whether person is subject or officer
                validator=role_validator,
                always_validate=True)
            if len(match_cols) > 1:
                raise NotImplementedError(f"Multiple columns {match_cols} found for {defs.columns.SUBJECT_OR_OFFICER} column")
            elif len(match_cols) == 1:
                logger.info(f"Column {match_cols[0]} identified as a {defs.columns.SUBJECT_OR_OFFICER} column")
                self.col_map[defs.columns.SUBJECT_OR_OFFICER] = match_cols[0]

            match_cols = self._find_col_matches("race", ["race", "citizen_demographics","officer_demographics","ethnicity","re_grp"],
                validator=_race_validator, 
                validate_args=[self.source_name],
                exclude_table_types=[defs.TableType.INCIDENTS],  # Standardization will be make it unclear if column is for suspect or victim
                search_data=True)  
            
            logger.info(f"Potential race columns found: {match_cols}")

            # TODO: Remove failures when column are not found
            race_cols, race_types = self._id_demographic_column(match_cols,
                defs.columns.RACE_SUBJECT, defs.columns.RACE_OFFICER,
                defs.columns.RACE_OFFICER_SUBJECT,
                specific_cases=[_case("California", defs.TableType.USE_OF_FORCE_SUBJECTS_OFFICERS, "Race_Ethnic_Group", defs.columns.RACE_OFFICER_SUBJECT),
                                _case("Minneapolis", defs.TableType.STOPS, "race", defs.columns.RACE_SUBJECT),
                                _case("Austin", defs.TableType.USE_OF_FORCE_SUBJECTS, "subject_race_ethnicity", defs.columns.RACE_SUBJECT),
                                _case("Fairfax County", defs.TableType.ARRESTS, ["ArresteeRa","OfficerRac"], [defs.columns.RACE_SUBJECT, defs.columns.RACE_OFFICER], year=range(2016,2021)),
                                _case("Fairfax County", defs.TableType.TRAFFIC_CITATIONS, ["Person_Rac","OfficerRac"], [defs.columns.RACE_SUBJECT, defs.columns.RACE_OFFICER]),
                                _case("Lansing", defs.TableType.SHOOTINGS, ["Race_Sex","Officer"], [defs.columns.RACE_SUBJECT, defs.columns.RACE_OFFICER]),
                                _case("Dallas", defs.TableType.SHOOTINGS, "officer_s", defs.columns.RACE_OFFICER)
                    ],
                adv_type_match=_find_race_col_type_advanced)

            # enthnicity is to deal with typo in Ferndale data. Consider using rapidfuzz in future for fuzzy matching
            match_cols = self._find_col_matches("ethnicity", ["ethnicity", "ethnic", "enthnicity"], exclude_col_names=race_cols,
                                                exclude_table_types=[defs.TableType.COMPLAINTS_ALLEGATIONS,defs.TableType.INCIDENTS],
                                                validator=_eth_validator,
                                                always_validate=True)
            logger.info(f"Potential ethnicity columns found: {match_cols}")

            self._id_ethnicity_column(race_types, match_cols, race_cols,
                specific_cases=[
                    _case("Fairfax County", defs.TableType.ARRESTS, "ArresteeEt", defs.columns.ETHNICITY_SUBJECT)
                ])

            # Do not want the result to contain the word agency
            match_substr=["age","citizen_demographics","officer_demographics"]
            match_cols = self._find_col_matches("age", match_substr, 
                exclude_col_names=[("does not contain",["group","range","at_hire"])],
                validator=_age_validator,
                validate_args=[match_substr],
                exclude_table_types=[defs.TableType.INCIDENTS],  # Standardization will be make it unclear if column is for suspect or victim
                always_validate=True)
            
            logger.info(f"Potential age columns found: {match_cols}")

            self._id_demographic_column(match_cols, 
                defs.columns.AGE_SUBJECT, defs.columns.AGE_OFFICER,
                defs.columns.AGE_OFFICER_SUBJECT,
                required=False,
                specific_cases=[_case("Norman", defs.TableType.COMPLAINTS, "Age", defs.columns.AGE_OFFICER, year=[2016,2017,2018]),
                                _case("Norman", defs.TableType.USE_OF_FORCE, "Age", defs.columns.AGE_OFFICER, year=[2016,2017]),
                                _case("Fairfax County", defs.TableType.ARRESTS, "ArresteeAg", defs.columns.AGE_SUBJECT)]
                )

            match_cols = self._find_col_matches("age range", ["agerange","age_range","age range","agegroup","age_group"],
                                                exclude_table_types=[defs.TableType.INCIDENTS])  # Standardization will be make it unclear if column is for suspect or victim)
            
            logger.info(f"Potential age group columns found: {match_cols}")

            self._id_demographic_column(match_cols,
                defs.columns.AGE_RANGE_SUBJECT, defs.columns.AGE_RANGE_OFFICER,
                defs.columns.AGE_RANGE_OFFICER_SUBJECT,
                required=False)
            
            match_cols = self._find_col_matches("gender", ["gender", "gend", "sex","citizen_demographics","officer_demographics"],
                validator=_gender_validator, validate_args=[self.source_name],
                exclude_table_types=[defs.TableType.INCIDENTS])  # Standardization will be make it unclear if column is for suspect or victim) 

            logger.info(f"Potential gender columns found: {match_cols}")

            self._id_demographic_column(match_cols,
                defs.columns.GENDER_SUBJECT, defs.columns.GENDER_OFFICER, 
                defs.columns.GENDER_OFFICER_SUBJECT,
                required=False,
                specific_cases=[_case("California", defs.TableType.STOPS, "G_FULL", defs.columns.GENDER_SUBJECT),
                                _case("Lansing", defs.TableType.SHOOTINGS, ["Race_Sex","Officer"], [defs.columns.GENDER_SUBJECT, defs.columns.GENDER_OFFICER]),
                                _case("Fairfax County", defs.TableType.ARRESTS, ["ArresteeSe","OfficerSex"], [defs.columns.GENDER_SUBJECT, defs.columns.GENDER_OFFICER]),
                                _case("Dallas", defs.TableType.SHOOTINGS, "officer_s", defs.columns.GENDER_OFFICER)
                    ],
                adv_type_match=_find_gender_col_type_advanced)

        match_cols = self._find_col_matches("agency", [], known_col_name=self.known_cols[defs.columns.AGENCY])
        if len(match_cols) > 1:
            raise NotImplementedError()
        elif len(match_cols) == 1:
            logger.info(f"Column {match_cols[0]} identified as a {defs.columns.AGENCY} column")
            self.col_map[defs.columns.AGENCY] = match_cols[0]

        for key, value in self.col_map.items():
            if key == value:
                self.col_map.update_current(key, self._cleanup_old_column(value, keep_raw=True))
                logger.info(f"Column {key} matches output standardized column name so changed to {self.col_map[key]}")

    
    def _id_ethnicity_column(self, race_types, eth_cols, race_cols, specific_cases=[]):
        for c in specific_cases:
            if c.equals(self.source_name, self.table_type, self.year) and c.findcols(self.df.columns):
                for k in range(len(c.old_name)):
                    self.col_map[c.new_name[k]] = c.old_name[k]
                return

        if len(eth_cols)==0:
            return

        eth_types = []
        validation_types = []
        if defs.columns.SUBJECT_OR_OFFICER in self.col_map:
            if len(eth_cols) > 1:
                raise NotImplementedError()

            eth_types.append(defs.columns.ETHNICITY_OFFICER_SUBJECT)
            validation_types.append(defs.columns.RACE_OFFICER_SUBJECT)
            logger.info(f"Column {eth_cols[0]} associated with {eth_types[0]}")
        else:
            is_officer_table = self.table_type == defs.TableType.EMPLOYEE.value or \
                    ("- OFFICERS" in self.table_type and "SUBJECTS" not in self.table_type)
            for k in range(len(eth_cols)):
                if "officer" in eth_cols[k].lower() or \
                    "offcr" in eth_cols[k].lower() or is_officer_table or \
                    (self.source_name=="Orlando" and self.table_type==defs.TableType.SHOOTINGS and eth_cols[k]=="ethnicity"):
                    eth_types.append(defs.columns.ETHNICITY_OFFICER)
                    validation_types.append(defs.columns.RACE_OFFICER)
                else:
                    eth_types.append(defs.columns.ETHNICITY_SUBJECT)
                    validation_types.append(defs.columns.RACE_SUBJECT)

                logger.info(f"Column {eth_cols[k]} associated with {eth_types[k]}")

        if len(set(eth_types)) != len(eth_types) and all([x==defs.columns.ETHNICITY_SUBJECT for x in eth_types]):
            # See if we can split the column names and find PO for police officer
            eth_types = [defs.columns.ETHNICITY_OFFICER if "PO" in re.split(r"[\W^_]+", x.upper()) else defs.columns.ETHNICITY_SUBJECT for x in eth_cols]
            validation_types = [defs.columns.RACE_OFFICER if "PO" in re.split(r"[\W^_]+", x.upper()) else defs.columns.RACE_SUBJECT for x in eth_cols]
            if any([x==defs.columns.ETHNICITY_OFFICER for x in eth_types]):
                tmp = [x for x,y in zip(eth_cols, eth_types) if y==defs.columns.ETHNICITY_OFFICER]
                logger.info(f"Column(s) {tmp} updated to be associated with {defs.columns.ETHNICITY_OFFICER}")

        if len(set(eth_types)) != len(eth_types):
            if (len(eth_cols)==2 and self.table_type==defs.TableType.INCIDENTS and \
                    ( ("victim" in eth_cols[0] and any([x in eth_cols[1] for x in ["offender","suspect"]])) or \
                    (any([x in eth_cols[0] for x in ["offender","suspect"]]) and "victim" in eth_cols[1]))):
                # Has columns for officer perceived and actual race. We don't code separately for this.
                # Warn and do not create a standardized race column
                warnings.warn(f"{self.source_name} has multiple race columns for subjects ({eth_cols}). " +
                            "Neither will be standardized to avoid creating ambiguity.")
                return
            else:
                # TODO: Convert to a warning with an informative message
                raise NotImplementedError()

        k = 0
        while k < len(validation_types):
            # Check if there is a corresponding race column (this is required)
            if validation_types[k] not in race_types:
                # Check if detected ethnicity column might actually be a race/ethnicity column
                try:
                    mult_data = _MultData()
                    num_race, mult_data.delim_race, max_count_race, _ = _count_values(self.df[eth_cols[k]])
                    if max_count_race>0:
                        potential_mults = self.df[eth_cols[k]][num_race>1]
                        # races = ["WHITE","BLACK","ASIAN"]  # Leaving out HISPANIC as items could be labeled race/ethnicity
                        # r = ["W","B","H","A"]
                        for x in potential_mults:
                            # Check for repeats
                            x_split = x.split(mult_data.delim_race)
                            if any([y>1 for y in Counter(x_split).values()]):
                                mult_data.type = MultType.DELIMITED
                                break
                    _race_validator(self.df[eth_cols[k]], self.source_name, self.source_name, mult_data=mult_data)

                    # If the validator does not throw an error, this is a race column
                    if validation_types[k] in self.col_map:
                        raise KeyError(f"{validation_types[k]} not expected to be in col_map")
                    self.col_map[validation_types[k]] = eth_cols[k]
                    eth_cols.pop(k)
                    validation_types.pop(k)
                    eth_types.pop(k)
                except:
                    raise NotImplementedError() 
            else:
                k+=1

        for k in range(len(eth_cols)):
            if eth_types[k] == defs.columns.ETHNICITY_SUBJECT and \
                "subject_race" in race_cols and eth_cols[k].startswith("raw"):
                # This is a raw column from Stanford data that has already been standardized into subject_race
                continue
            self.col_map[eth_types[k]] = eth_cols[k]

    
    def _id_demographic_column(self, col_names, 
        civilian_col_name, officer_col_name, civ_officer_col_name,
        required=False,
        tables_to_exclude=[],
        sources_to_exclude=[],
        specific_cases=[],
        adv_type_match=None):

        for c in specific_cases:
            if c.equals(self.source_name, self.table_type, self.year) and c.findcols(self.df.columns):
                col_names = c.old_name
                types = c.new_name
                for k in range(len(c.old_name)):
                    self.col_map[c.new_name[k]] = c.old_name[k]
                return col_names, types

        if len(col_names) == 0:
            if not required or \
                self.table_type in [defs.TableType.USE_OF_FORCE_INCIDENTS, defs.TableType.SHOOTINGS_INCIDENTS, 
                                    defs.TableType.CALLS_FOR_SERVICE, 
                                    defs.TableType.CRASHES, defs.TableType.CRASHES_INCIDENTS, defs.TableType.CRASHES_VEHICLES, 
                                    defs.TableType.CRASHES_SUBJECTS, defs.TableType.CRASH_NONMOTORIST,
                                    defs.TableType.INCIDENTS, defs.TableType.COMPLAINTS_BACKGROUND,
                                    defs.TableType.COMPLAINTS_ALLEGATIONS, defs.TableType.COMPLAINTS_PENALTIES] or \
                self.source_name in sources_to_exclude or \
                any([(self.source_name, self.table_type)==x for x in tables_to_exclude]):
                return col_names, []
            else:
                raise NotImplementedError()

        if defs.columns.SUBJECT_OR_OFFICER in self.col_map:
            if len(col_names) > 1:
                raise NotImplementedError()

            self.col_map[civ_officer_col_name] = col_names[0]
            return col_names, [civ_officer_col_name]
        else:     
            is_officer_table = self.table_type == defs.TableType.EMPLOYEE.value or \
                ("- OFFICERS" in self.table_type and "SUBJECTS" not in self.table_type)
            off_words = ["off", "deputy", "employee", "ofc", "empl"]
            not_off_words = ["offender"]

            types = []
            for k in range(len(col_names)):
                words = split_words(col_names[k])
                for w in words:
                    if (any([x in w.lower() for x in off_words]) and not any([x in w.lower() for x in not_off_words])) or \
                        (is_officer_table and "suspect" not in w.lower() and "supsect" not in w.lower()):
                        types.append(officer_col_name)
                        break
                else:
                    types.append(civilian_col_name)

                logger.info(f"Column {col_names[k]} associated with {types[k]}")

            if len(set(types)) != len(types):
                if is_officer_table:
                    # Check if one of the columns is clearly an officer column
                    is_def_off = []
                    for k in range(len(col_names)):
                        if "off" in col_names[k].lower() or "deputy" in col_names[k].lower() or \
                            "employee" in col_names[k].lower() or "ofcr" in col_names[k].lower():
                            logger.info(f"Column {col_names[k]} contains information suggesting it describes officer demographics")
                            is_def_off.append(True)
                        else:
                            is_def_off.append(False)
                    if any(is_def_off):
                        for k in reversed(range(len(col_names))):
                            if types[k]==officer_col_name and not is_def_off[k]:
                                logger.info(f"Removing {col_names[k]} from columns that will be standardized")
                                col_names.pop(k)
                                types.pop(k)

                if len(set(types)) != len(types) and all([x==civilian_col_name for x in types]):
                    # See if we can split the column names and find PO for police officer
                    types = [officer_col_name if "PO" in re.split(r"[\W^_]+", x.upper()) else civilian_col_name for x in col_names]
                    if any([x==officer_col_name for x in types]):
                        tmp = [x for x,y in zip(col_names, types) if y==officer_col_name]
                        logger.info(f"Column(s) {tmp} updated to be associated with {officer_col_name}")

                if len(set(types)) != len(types) and all([x==types[0] for x in types]):
                    # Check if all columns are the same except an incrementing number indicating same data for multiple people
                    strings = ["" for _ in col_names]
                    nums = ["" for _ in col_names]
                    is_nums = True
                    # Separate column names into numbers and non-numbers
                    for k, c in enumerate(col_names):
                        for l in c:
                            if l.isdigit():
                                nums[k]+=l
                            else:
                                strings[k]+=l
                        if len(nums[k])>0:
                            nums[k] = int(nums[k])
                        else:
                            is_nums = False
                            break

                    if is_nums and set(nums) == set(range(1, len(types)+1)) and \
                        all([x==strings[0] for x in strings]):
                        # Values are the same except for increment number
                        # Sort columns by number
                        col_names_new = []
                        for k in range(1, len(types)+1):
                            cur_num = [j for j,n in enumerate(nums) if n==k][0]
                            col_names_new.append(col_names[cur_num])

                        # Merge columns
                        # TODO: Add # of persons method
                        # TODO: Add get person by # item
                        def combine_col(x):
                            # Create a list of values to include starting from first non-null to exclude empties at end
                            vals_reversed = []
                            for k in reversed(x.values):
                                if pd.notnull(k) and not (isinstance(x,str) and len(x)==0):
                                    vals_reversed.append(k)
                                elif len(vals_reversed)>0:
                                    vals_reversed.append("")

                            persons = {}
                            for k, val in enumerate(reversed(vals_reversed)):
                                persons[k] = val

                            return persons

                        logger.info(f"Combining multiple subject demographics columns, {col_names_new}, into a new column")
                        self.df[types[0]] = self.df[col_names_new].apply(combine_col,axis=1)
                        for col in col_names:
                            self._cleanup_old_column(col)
                        # Force rename of column to not conflict with final standardized column
                        new_name = self._cleanup_old_column(types[0], keep_raw=True)
                        logger.info(f"New column name is {new_name}")
                        self.data_maps.append(DataMapping(orig_column_name=col_names, new_column_name=new_name))
                        col_names = [new_name]
                        types = types[0:1]
                
                if len(set(types)) != len(types) and adv_type_match != None:
                    col_names, types = adv_type_match(self.df, self.source_name, col_names, types, self.col_map, civilian_col_name, officer_col_name)

                if len(set(types)) != len(types):
                    if (self.source_name=="Winooski" and set(col_names)==set(['Perceived Race', 'Issued To Race'])) or \
                        (len(col_names)==2 and self.table_type==defs.TableType.INCIDENTS and \
                            ( ("victim" in col_names[0] and any([x in col_names[1] for x in ["offender","suspect"]])) or \
                            (any([x in col_names[0] for x in ["offender","suspect"]]) and "victim" in col_names[1]))):
                        # Has columns for officer perceived and actual race. We don't code separately for this.
                        # Warn and do not create a standardized race column
                        warnings.warn(f"{self.source_name} has multiple race columns for subjects ({col_names}). " +
                                    "Neither will be standardized to avoid creating ambiguity.")
                        col_names = []
                        types = []
                    else:
                        # TODO: Convert to a warning with an informative message
                        raise NotImplementedError()

            for k in range(len(col_names)):
                logger.info(f"Column {col_names[k]} will be mapped to {types[k]}")
                self.col_map[types[k]] = col_names[k]

        return col_names, types
    
    def _cleanup_old_column(self, col_name, keep_raw=None):
        keep_raw = self.keep_raw if keep_raw is None else keep_raw
        if keep_raw:
            if not col_name.startswith(_OLD_COLUMN_INDICATOR):
                new_name = _OLD_COLUMN_INDICATOR+"_"+col_name
                logger.info(f"Renaming raw column {col_name} to {new_name}")
                self.df.rename(columns={col_name : new_name}, inplace=True)
            else:
                new_name = col_name
            return new_name
        else:
            logger.info(f"Removing raw column {col_name}")
            self.df.drop(col_name, axis=1, inplace=True)
            return None
        

    def standardize_date(self):
        if defs.columns.DATE in self.col_map:
            date_data = self.df[self.col_map[defs.columns.DATE]]
            if self.col_map[defs.columns.DATE].lower() == "year":
                month_idx = [k for k,x in enumerate(self.df.columns) if x.lower() == "month"]
                if len(month_idx) > 0:
                    date_cols = [self.col_map[defs.columns.DATE], self.df.columns[month_idx[0]]]

                    day_idx = [k for k,x in enumerate(self.df.columns) if x.lower() == "day"]
                    if len(day_idx) > 0:
                        date_cols.append(self.df.columns[day_idx[0]])
                        date_data = self.df[date_cols]
                    else:
                        date_data = self.df[date_cols].copy()
                        date_data["day"] = [1 for _ in range(len(self.df))]
                                    
            s_date = datetime_parser.parse_date_to_datetime(date_data)
            # s_date.name = defs.columns.DATE
            self.df[defs.columns.DATE] = s_date
            # self.df = pd.concat([self.df, s_date], axis=1)

            self.data_maps.append(DataMapping(orig_column_name=self.col_map.get_original(defs.columns.DATE), new_column_name=defs.columns.DATE,
                                              orig_column=self.df[self.col_map[defs.columns.DATE]]))


    def standardize_time(self):
        if defs.columns.TIME in self.col_map:
            self.df[defs.columns.TIME] = datetime_parser.parse_time(self.df[self.col_map[defs.columns.TIME]])

            self.data_maps.append(DataMapping(orig_column_name=self.col_map.get_original(defs.columns.TIME), new_column_name=defs.columns.TIME,
                                              orig_column=self.df[self.col_map[defs.columns.TIME]]))
            
    
    def merge_date_time(self, empty_time="NaT"):
        if defs.columns.DATE in self.col_map and defs.columns.TIME in self.col_map:
            empty_time = empty_time.lower()
            if empty_time not in ["nat", "ignore"]:
                raise ValueError("empty_time must either be 'NaT' or 'ignore'")
            self.df[defs.columns.DATETIME] = datetime_parser.merge_date_and_time(self.df[defs.columns.DATE], self.df[defs.columns.TIME])
            self.data_maps.append(DataMapping(orig_column_name=[defs.columns.DATE, defs.columns.TIME], new_column_name=defs.columns.DATETIME))
            if empty_time == "nat":
                self.df.loc[self.df[defs.columns.TIME] == "", defs.columns.DATETIME] = pd.NaT

        # Commenting this out. Trying to keep time column as local time to enable day vs. night analysis.
        # Date column is often in UTC but it's not easy to tell when that is the case nor what the local timezone is 
        # if UTC needs converted
        # We are assuming that the time column is already local
        # elif defs.columns.DATE in self.table and len(self.table[defs.columns.DATE].dt.time.unique()) > 3: 
        #     # Date column may be a datetime column. When the date has no time, the time is 00:00 which
        #     # can get converted to UTC. The offset at UTC can have up to 2 values due to daylight savings
        #     # time so the threshold is 3.
        #     self.table[defs.columns.DATETIME] = self.table[defs.columns.DATE]
        #     if not keeporig:
        #         self.table.drop(columns=[defs.columns.DATE], inplace=True)


    def standardize_gender(self):
        for col,mult in zip([defs.columns.GENDER_SUBJECT, defs.columns.GENDER_OFFICER, defs.columns.GENDER_OFFICER_SUBJECT], 
                            [self.mult_civilian, self.mult_officer, self.mult_both]):
            if col in self.col_map:
                std_map = {}
                self.df[col] = convert.convert(convert._create_gender_lut, self.df[self.col_map[col]], self.source_name, self.gender_cats, std_map=std_map, 
                                                    mult_type=mult.type, delim=mult.delim_gender, no_id=self.no_id, item_num=mult.item_gender)

                self.data_maps.append(DataMapping(orig_column_name=self.col_map.get_original(col), new_column_name=col, data_maps=std_map,
                                                orig_column=self.df[self.col_map[col]]))


    def standardize_agency(self):
        if defs.columns.AGENCY in self.col_map:
            self.df[defs.columns.AGENCY] = self.df[self.col_map[defs.columns.AGENCY]]
            self.data_maps.append(DataMapping(orig_column_name=self.col_map.get_original(defs.columns.AGENCY), new_column_name=defs.columns.AGENCY,
                                    orig_column=self.df[self.col_map[defs.columns.AGENCY]]))

    def cleanup(self):
        for v in self.col_map.values():
            self._cleanup_old_column(v)

        
    def sort_columns(self):
        # Reorder columns so standardized columns are first and any old columns are last
        old_cols = [x for x in self.df.columns if x.startswith(_OLD_COLUMN_INDICATOR)]
        reordered_cols = [x.new_column_name for x in self.data_maps if x.new_column_name in self.df.columns and x.new_column_name not in old_cols]
        reordered_cols.extend([x for x in self.df.columns if x not in old_cols and x not in reordered_cols])
        reordered_cols.extend([x for x in old_cols])
        self.df = self.df[reordered_cols]

    
    def standardize_off_or_civ(self):
        if defs.columns.SUBJECT_OR_OFFICER in self.col_map:
            col_name = defs.columns.SUBJECT_OR_OFFICER
            std_map = {}
            self.df[col_name] = convert.convert(convert.convert_off_or_civ, self.df[self.col_map[col_name]], self.source_name, 
                                                        std_map=std_map, no_id=self.no_id)

            self.data_maps.append(DataMapping(orig_column_name=self.col_map.get_original(col_name), new_column_name=col_name,
                                              data_maps=std_map,
                                              orig_column=self.df[self.col_map[col_name]]))


    def standardize_race(self):        
        for col,mult in zip([defs.columns.RACE_SUBJECT, defs.columns.RACE_OFFICER, defs.columns.RACE_OFFICER_SUBJECT], 
                            [self.mult_civilian, self.mult_officer, self.mult_both]):
            if col in self.col_map:
                race_column = self.col_map[col]
                race_map_dict = {}
                self.df[col] = convert.convert(convert._create_race_lut, self.df[race_column], self.source_name, 
                                                        std_map=race_map_dict, delim=mult.delim_race,
                                                        mult_type=mult.type, cats=self.race_cats, agg_cat=self.agg_race_cat, 
                                                        no_id=self.no_id, item_num=mult.item_race)

                self.data_maps.append(
                    DataMapping(orig_column_name=self.col_map.get_original(col), new_column_name=col,
                        data_maps=race_map_dict, orig_column=self.df[race_column])
                )

    
    def standardize_ethnicity(self):
        
        for col,mult in zip([defs.columns.ETHNICITY_SUBJECT, defs.columns.ETHNICITY_OFFICER, defs.columns.ETHNICITY_OFFICER_SUBJECT], 
                            [ self.mult_civilian,  self.mult_officer,  self.mult_both]):
            if col in self.col_map:
                eth_column = self.col_map[col]

                eth_map_dict = {}
                self.df[col] = convert.convert(convert._create_ethnicity_lut, self.df[eth_column], 
                                                self.source_name, self.eth_cats, std_map=eth_map_dict, 
                                                delim=mult.delim_race, mult_type=mult.type,
                                                no_id=self.no_id, item_num=mult.item_eth)

                self.data_maps.append(
                    DataMapping(orig_column_name=self.col_map.get_original(col), new_column_name=col,
                        data_maps=eth_map_dict, orig_column=self.df[eth_column])
                )

    
    def combine_race_ethnicity(self, combo_type):
        for eth_col,race_col_orig, race_col in zip([defs.columns.ETHNICITY_SUBJECT, defs.columns.ETHNICITY_OFFICER, defs.columns.ETHNICITY_OFFICER_SUBJECT], 
                        [defs.columns.RACE_ONLY_SUBJECT, defs.columns.RACE_ONLY_OFFICER, defs.columns.RACE_ONLY_OFFICER_SUBJECT], 
                        [defs.columns.RACE_SUBJECT, defs.columns.RACE_OFFICER, defs.columns.RACE_OFFICER_SUBJECT]):
            self._combine_race_ethnicity(race_col, eth_col, race_col_orig, combo_type)


    def _combine_race_ethnicity(self, race_col, eth_col, race_col_orig, type):
        type_vals = [False, "merge", "concat"]
        if type not in [False, "merge", "concat"]:
            raise ValueError(f"type must be one of the following values: {type_vals}")
        
        if not type or race_col not in self.df or eth_col not in self.df:
            return
        
        self.df[race_col_orig] = self.df[race_col]
        if type=="concat":
            def concat(x):
                if isinstance(x[race_col],dict):
                    return {k:f"{r} {e}" for k,r,e in zip(x[race_col].keys(), x[race_col].values(), x[eth_col].values())}
                else:
                    return f"{x[race_col]} {x[eth_col]}"
                
            f = concat
        elif type=="merge":
            if defs._eth_keys.NONLATINO not in self.eth_cats:
                raise KeyError(f"Unable to merge race and ethnicity columns without a value for self.eth_cats[{defs._eth_keys.NONLATINO}]")
            def merge(x):
                 if isinstance(x[race_col],dict) and isinstance(x[eth_col],dict):
                    return {k:(r if e==self.eth_cats[defs._eth_keys.NONLATINO] else e) for k,r,e in 
                            zip(x[race_col].keys(), x[race_col].values(), x[eth_col].values())}
                 elif isinstance(x[race_col],dict):
                    if isinstance(x[eth_col], str) and ("exempt" in x[eth_col].lower() or x[eth_col].lower()=="unspecified"):
                        return {k:
                                v+" - Ethnicity " +x[eth_col] if "exempt" not in v.lower() else v 
                                for k,v in x[race_col].items()
                                }
                    else:
                        raise NotImplementedError()
                 elif isinstance(x[eth_col],dict):
                     raise NotImplementedError()
                 else:
                    return x[race_col] if x[eth_col]==self.eth_cats[defs._eth_keys.NONLATINO] else x[eth_col]

            f = merge

        self.df[race_col] = self.df[[race_col, eth_col]].apply(f, axis=1)
        self.data_maps.append(
            DataMapping(orig_column_name=[race_col_orig, eth_col], new_column_name=race_col)
        )
        
    
    def check_for_multiple(self):        
        self._check_for_multiple(self.mult_civilian, defs.columns.RACE_SUBJECT, defs.columns.AGE_SUBJECT, 
                                 defs.columns.GENDER_SUBJECT, defs.columns.ETHNICITY_SUBJECT,
                                 defs.columns.AGE_RANGE_SUBJECT)
        self._check_for_multiple(self.mult_officer, defs.columns.RACE_OFFICER, defs.columns.AGE_OFFICER, 
                                 defs.columns.GENDER_OFFICER, defs.columns.ETHNICITY_OFFICER,
                                 defs.columns.AGE_RANGE_OFFICER)
        self._check_for_multiple(self.mult_both, defs.columns.RACE_OFFICER_SUBJECT, defs.columns.AGE_OFFICER_SUBJECT, 
                                 defs.columns.GENDER_OFFICER_SUBJECT, defs.columns.ETHNICITY_OFFICER_SUBJECT,
                                 defs.columns.AGE_RANGE_OFFICER_SUBJECT)
    

    def _check_for_multiple(self, mult_data, race_col, age_col, gender_col, eth_col, age_range_col):
        all_dict = True
        any_cols = False
        any_dict = False
        avail_cols = []
        for col in [race_col, age_col, gender_col, eth_col, age_range_col]:
            if col in self.col_map:
                avail_cols.append(self.col_map[col])
                any_cols = True
                is_dict = self.df[self.col_map[col]].apply(lambda x: isinstance(x,dict)).all()
                any_dict|=is_dict
                is_dict&=is_dict

        if any_cols and any_dict:
            if all_dict:
                logger.info("Demographics columns are all dictionaries where each entry corresponds to a separate person.")
                logger.info("Setting flag to handle this case.")
                mult_data.type = MultType.DICT

                # Ensure that all dictionaries have the same length. If not, add empty vals
                lens = self.df[avail_cols].apply(lambda x: [len(y) for y in x])
                max_counts = lens.max(axis=1)
                for col in avail_cols:
                    needs_append = lens.index[lens[col] < max_counts]
                    listcol = self.df[col].tolist()
                    for idx in needs_append:
                        val = self.df[col].loc[idx]
                        for k in range(lens[col].loc[idx], max_counts.loc[idx]):
                            val[k] = ""
                        
                        listcol[idx] = val
                    self.df[col] = listcol

                return
            else:
                raise NotImplementedError("One or more but not all demographics columns are dictionaries")
            
        if self.source_name=="Dallas" and self.table_type==defs.TableType.SHOOTINGS and "officer_s" in self.df.columns:
            logger.info("officer_s column has race and gender in it and will be parsed and split into standardized officer race and gender columns")
            mult_data.type = MultType.WITH_NAME
            mult_data.item_race = 0
            mult_data.item_gender = 1
            return

        if self.table_type not in [defs.TableType.SHOOTINGS, defs.TableType.USE_OF_FORCE, defs.TableType.COMPLAINTS,
                defs.TableType.SHOOTINGS_SUBJECTS, defs.TableType.USE_OF_FORCE_SUBJECTS,
                defs.TableType.SHOOTINGS_OFFICERS, defs.TableType.USE_OF_FORCE_OFFICERS, defs.TableType.USE_OF_FORCE_SUBJECTS_OFFICERS]:
            return
        
        # If only 1 of these columns, it will be handled by the standardize function
        if (race_col in self.col_map) + (age_col in self.col_map) + (gender_col in self.col_map) + (eth_col in self.col_map) > 1:
            cols_test = [race_col, age_col, gender_col]
            test = True
            found = 0
            for k in range(len(cols_test)):
                if cols_test[k] in self.col_map:
                    for m in range(k+1,len(cols_test)):
                        if cols_test[m] in self.col_map:
                            found+=1
                            test = test and self.col_map[cols_test[k]] == self.col_map[cols_test[m]]

            if test and found>0:
                vals = self.df[self.col_map[[x for x in cols_test if x in self.col_map][0]]].unique()
                delims = ["(", ","]
                found = False
                for x in vals:
                    if pd.isnull(x):
                        continue
                    items = [x]
                    for d in delims:
                        if d in x:
                            items = x.split(d)
                            break
                    for i in items:
                        if "," in i:
                            i = i.split(",")
                        elif "/" in i:
                            i = i.split("/")
                        race_item = None
                        gen_item = None
                        age_item = None
                        for k, value in enumerate(i):
                            value = value.strip().lower()
                            value = value[:-1] if len(value)>0 and value[-1]==")" else value
                            if value in ["black","white","asian","h","w","a","b","i"]:
                                race_item = k
                            elif value in ["male","female","m","f"]:
                                gen_item = k
                            elif value.isdigit():
                                age_item = k

                            if race_item is not None and gen_item is not None and (len(i)<3 or age_item is not None):
                                found = True
                                break
                        if found:
                            break      
                    if found:
                        break
                
                if not found:
                    raise ValueError("Unable to find pattern for demographics column")
                
                mult_data.item_race = race_item
                mult_data.item_gender = gen_item
                mult_data.item_age = age_item
                # This is a column with all demographics that will be handled by standardize function
                mult_data.type = MultType.DEMO_COL
                return
            else:
                max_count_race = 0
                max_count_eth = 0
                max_count_age = 0
                max_count_gender = 0
                has_exempt = False
                delim = None
                if age_col in self.col_map:
                    all_bad_age = self.df[self.col_map[age_col]].apply(lambda x: pd.isnull(x) or (isinstance(x,str) and "exempt" in x.lower())).all()
                    if not all_bad_age:  # Not all null or exempt
                        # Starting with age because it is least likely to have complicated values that could trick
                        # the delimiter search
                        num_age, mult_data.delim_age, max_count_age, is_exempt_age = _count_values(self.df[self.col_map[age_col]])
                        if is_exempt_age.any():
                            # It's possible that one of the columns has multiple values but this column has exemptions in the same place
                            has_exempt = True
                        elif max_count_age==0:
                            return
                        delim = mult_data.delim_age

                if race_col in self.col_map:
                    num_race, mult_data.delim_race, max_count_race, is_exempt_race = _count_values(self.df[self.col_map[race_col]], known_delim=delim)
                    has_any = max_count_race!=0
                    if has_any:
                        if age_col in self.col_map and not all_bad_age and max_count_age>0:
                            idx = num_race.notnull() & num_age.notnull()
                            # Check commonality between race and age values with multiple values
                            if (num_race[idx]!=num_age[idx]).mean()>0.1:
                                return                          
                    else:
                        if age_col in self.col_map:
                            if has_exempt and is_exempt_age.equals(is_exempt_race):
                                # It's possible that one of the columns has multiple values but this column has exemptions in the same place
                                pass
                            elif not all_bad_age and num_race[num_age>1].notnull().any():
                                return
                        elif is_exempt_race.any():
                            # It's possible that one of the columns has multiple values but this column has exemptions in the same place
                            has_exempt = True
                        else:
                            return

                    delim = mult_data.delim_race

                if eth_col in self.col_map:
                    num_eth, mult_data.delim_eth, max_count_eth, is_exempt_eth = _count_values(self.df[self.col_map[eth_col]], known_delim=delim)
                    has_any = max_count_eth!=0
                    if not has_any and num_eth[num_race>1].notnull().any():
                        return
                    if has_any and race_col in self.col_map and max_count_race>0:
                        idx = num_race.notnull() & num_eth.notnull()
                        if (num_race[idx]!=num_eth[idx]).any():
                            return
                        
                    delim = mult_data.delim_eth

                if gender_col in self.col_map:
                    num_gender, mult_data.delim_gender, max_count_gender, _ = _count_values(self.df[self.col_map[gender_col]])
                    if max_count_gender==0:
                        return
                    if race_col in self.col_map and max_count_race>0:
                        idx = num_race.notnull() & num_gender.notnull()
                        if (num_race[idx]!=num_gender[idx]).any():
                            # count = (num_race[idx]!=num_gender[idx]).sum()
                            # if count==1 and (max_count_race>3 or max_count_gender>3):
                            #     # One of the columns has fewer
                            #     idx = [k for k in range(len(num_race)) if pd.notnull(num_race[k]) and pd.notnull(num_gender[k]) and num_race[k]!=num_gender[k]][0]
                            #     col1 = self.col_map[race_col]
                            #     col2 = self.col_map[gender_col]
                            #     col1_val = self.df[self.col_map[race_col]].iloc[idx]
                            #     col2_val = self.df[self.col_map[gender_col]].iloc[idx]
                            #     print(f"Columns {col1} and {col2} contain demographics for multiple people per row and will be converted to columns "+
                            #         f"{race_col} and {gender_col}. However, the number of data in row {idx} is not consistent: "+
                            #         f"{col1_val} vs. {col2_val}")
                            if (num_race[idx]!=num_gender[idx]).mean() > 0.25:
                                return
                    if age_col in self.col_map and not all_bad_age and max_count_age>0:
                        idx = num_gender.notnull() & num_age.notnull()
                        if (num_age[idx]!=num_gender[idx]).mean()>0.1:
                            return
                    
                if int(max_count_race>0) + int(max_count_age>0) + int(max_count_gender>0) + int(max_count_eth>0) > 1:
                    logger.info("Demographics columns identified as contained delimited demographics data for multiple individuals. "+
                                "Resulting standardized demographics columns will be dictionaries.")
                    mult_data.type = MultType.DELIMITED
                elif int(max_count_race>0) + int(max_count_age>0) + int(max_count_gender>0) + int(max_count_eth>0) == 1 and has_exempt:
                    logger.info("Demographics columns identified as contained delimited demographics data for multiple individuals with Florida Marsy's Law Exemptions. "+
                                "Resulting standardized demographics columns will be dictionaries.")
                    mult_data.type = MultType.DELIMITED
        elif race_col in self.col_map:
            # Look for count followed by race            
            race_count_re = re.compile("\d+\s?-\s?[A-Za-z]+")
            if self.df[self.col_map[race_col]].apply(lambda x: race_count_re.search(x) is not None if pd.notnull(x) else False).any():
                mult_data.type = MultType.COUNTS
                return
            num_race, mult_data.delim_race, max_count_race, _ = _count_values(self.df[self.col_map[race_col]])
            if max_count_race>0:
                potential_mults = self.df[self.col_map[race_col]][num_race>1]
                # races = ["WHITE","BLACK","ASIAN"]  # Leaving out HISPANIC as items could be labeled race/ethnicity
                # r = ["W","B","H","A"]
                for x in potential_mults:
                    # Check for repeats
                    x_split = x.split(mult_data.delim_race)
                    if any([y>1 for y in Counter(x_split).values()]):
                        mult_data.type = MultType.DELIMITED
                        break
                    # else:
                    #     count = sum([y.upper() in races for y in x])
                    #     if count>0


    def standardize_age_range(self):
        for col_prop,mult in zip(["AGE_RANGE_SUBJECT", "AGE_RANGE_OFFICER", "AGE_RANGE_OFFICER_SUBJECT"],
                            [self.mult_civilian, self.mult_officer, self.mult_both]):
            col = getattr(defs.columns, col_prop)
            if col in self.col_map:
                map = {}
                self.df[col] = convert.convert(convert._create_age_range_lut, self.df[self.col_map[col]], source_name=self.source_name,
                                               std_map=map, delim=mult.delim_age, mult_type=mult.type, no_id=self.no_id)
                
                # Check for age ranges
                validator = re.compile(r'\d+-\d+')
                val = self.df[col].apply(lambda x: isinstance(x,str) and validator.search(x) is not None)
                if not val.any():
                    # Check if values are numbers (i.e. ages)
                    try:
                        new_col = pd.to_numeric(self.df[col], errors="raise", downcast="integer").astype("Int64")
                        if new_col.notnull().mean() > 0.5:
                            # This appears to be an age column
                            new_col_prop = col_prop.replace("_RANGE","")
                            if hasattr(defs.columns, new_col_prop):
                                new_col_name = getattr(defs.columns, new_col_prop)
                                self.col_map.replace(col, new_col_name)
                                self.df.rename(columns={col:new_col_name}, inplace=True)
                                col = new_col_name
                                map = None
                        else:
                            raise NotImplementedError()
                    except:
                        pass

                self.data_maps.append(DataMapping(orig_column_name=self.col_map.get_original(col), new_column_name=col,
                        orig_column=self.df[self.col_map[col]],
                        data_maps=map))


    def standardize_age(self):
        for col_prop, mult in zip(["AGE_SUBJECT", "AGE_OFFICER", "AGE_OFFICER_SUBJECT"],
                            [self.mult_civilian, self.mult_officer, self.mult_both]):
            if getattr(defs.columns,col_prop) in self.col_map:
                self._standardize_age(col_prop, mult)
        

    def _standardize_age(self, col_prop, mult_info):
        col_name = getattr(defs.columns,col_prop)
        max_age = 120  # Somewhat conservative max age of a human
        val_counts = self.df[self.col_map[col_name]].value_counts()
        vals = [x for x in val_counts.index if type(x)==str]
        if mult_info.type == MultType.DICT:
            def converter(x):
                out = x.copy()
                for k,v in x.items():
                    if pd.isnull(v) or (isinstance(v,str) and len(v)==0):
                        out[k] = np.nan
                    else:
                        out[k] = int(v)
                    
                return out

            self.df[col_name] = self.df[self.col_map[col_name]].apply(converter)
        elif mult_info.type == MultType.DEMO_COL:
            def extract_ages(x):
                if type(x) == str:
                    items = x.split("(")
                    result = {}
                    for k, i in enumerate(items[1:]):
                        val = i.split(",")[mult_info.item_age].strip()
                        if ")" in val:
                            val = val[:val.find(")")].strip()
                        if val=="":
                            result[k] = np.nan
                        else:
                            result[k] = int(val)

                    return result
                else:
                    return np.nan

            self.df[col_name] = self.df[self.col_map[col_name]].apply(extract_ages)
        elif mult_info.type == MultType.DELIMITED:
            # This column contains multiple ages in each cell
            new_ages = []
            num_na = 0
            num_ranges = 0
            multi_found = False
            for x in self.df[self.col_map[col_name]]:
                if pd.isnull(x):
                    new_ages.append(np.nan)
                    num_na+=1
                else:
                    cur_val = {}
                    contains_range = False
                    contains_na = False

                    loop_over = [x] if isinstance(x,Number) else enumerate(x.split(mult_info.delim_age))
                    for k,y in loop_over:
                        if k>0:
                            multi_found = True
                        if pd.notnull(y) and (isinstance(x,Number) or (y.strip().isdigit() and 0<int(y)<=max_age)):
                            cur_val[k] = int(y)
                        else:
                            if isinstance(y, str) and _p_age_range.search(y) and not contains_range:
                                contains_range = True
                                num_ranges+=1
                            elif not contains_range and not contains_na:
                                num_na+=1
                                contains_na = True
                                
                            cur_val[k] = np.nan

                    new_ages.append(cur_val)

            if not multi_found:
                # Convert from dicts to scalars
                new_ages = [x[0] if isinstance(x,dict) else x for x in new_ages]
            if "AGE" in col_name and (num_ranges / len(self.df)>0.6 or \
                (num_ranges/len(self.df) > 0.05 and num_ranges+num_na==len(self.df))):
                # This appears to be an age range column not an age column
                new_col_prop = col_prop.replace("AGE","AGE_RANGE")
                if hasattr(defs.columns, new_col_prop):
                    self.col_map.replace(col_name, getattr(defs.columns, new_col_prop))
                    return
            self.df[col_name] = new_ages
        else:
            try:
                col = pd.to_numeric(self.df[self.col_map[col_name]], errors="raise", downcast="integer")
            except Exception as e:
                # Not forcing all vals to have a match for _p_age_range because low ranges converted
                # to Month/Year at some point for at least 1 case (i.e. 10-17 to 17-Oct)
                if sum([val_counts[x] for x in vals if _p_age_range.search(x)!=None]) / len(self.df) > 0.6 or  \
                    all([x in ["UNKNOWN","ADULT","JUVENILE"] for x in vals]):
                    if "AGE" in col_name:
                        # This appears to be an age range column not an age column
                        new_col_prop = col_prop.replace("AGE","AGE_RANGE")
                        if hasattr(defs.columns, new_col_prop):
                            self.col_map.replace(col_name, getattr(defs.columns, new_col_prop))
                            return
                else:            
                    # Attempt to convert most values to numbers
                    col = pd.to_numeric(self.df[self.col_map[col_name]], errors="coerce", downcast="integer")
                    if pd.isnull(col).all():
                        logger.warn(f"Unable to convert column {self.col_map[col_name]} to an age")
                        self.col_map.pop(col_name)
                        return

                    test = [int(x)==y if ((isinstance(x,Number) and pd.notnull(x)) or (type(x)==str and x.strip().isdigit())) 
                            else False for x,y in zip(self.df[self.col_map[col_name]],col)]
                    sum_test = sum(test)
                    if not check_column(self.col_map[col_name], "age") and  sum_test / len(test) < 0.2:
                        logger.warn(f"Not converting {self.col_map[col_name]} to an age. If this is an age column only {sum(test) / len(test)*100:.1f}% of the data has a valid value")
                        self.col_map.pop(col_name)
                        return
                    elif sum_test / len(test) < 0.85 and not (sum_test>1 and len(test)-sum_test==1):
                        throw = True
                        if check_column(self.col_map[col_name], "age"):
                            # See if all values are null or acceptable values
                            badvals = 0                        
                            for x in self.df[self.col_map[col_name]].unique():
                                x = x.lower().strip() if isinstance(x,str) else x
                                if pd.isnull(x) or (isinstance(x,str) and x in ["", "unknown"]):
                                    continue
                                elif isinstance(x,Number) or  (isinstance(x,str) and x.isdigit()):
                                    x = int(x)
                                    if x<0 or x>max_age:
                                        badvals+=1
                                else:
                                    badvals+=1

                            if badvals<2 or badvals / len(self.df[self.col_map[col_name]].unique())<0.05:
                                throw = False
                        if throw:
                            raise e
                    
            min_val = col.min()
            if min_val < -1000 and (col==min_val).sum()==1:
                col[col==min_val] = np.nan

            if col.min() < 0 or col.max() > max_age:
                if not check_column(self.col_map[col_name], "age") and \
                    ((col<0) | (col>max_age)).sum()>1 and \
                    ((col<0) | (col>max_age)).mean()>0.01:
                    raise ValueError("Age is outside expected range. Double check that this is an age column")
                else:
                    # Some age columns have lots of bad data
                    col.loc[col > max_age] = np.nan
                    col.loc[col < 0] = np.nan

            col = col.round()
            col[col == 0] = np.nan

            # Int64 rather than int64 allows NaNs to be used
            self.df[col_name] = col.astype("Int64")

        self.data_maps.append(DataMapping(orig_column_name=self.col_map.get_original(col_name), new_column_name=col_name,
                                orig_column=self.df[self.col_map[col_name]]))
   

def _age_validator(df, cols_test, match_substr):
    match_cols = []
    for col_name in cols_test:
        # Only want to check cols_test that contain age
        for s in match_substr:
            if s.lower()=="age":
                pass
            elif s.lower() in col_name.lower():
                match_cols.append(col_name)
                continue

        if check_column(col_name, "age"):
            # Check for unambiguous column name
            match_cols.append(col_name)
            continue

        # Split into words based on white space and underscore. Then look for word "age"
        words = re.split(r"[\W^_]+", col_name)
        if any([x.lower()=="age" for x in words]):
            match_cols.append(col_name)
            continue

        for w in words:
            # Split into words based on Camel Case. Then look for word "age"
            if w.isupper():
                # Not camel case. Already checked above.
                continue
            elif any([x.lower()=="age" for x in camel_case_split(w)]):
                match_cols.append(col_name)
                break

    return match_cols
    

def _race_validator(df, cols_test, source_name, mult_data=_MultData()):
    search_all = df.columns.equals(cols_test)
    match_cols = []
    for col_name in cols_test:
        if check_column(col_name, "race"):
            match_cols.append(col_name)
            continue
        
        # Anything checked beyond this point is less likely to be a race column
        col = df[col_name]
        try:
            if "address" in col_name.lower() or \
                (search_all and pd.api.types.is_numeric_dtype(col.dtype)):
                # Addresses are complicated and could trigger false alarms
                # Don't search numeric codes when searching all columns. Will lead to false matches
                continue

            race_cats = defs.get_race_cats()
            # Function for validating a column is a race column
            col = convert.convert(convert._create_race_lut, col, source_name, cats=race_cats, 
                                  delim=mult_data.delim_race, mult_type=mult_data.type, item_num=mult_data.item_race)
            counts = col.value_counts()

            total = 0
            white_or_black_found = False
            used = [False for _ in counts.index]
            for k in [defs._race_keys.AAPI, defs._race_keys.ASIAN, defs._race_keys.WHITE, defs._race_keys.BLACK, defs._race_keys.LATINO]:
                matches = [race_cats[k] in x if isinstance(x,list) else x==race_cats[k] for x in counts.index]
                if any(matches):
                    for j, m in enumerate(matches):
                        if m and not used[j]:
                            total+=counts.iloc[j]
                            used[j] = True
                    if k in [defs._race_keys.WHITE, defs._race_keys.BLACK]:
                        white_or_black_found = True

            if not white_or_black_found or total / len(col) < 1/3:
                continue
            
            if total!=len(col):
                # A and B could be codes for something else instead of values for Asian and Black
                # Check if all converted values are Asian and Black and the rest are not known race values
                for k in race_cats.values():
                    if k not in [race_cats[defs._race_keys.ASIAN],race_cats[defs._race_keys.BLACK],
                                 race_cats[defs._race_keys.OTHER],race_cats[defs._race_keys.UNSPECIFIED]] and \
                        k in counts.index:
                        break
                else:     
                    continue

                if len(cols_test) > 5: # Most likely we are searching all columns
                    matches = (df[col_name] == col)
                    all_races = [x for x in race_cats.values() if x not in [race_cats[defs._race_keys.UNSPECIFIED],race_cats[defs._race_keys.UNKNOWN],
                                                                            race_cats[defs._race_keys.MULTIPLE],race_cats[defs._race_keys.OTHER]]]
                    knowns = df[col_name].isin(all_races)
                    unknowns = col.isin([race_cats[defs._race_keys.UNSPECIFIED],race_cats[defs._race_keys.UNKNOWN]]) & matches
                    if (matches.sum() - knowns.sum() - unknowns.sum()) / (len(matches) - unknowns.sum()) > 0.1:
                        continue
            elif len(cols_test) > 5 and df[col_name].isin(["A","B"]).all():
                # Most likely we are searching all columns in the table and this a column that uses
                # letters to indicate something other than race
                continue

            if col.apply(lambda x: isinstance(x,list)).mean() > 0.95 and \
                col.apply(lambda x: any([isinstance(y,Number) or (isinstance(y,str) and y.isdigit()) for y in x]) 
                          if isinstance(x,list) else False).mean() > 0.95:
                # Date columns can result in lists that are mostly of numbers
                continue

            match_cols.append(col_name)
        except Exception as e:
            pass

    return match_cols


def _eth_validator(df, cols_test):
    match_cols = []
    ferndale_eth_vals = ['FRENCH/GERMAN', 'MEXICAN', 'HUNGARIAN', 'LEBANESE', 'POLISH/SCOTTISH', 'IRISH', 'SYRIAN', 'POLISH']
    for col_name in cols_test:
        col = df[col_name]
        try:
            for x in col.unique():
                if isinstance(x,str):
                    x = x.upper().strip()
                    if x in ["N","H","NHIS","HIS","LAT"] or "LATINO" in x or "HISPANIC" in x or x in ferndale_eth_vals:
                        found = True
                        break
            else:
                found = False

            if found:
                match_cols.append(col_name)
        except Exception as e:
            pass

    return match_cols


def _gender_validator(df, match_cols_test, source_name):
    match_cols = []
    for col_name in match_cols_test:
        if check_column(col_name, ["gender", "sex"]):
            # Check for unambiguous column names
            match_cols.append(col_name)
            continue
        
        try:
            col = df[col_name]
            # Verify gender column
            gender_cats = defs.get_gender_cats()
            col = convert.convert(convert._create_gender_lut, col, source_name, gender_cats)

            counts = col.value_counts()

            total = 0
            for k, v in gender_cats.items():
                if k in [defs._gender_keys.UNKNOWN, defs._gender_keys.OTHER]:
                    continue
                if v in counts.index:
                    total+=counts[v]

            if total / len(col) < 0.5:
                continue

            match_cols.append(col_name)
        except:
            pass
    
    return match_cols
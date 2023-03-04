from datetime import datetime
import logging
import numbers
import pandas as pd
import re
from collections import Counter, defaultdict
import numpy as np
from numbers import Number

try:
    from . import datetime_parser
    from . import defs
except:
    import defs
    import datetime_parser

_skip_tables = ["calls for service"]

# Age range XX - YY
_p_age_range = re.compile(r"""
    ^                   # Start of string
    (\d+)               # Lower bound of age range. Capture for reuse.
    \s?(-|_|TO)-?\s?          # - or _ between lower and upper bound with optional spaces 
    (\d+)               # Upper bound of age range. Capture for reuse.
    (\s?[-_]\s?\1\s?[-_]\s?\3)?  # Optional mistaken repeat of same pattern. 
    $                   # End of string
    """, re.VERBOSE)

class DataMapping:
    def __init__(self, orig_column_name=None, new_column_name = None, data_maps=None, orig_column=None):
        self.orig_column_name = orig_column_name
        self.new_column_name = new_column_name
        self.data_maps = data_maps
        self.orig_value_counts = orig_column.value_counts().head() if orig_column is not None else None

    def __repr__(self) -> str:
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
                        kother = np.nan
                    else:
                        kother = k
                    if self.data_maps[k] != other.data_maps[kother]:
                        return False
                
                tf_data_maps = True

        tf_vals = self.orig_value_counts is None and other.orig_value_counts is None
        if not tf_vals:
            tf_vals = self.orig_value_counts.equals(other.orig_value_counts)
            if not tf_vals and other.orig_value_counts.index.dtype=="int64" and all([x.isdigit() for x in self.orig_value_counts.index if isinstance(x,str)]):
                # Found a case where indices were numeric strings but read back in as numbers
                other.orig_value_counts.index = [str(x) for x in other.orig_value_counts.index]
                tf_vals = self.orig_value_counts.equals(other.orig_value_counts)


        tf = self.orig_column_name == other.orig_column_name and \
            self.new_column_name == other.new_column_name and \
            tf_data_maps and tf_vals

        return tf


def standardize(df, table_type, year, 
    known_cols={}, 
    source_name=None, 
    keep_raw=True,
    agg_race_cat=False,
    race_cats=defs.get_race_cats(),
    eth_cats=defs.get_eth_cats(),
    gender_cats=defs.get_gender_cats(),
    verbose=False,
    no_id="pass",
    race_eth_combo="merge",
    merge_date_time=True,
    empty_time="NaT"): 

    if table_type.lower() in _skip_tables:
        print(f"Standardization is not currently applied to {table_type} table.")
        return df, None
    
    if verbose:
        logging.getLogger("opd").debug(f"Original columns:\n{df.columns}")
    
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

    if verbose:
        # Print column changes and then full maps
        logging.getLogger("opd").debug(f"Identified columns:")
        for map in std.data_maps:
            logging.getLogger("opd").debug(f"\t{map.orig_column_name}: {map.new_column_name}")

        for map in std.data_maps:
            logging.getLogger("opd").debug(map)
            logging.getLogger("opd").debug("\n")
    
    return std.df, std.data_maps

def _count_values(col, known_delim=None):
    if known_delim is None:
        delims = [",", "|", ";", "/"]
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
    max_num_vals[col == "Marsy’s Law Exempt"] = np.nan

    return max_num_vals, delim, max_count

class _MultData:
    is_mult = False
    delim_race = None
    delim_age = None
    delim_gender = None
    delim_eth = None
    has_demo_col = False


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
        col_names = ['tcole_race_ethnicity']
        types = types[0:1]
    elif len(col_names)==2 and source_name in ["Los Angeles"] and any([x.endswith("_cd") for x in col_names]):
        # The one with cd is a coded-version of the other
        keep = [k for k,x in enumerate(col_names) if not x.endswith("_cd")]
        col_names = col_names[keep[0]:keep[0]+1]
        types = types[keep[0]:keep[0]+1]
    else:
        orig_race_cols = col_names
        orig_type = types
        col_names = []
        types = []
        vals = []
        for k, col in enumerate(orig_race_cols):
            new_col = convert_race(df[col], source_name)
            
            found = False
            for j, t in enumerate(types):
                if t == orig_type[k] and new_col.equals(vals[j]):
                    found = True
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

        self.col_map = {}
        self.data_maps = []

        self.mult_civilian = _MultData()
        self.mult_officer = _MultData()
        self.mult_both = _MultData()

    def _pattern_search(self, select_cols, patterns, match_substr):
        matches = []
        for p in patterns:
            if p[0].lower() == "equals":
                matches = [x for x in select_cols if x.lower() == p[1].lower()]
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

            if len(matches) > 0:
                break

        return matches


    def _remove_excluded(self, match_cols, exclude_col_names, match_substr):
        for e in exclude_col_names:
            if type(e) == tuple:
                match_cols = self._pattern_search(match_cols, [e], match_substr[0])
            elif e in match_cols:
                match_cols.remove(e)

        return match_cols


    def _find_col_matches(self, match_substr, 
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
        search_data=False):

        if self.table_type in exclude_table_types or \
            (only_table_types != None and self.table_type not in only_table_types):
            if std_col_name in self.df.columns:
                return [std_col_name]
            else:
                return []

        officer_terms = ["officer","deputy"]
        if known_col_name != None:
            if known_col_name not in self.df.columns:
                raise ValueError(f"Known column {known_col_name} is not in the DataFrame")
            return [known_col_name]
        else:
            if isinstance(match_substr, str):
                match_substr = [match_substr]

            match_cols = []
            civilian_terms = ["citizen","subject","suspect","civilian"]
            civilian_found = False
            officer_found = False
            for s in match_substr:
                new_matches = [x for x in self.df.columns if s.lower() in x.lower()]
                new_matches = self._remove_excluded(new_matches, exclude_col_names, match_substr)
                if len(new_matches)>0:
                    if not officer_found and not civilian_found:
                        match_cols.extend(new_matches)
                    elif officer_found:
                        # Only keep columns with civilian terms
                        match_cols.extend([x for x in new_matches if any([y in x for y in civilian_terms])])
                    elif civilian_found:
                        # Only keep columns with officer terms
                        match_cols.extend([x for x in new_matches if any([y in x for y in officer_terms])])

                    all_raw = all([x.startswith("raw_") for x in match_cols])

                    # There are cases where there should be multiple matches for both officer and community member
                    # columns. On occasion, they are labeled with different terms 
                    officer_found = any([any([y in x for y in officer_terms]) for x in match_cols])
                    civilian_found = any([any([y in x for y in civilian_terms]) for x in match_cols])

                    if civilian_found == officer_found and not all_raw:  # civilian_found == officer_found: Both found or not found
                        break

            if len(match_cols)==0 and len(match_substr)>0:
                match_cols = self._pattern_search(self.df.columns, secondary_patterns, match_substr[0])
                match_cols = self._remove_excluded(match_cols, exclude_col_names, match_substr)
                
                
            if len(match_cols)==0:
                if not_required_table_types != "ALL" and self.table_type not in not_required_table_types:
                    raise ValueError(f"Column not found with substring {match_substr}")
            elif len(match_cols)>1:
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

            if (multi_check and validator != None and (len(match_cols) > 1 or always_validate)) or \
                (search_data and len(match_cols)==0):
                if search_data and len(match_cols)==0:
                    # Search everything
                    match_cols_test = self.df.columns
                else:
                    match_cols_test = match_cols

                match_cols = validator(self.df, match_cols_test, *validate_args)
                
            match_cols = self._remove_excluded(match_cols, exclude_col_names, match_substr)

            return match_cols


    def id_columns(self):
        # Find the date columns
        match_cols = self._find_col_matches("date", known_col_name=self.known_cols[defs.columns.DATE], 
            std_col_name=defs.columns.DATE,
            secondary_patterns = [("equals","date"),("contains","time"), ("contains", "cad"),  ("contains", "assigned"), ("contains", "occurred")],
            not_required_table_types=[defs.TableType.USE_OF_FORCE_CIVILIANS_OFFICERS, defs.TableType.USE_OF_FORCE_CIVILIANS, 
                defs.TableType.USE_OF_FORCE_OFFICERS, defs.TableType.SHOOTINGS_CIVILIANS, defs.TableType.SHOOTINGS_OFFICERS,
                defs.TableType.TRAFFIC_ARRESTS],
            # Calls for services often has multiple date/times with descriptive names for what it corresponds to.
            # Don't generalize by standardizing
            exclude_table_types=[defs.TableType.EMPLOYEE, defs.TableType.CALLS_FOR_SERVICE], 
            validator=datetime_parser.validate_date)

        if len(match_cols) > 1:
            raise NotImplementedError()
        elif len(match_cols) == 1:
            self.col_map[defs.columns.DATE] = match_cols[0]
        
        secondary_patterns = []
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
                segs = self.col_map[defs.columns.DATE].split("_")
                if len(segs)>1:
                    date_str = defs.columns.DATE.lower()
                    pattern = ""
                    date_found = False
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
                            pattern+="_"

                    secondary_patterns = [("format", pattern)]

            exclude_col_names = ["rankattimeofincident"]
            validator_args = []
            if defs.columns.DATE in self.col_map:
                # Don't select the date column as the time column too
                exclude_col_names.append(self.col_map[defs.columns.DATE])
                validator_args.append(self.df[self.col_map[defs.columns.DATE]])
            
            match_cols = self._find_col_matches("time", 
                std_col_name=defs.columns.TIME,
                secondary_patterns=secondary_patterns, validator=datetime_parser.validate_time,
                validate_args=validator_args,
                exclude_col_names=exclude_col_names,
                exclude_table_types=[defs.TableType.CALLS_FOR_SERVICE],
                always_validate=True)

            if len(match_cols) > 1:
                raise NotImplementedError()
            elif len(match_cols) == 1:
                # Found time column
                self.col_map[defs.columns.TIME] = match_cols[0]
            
        # Incidents tables shouldn't have demographics data
        if self.table_type not in [defs.TableType.SHOOTINGS_INCIDENTS, defs.TableType.USE_OF_FORCE_INCIDENTS]:    
            def role_validator(df, match_cols_test):
                match_cols = []
                for col_name in match_cols_test:
                    try:
                        col = df[col_name]
                        # Function for validating a column indicates whether the person described is a civilian or officer
                        new_col = convert_off_or_civ(col)
                        vals = new_col.unique()

                        if defs._roles.CIVILIAN not in vals and defs._roles.OFFICER not in vals:
                            continue

                        if defs._roles.CIVILIAN not in vals or defs._roles.OFFICER not in vals:
                            # California data, RAE = Race and Ethnicity
                            # Check if it's possible this does not indicate civilian vs officer
                            match_cols = self._find_col_matches(["race", "descent","rae_full","citizen_demographics","officer_demographics","ethnicity"],
                                validator=_race_validator, validate_args=[self.source_name])

                            off_type = False
                            civ_type = False
                            is_officer_table = self.table_type == defs.TableType.EMPLOYEE.value or \
                                ("- OFFICERS" in self.table_type and "CIVILIANS" not in self.table_type)
                            for k in range(len(match_cols)):
                                if "off" in match_cols[k].lower() or "deputy" in match_cols[k].lower() or \
                                    (is_officer_table and "suspect" not in match_cols[k].lower() and "supsect" not in match_cols[k].lower()):
                                    off_type = True
                                else:
                                    civ_type = True
                                
                            if civ_type and off_type:
                                continue

                        match_cols.append(col_name)
                    except:
                        pass

                return match_cols

            match_cols = self._find_col_matches(["Civilian_Officer","ROLE"], 
                only_table_types = [defs.TableType.USE_OF_FORCE, defs.TableType.USE_OF_FORCE_CIVILIANS_OFFICERS, defs.TableType.SHOOTINGS],
                validator=role_validator,
                always_validate=True)
            if len(match_cols) > 1:
                raise NotImplementedError()
            elif len(match_cols) == 1:
                self.col_map[defs.columns.CIVILIAN_OR_OFFICER] = match_cols[0]

            match_cols = self._find_col_matches(["race", "citizen_demographics","officer_demographics"],
                validator=_race_validator, 
                validate_args=[self.source_name],
                search_data=True)  
            # TODO: Remove failures when column are not found
            race_cols, race_types = self._id_demographic_column(match_cols,
                defs.columns.RACE_CIVILIAN, defs.columns.RACE_OFFICER,
                defs.columns.RACE_OFF_AND_CIV,
                tables_to_exclude=[
                    ("Montgomery County", defs.TableType.COMPLAINTS),
                    ("Albany", defs.TableType.COMPLAINTS),
                    ("Denver", defs.TableType.STOPS),
                    ("Lincoln", defs.TableType.VEHICLE_PURSUITS),
                    ("Los Angeles", defs.TableType.STOPS),
                    ("South Bend", defs.TableType.USE_OF_FORCE),
                    ("South Bend", defs.TableType.COMPLAINTS),
                    ("State Police", defs.TableType.SHOOTINGS),
                    ("Menlo Park",defs.TableType.STOPS),
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
                    ],
                specific_cases=[_case("California", defs.TableType.USE_OF_FORCE_CIVILIANS_OFFICERS, "Race_Ethnic_Group", defs.columns.RACE_OFF_AND_CIV),
                                _case("Minneapolis", defs.TableType.STOPS, "race", defs.columns.RACE_CIVILIAN),
                                _case("Fairfax County", defs.TableType.ARRESTS, ["ArresteeRa","OfficerRac"], [defs.columns.RACE_CIVILIAN, defs.columns.RACE_OFFICER], year=range(2016,2021)),
                                _case("Lansing", defs.TableType.SHOOTINGS, ["Race_Sex","Officer"], [defs.columns.RACE_CIVILIAN, defs.columns.RACE_OFFICER])
                    ],
                adv_type_match=_find_race_col_type_advanced)

            # enthnicity is to deal with typo in Ferndale data. Consider using fuzzywuzzy in future for fuzzy matching
            match_cols = self._find_col_matches(["ethnic", "enthnicity"], exclude_col_names=race_cols)
            self._id_ethnicity_column(race_types, match_cols, race_cols,
                specific_cases=[
                    _case("Fairfax County", defs.TableType.ARRESTS, "ArresteeEt", defs.columns.ETHNICITY_CIVILIAN, year=range(2016,2021))
                ])

            # Do not want the result to contain the word agency
            match_substr=["age","citizen_demographics","officer_demographics"]
            match_cols = self._find_col_matches(match_substr, 
                exclude_col_names=[("does not contain",["group","range","at_hire"])],
                validator=_age_validator,
                validate_args=[match_substr],
                always_validate=True)
            self._id_demographic_column(match_cols, 
                defs.columns.AGE_CIVILIAN, defs.columns.AGE_OFFICER,
                defs.columns.AGE_OFF_AND_CIV,
                required=False,
                specific_cases=[_case("Norman", defs.TableType.COMPLAINTS, "Age", defs.columns.AGE_OFFICER, year=[2016,2017,2018]),
                                _case("Norman", defs.TableType.USE_OF_FORCE, "Age", defs.columns.AGE_OFFICER, year=[2016,2017]),
                                _case("Fairfax County", defs.TableType.ARRESTS, "ArresteeAg", defs.columns.AGE_CIVILIAN, year=range(2016,2021))]
                )

            match_cols = self._find_col_matches(["agerange","age_range","agegroup","age_group"])
            self._id_demographic_column(match_cols,
                defs.columns.AGE_RANGE_CIVILIAN, defs.columns.AGE_RANGE_OFFICER,
                defs.columns.AGE_RANGE_OFF_AND_CIV,
                required=False)
            
            match_cols = self._find_col_matches(["gend", "sex","citizen_demographics","officer_demographics"],
                validator=_gender_validator, validate_args=[self.source_name])        
            self._id_demographic_column(match_cols,
                defs.columns.GENDER_CIVILIAN, defs.columns.GENDER_OFFICER, 
                defs.columns.GENDER_OFF_AND_CIV,
                required=False,
                specific_cases=[_case("California", defs.TableType.STOPS, "G_FULL", defs.columns.GENDER_CIVILIAN),
                                _case("Lansing", defs.TableType.SHOOTINGS, ["Race_Sex","Officer"], [defs.columns.GENDER_CIVILIAN, defs.columns.GENDER_OFFICER]),
                                _case("Fairfax County", defs.TableType.ARRESTS, ["ArresteeSe","OfficerSex"], [defs.columns.GENDER_CIVILIAN, defs.columns.GENDER_OFFICER], year=range(2016,2021))
                    ],
                adv_type_match=_find_gender_col_type_advanced)

        match_cols = self._find_col_matches([], known_col_name=self.known_cols[defs.columns.AGENCY])
        if len(match_cols) > 1:
            raise NotImplementedError()
        elif len(match_cols) == 1:
            self.col_map[defs.columns.AGENCY] = match_cols[0]

        for key, value in self.col_map.items():
            if key == value:
                # Need to rename original column
                self.col_map[key] = self._cleanup_old_column(value, keep_raw=True)

    
    def _id_ethnicity_column(self, race_types, eth_cols, race_cols, specific_cases=[]):
        for c in specific_cases:
            if c.equals(self.source_name, self.table_type, self.year):
                c.update_map(self.col_map)
                return

        if len(eth_cols)==0:
            return

        eth_types = []
        validation_types = []
        if defs.columns.CIVILIAN_OR_OFFICER in self.col_map:
            if len(eth_cols) > 1:
                raise NotImplementedError()

            eth_types.append(defs.columns.ETHNICITY_OFF_AND_CIV)
            validation_types.append(defs.columns.RACE_OFF_AND_CIV)
        else:
            is_officer_table = self.table_type == defs.TableType.EMPLOYEE.value or \
                    ("- OFFICERS" in self.table_type and "CIVILIANS" not in self.table_type)
            for k in range(len(eth_cols)):
                if "officer" in eth_cols[k].lower() or \
                    "offcr" in eth_cols[k].lower() or is_officer_table or \
                    (self.source_name=="Orlando" and self.table_type==defs.TableType.SHOOTINGS and eth_cols[k]=="ethnicity"):
                    eth_types.append(defs.columns.ETHNICITY_OFFICER)
                    validation_types.append(defs.columns.RACE_OFFICER)
                else:
                    eth_types.append(defs.columns.ETHNICITY_CIVILIAN)
                    validation_types.append(defs.columns.RACE_CIVILIAN)

        if len(set(eth_types)) != len(eth_types) and all([x==defs.columns.ETHNICITY_CIVILIAN for x in eth_types]):
            # See if we can split the column names and find PO for police officer
            eth_types = [defs.columns.ETHNICITY_OFFICER if "PO" in re.split(r"[\W^_]+", x.upper()) else defs.columns.ETHNICITY_CIVILIAN for x in eth_cols]
            validation_types = [defs.columns.RACE_OFFICER if "PO" in re.split(r"[\W^_]+", x.upper()) else defs.columns.RACE_CIVILIAN for x in eth_cols]

        if len(set(eth_types)) != len(eth_types):
            raise NotImplementedError()

        if any([x not in race_types for x in validation_types]):
            raise NotImplementedError()

        for k in range(len(eth_cols)):
            if eth_types[k] == defs.columns.ETHNICITY_CIVILIAN and \
                "subject_race" in race_cols and eth_cols[k].startswith("raw"):
                # This is a raw column from Stanford data that has already been standardized into subject_race
                continue
            self.col_map[eth_types[k]] = eth_cols[k]

    
    def _id_demographic_column(self, col_names, 
        civilian_col_name, officer_col_name, civ_officer_col_name,
        required=True,
        tables_to_exclude=[],
        sources_to_exclude=[],
        specific_cases=[],
        adv_type_match=None):

        for c in specific_cases:
            if c.equals(self.source_name, self.table_type, self.year):
                c.update_map(self.col_map)
                return col_names, []

        if len(col_names) == 0:
            if not required or \
                self.table_type == defs.TableType.USE_OF_FORCE_INCIDENTS or \
                self.table_type == defs.TableType.SHOOTINGS_INCIDENTS or \
                self.table_type == defs.TableType.CALLS_FOR_SERVICE or \
                self.table_type == defs.TableType.CRASHES or \
                self.table_type == defs.TableType.INCIDENTS or \
                self.source_name in sources_to_exclude or \
                any([(self.source_name, self.table_type)==x for x in tables_to_exclude]):
                return col_names, []
            else:
                raise NotImplementedError()

        if defs.columns.CIVILIAN_OR_OFFICER in self.col_map:
            if len(col_names) > 1:
                raise NotImplementedError()

            self.col_map[civ_officer_col_name] = col_names[0]
            return col_names, [civ_officer_col_name]
        else:     
            is_officer_table = self.table_type == defs.TableType.EMPLOYEE.value or \
                ("- OFFICERS" in self.table_type and "CIVILIANS" not in self.table_type)

            types = []
            for k in range(len(col_names)):
                if "off" in col_names[k].lower() or "deputy" in col_names[k].lower() or \
                    "employee" in col_names[k].lower() or "ofcr" in col_names[k].lower() or \
                    (is_officer_table and "suspect" not in col_names[k].lower() and "supsect" not in col_names[k].lower()):
                    types.append(officer_col_name)
                else:
                    types.append(civilian_col_name)

            if len(set(types)) != len(types):
                if is_officer_table:
                    # Check if one of the columns is clearly an officer column
                    is_def_off = []
                    for k in range(len(col_names)):
                        if "off" in col_names[k].lower() or "deputy" in col_names[k].lower() or \
                            "employee" in col_names[k].lower() or "ofcr" in col_names[k].lower():
                            is_def_off.append(True)
                        else:
                            is_def_off.append(False)
                    if any(is_def_off):
                        for k in reversed(range(len(col_names))):
                            if types[k]==officer_col_name and not is_def_off[k]:
                                col_names.pop(k)
                                types.pop(k)

                if len(set(types)) != len(types) and all([x==civilian_col_name for x in types]):
                    # See if we can split the column names and find PO for police officer
                    types = [officer_col_name if "PO" in re.split(r"[\W^_]+", x.upper()) else civilian_col_name for x in col_names]
                if len(set(types)) != len(types) and adv_type_match != None:
                    col_names, types = adv_type_match(self.df, self.source_name, col_names, types, self.col_map, civilian_col_name, officer_col_name)

                if len(set(types)) != len(types):
                    raise NotImplementedError()

            for k in range(len(col_names)):
                self.col_map[types[k]] = col_names[k]

        return col_names, types
    
    def _cleanup_old_column(self, col_name, keep_raw=None):
        keep_raw = self.keep_raw if keep_raw is None else keep_raw
        if keep_raw:
            new_name = "RAW_"+col_name
            self.df.rename(columns={col_name : new_name}, inplace=True)
            return new_name
        else:
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
                        date_data["day"] = [1 for _unused in range(len(self.df))]
                                    
            s_date = datetime_parser.parse_date_to_datetime(date_data)
            # s_date.name = defs.columns.DATE
            self.df[defs.columns.DATE] = s_date
            # self.df = pd.concat([self.df, s_date], axis=1)

            self.data_maps.append(DataMapping(orig_column_name=self.col_map[defs.columns.DATE], new_column_name=defs.columns.DATE,
                                              orig_column=self.df[self.col_map[defs.columns.DATE]]))


    def standardize_time(self):
        if defs.columns.TIME in self.col_map:
            self.df[defs.columns.TIME] = datetime_parser.parse_time(self.df[self.col_map[defs.columns.TIME]])

            self.data_maps.append(DataMapping(orig_column_name=self.col_map[defs.columns.TIME], new_column_name=defs.columns.TIME,
                                              orig_column=self.df[self.col_map[defs.columns.TIME]]))
            
    
    def merge_date_time(self, empty_time="NaT"):
        if defs.columns.DATE in self.df and defs.columns.TIME in self.df:
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
        for col,mult in zip([defs.columns.GENDER_CIVILIAN, defs.columns.GENDER_OFFICER, defs.columns.GENDER_OFF_AND_CIV], 
                            [self.mult_civilian, self.mult_officer, self.mult_both]):
            if col in self.col_map:
                self._standardize_gender(col, mult_info=mult)


    def _standardize_gender(self, col_name, mult_info):
        std_map = {}
        self.df[col_name] = _convert_gender(self.df[self.col_map[col_name]], self.source_name, self.gender_cats, std_map=std_map, mult_info=mult_info, no_id=self.no_id)

        self.data_maps.append(DataMapping(orig_column_name=self.col_map[col_name], new_column_name=col_name, data_maps=std_map,
                                          orig_column=self.df[self.col_map[col_name]]))
        

    def standardize_agency(self):
        if defs.columns.AGENCY in self.col_map:
            self.df[defs.columns.AGENCY] = self.df[self.col_map[defs.columns.AGENCY]]
            self.data_maps.append(DataMapping(orig_column_name=self.col_map[defs.columns.AGENCY], new_column_name=defs.columns.AGENCY,
                                    orig_column=self.df[self.col_map[defs.columns.AGENCY]]))

    def cleanup(self):
        for v in self.col_map.values():
            self._cleanup_old_column(v)

        
    def sort_columns(self):
        # Reorder columns so standardized columns are first and any old columns are last
        reordered_cols = [x.new_column_name for x in self.data_maps if x.new_column_name in self.df.columns]
        old_cols = [x for x in self.df.columns if x.startswith("RAW")]
        reordered_cols.extend([x for x in self.df.columns if x not in old_cols and x not in reordered_cols])
        reordered_cols.extend([x for x in old_cols])
        self.df = self.df[reordered_cols]

    
    def standardize_off_or_civ(self):
        if defs.columns.CIVILIAN_OR_OFFICER in self.col_map:
            col_name = defs.columns.CIVILIAN_OR_OFFICER
            std_map = {}
            self.df[col_name] = convert_off_or_civ(self.df[self.col_map[col_name]], std_map)

            self.data_maps.append(DataMapping(orig_column_name=self.col_map[col_name], new_column_name=col_name,
                                              data_maps=std_map,
                                              orig_column=self.df[self.col_map[col_name]]))


    def standardize_race(self):        
        for col,mult in zip([defs.columns.RACE_CIVILIAN, defs.columns.RACE_OFFICER, defs.columns.RACE_OFF_AND_CIV], 
                            [self.mult_civilian, self.mult_officer, self.mult_both]):
            if col in self.col_map:
                self._standardize_race(new_race_column=col, mult_info=mult)

    
    def standardize_ethnicity(self):
        
        for col,mult in zip([defs.columns.ETHNICITY_CIVILIAN, defs.columns.ETHNICITY_OFFICER, defs.columns.ETHNICITY_OFF_AND_CIV], 
                            [ self.mult_civilian,  self.mult_officer,  self.mult_both]):
            if col in self.col_map:
                self._standardize_eth(col, mult_info=mult)

    
    def combine_race_ethnicity(self, combo_type):
        for eth_col,race_col_orig, race_col in zip([defs.columns.ETHNICITY_CIVILIAN, defs.columns.ETHNICITY_OFFICER, defs.columns.ETHNICITY_OFF_AND_CIV], 
                        [defs.columns.RACE_ONLY_CIVILIAN, defs.columns.RACE_ONLY_OFFICER, defs.columns.RACE_ONLY_OFF_AND_CIV], 
                        [defs.columns.RACE_CIVILIAN, defs.columns.RACE_OFFICER, defs.columns.RACE_OFF_AND_CIV]):
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
                if isinstance(x,list):
                    return [f"{y} {z}" for y,z in zip(x[race_col], x[eth_col])]
                else:
                    return f"{x[race_col]} {x[eth_col]}"
                
            f = concat
        elif type=="merge":
            if defs._eth_keys.NONLATINO not in self.eth_cats:
                raise KeyError(f"Unable to merge race and ethnicity columns without a value for self.eth_cats[{defs._eth_keys.NONLATINO}]")
            def merge(x):
                 if isinstance(x,list):
                    return [r if e==self.eth_cats[defs._eth_keys.NONLATINO] else e for r,e in zip(x[race_col], x[eth_col])]
                 else:
                    return x[race_col] if x[eth_col]==self.eth_cats[defs._eth_keys.NONLATINO] else x[eth_col]

            f = merge

        self.df[race_col] = self.df[[race_col, eth_col]].apply(f, axis=1)
        self.data_maps.append(
            DataMapping(orig_column_name=[race_col_orig, eth_col], new_column_name=race_col)
        )
    

    def _standardize_eth(self, new_col, mult_info):
        eth_column = self.col_map[new_col]

        eth_map_dict = {}
        self.df[new_col] = convert_ethnicity(self.df[eth_column], self.source_name, eth_map_dict=eth_map_dict, mult_info=mult_info,
                 eth_cats=self.eth_cats, no_id=self.no_id)

        # col.name = new_col
        # # Concatentate to avoid fragmentation performance warnings
        # self.df = pd.concat([self.df, col], axis=1)

        self.data_maps.append(
            DataMapping(orig_column_name=eth_column, new_column_name=new_col,
                data_maps=eth_map_dict, orig_column=self.df[eth_column])
        )
        
        # if new_ethnicity_column in self.col_map and new_race_column in self.col_map and \
        #     self.col_map[new_ethnicity_column] != self.col_map[new_race_column]:
        #     ethnicity_column = self.col_map[new_ethnicity_column]
        # else:
        #     ethnicity_column = None

        # if ethnicity_column != None:      
        #     vals = [x if type(x)==str else x for x in self.df[ethnicity_column].unique()]

        #     eth_map_dict = {}
        #     if mult_info.is_mult:
        #         for k in range(len(self.df)):
        #             x = self.df[ethnicity_column][k]
        #             y = self.df[new_race_column][k]
        #             if type(x) == str:
        #                 items = x.split(mult_info.delim_eth)
        #                 if len(y) == len(items):
        #                     for m in range(len(y)):
        #                         i = items[m].strip()
        #                         if i not in eth_map_dict:
        #                             _create_ethnicity_lut(i, eth_map_dict, self.source_name)
        #                         if i in eth_map_dict:
        #                             self.df[new_race_column][k][m] = eth_map_dict[i]
        #     else:
        #         for x in vals:
        #             _create_ethnicity_lut(x, eth_map_dict, self.source_name)

        #         def update_race(x):
        #             if x[ethnicity_column] in eth_map_dict:
        #                 return eth_map_dict[x[ethnicity_column]]
        #             else:
        #                 return x[new_race_column]

        #         self.df[new_race_column] = self.df.apply(update_race, axis=1)

        #     self.data_maps[-1].old_column_name = [self.data_maps[-1].old_column_name, ethnicity_column]
        #     self.data_maps[-1].data_maps = [self.data_maps[-1].data_maps, eth_map_dict]


    def _standardize_race(self, new_race_column, mult_info):
        race_column = self.col_map[new_race_column]

        race_map_dict = {}
        self.df[new_race_column] = convert_race(self.df[race_column], self.source_name, race_map_dict=race_map_dict, mult_info=mult_info,
                 race_cats=self.race_cats, agg_cat=self.agg_race_cat, no_id=self.no_id)

        # col.name = new_race_column
        # # Concatentate to avoid fragmentation performance warnings
        # self.df = pd.concat([self.df, col], axis=1)

        self.data_maps.append(
            DataMapping(orig_column_name=race_column, new_column_name=new_race_column,
                data_maps=race_map_dict, orig_column=self.df[race_column])
        )

    
    def check_for_multiple(self):
        if self.table_type in [defs.TableType.SHOOTINGS, defs.TableType.USE_OF_FORCE, defs.TableType.COMPLAINTS,
                        defs.TableType.SHOOTINGS_CIVILIANS, defs.TableType.USE_OF_FORCE_CIVILIANS,
                        defs.TableType.SHOOTINGS_OFFICERS, defs.TableType.USE_OF_FORCE_OFFICERS, defs.TableType.USE_OF_FORCE_CIVILIANS_OFFICERS]:
            self._check_for_multiple(self.mult_civilian, defs.columns.RACE_CIVILIAN, defs.columns.AGE_CIVILIAN, defs.columns.GENDER_CIVILIAN, defs.columns.ETHNICITY_CIVILIAN)
            self._check_for_multiple(self.mult_officer, defs.columns.RACE_OFFICER, defs.columns.AGE_OFFICER, defs.columns.GENDER_OFFICER, defs.columns.ETHNICITY_OFFICER)
            self._check_for_multiple(self.mult_both, defs.columns.RACE_OFF_AND_CIV, defs.columns.AGE_OFF_AND_CIV, defs.columns.GENDER_OFF_AND_CIV, defs.columns.ETHNICITY_OFF_AND_CIV)
    

    def _check_for_multiple(self, mult_data, race_col, age_col, gender_col, eth_col):
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
                # This is a column with all demographics that will be handled by standardize function
                mult_data.has_demo_col = True
                return
            else:
                max_count_race = 0
                max_count_eth = 0
                max_count_age = 0
                max_count_gender = 0
                delim = None
                if age_col in self.col_map:
                    # Starting with age because it is least likely to have complicated values that could trick
                    # the delimiter search
                    num_age, mult_data.delim_age, max_count_age = _count_values(self.df[self.col_map[age_col]])
                    if max_count_age==0:
                        return
                    delim = mult_data.delim_age

                if race_col in self.col_map:
                    num_race, mult_data.delim_race, max_count_race = _count_values(self.df[self.col_map[race_col]], known_delim=delim)
                    has_any = max_count_race!=0
                    if not has_any and age_col in self.col_map and num_race[num_age>1].notnull().any():
                        return
                    if has_any and age_col in self.col_map and max_count_age>0:
                        idx = num_race.notnull() & num_age.notnull()
                        if (num_race[idx]!=num_age[idx]).any():
                            return
                    delim = mult_data.delim_race

                if eth_col in self.col_map:
                    num_eth, mult_data.delim_eth, max_count_eth = _count_values(self.df[self.col_map[eth_col]], known_delim=delim)
                    has_any = max_count_eth!=0
                    if not has_any and num_eth[num_race>1].notnull().any():
                        return
                    if has_any and race_col in self.col_map and max_count_race>0:
                        idx = num_race.notnull() & num_eth.notnull()
                        if (num_race[idx]!=num_eth[idx]).any():
                            return
                        
                    delim = mult_data.delim_eth

                if gender_col in self.col_map:
                    num_gender, mult_data.delim_gender, max_count_gender = _count_values(self.df[self.col_map[gender_col]])
                    if max_count_gender==0:
                        return
                    if race_col in self.col_map and max_count_race>0:
                        idx = num_race.notnull() & num_gender.notnull()
                        if (num_race[idx]!=num_gender[idx]).any():
                            count = (num_race[idx]!=num_gender[idx]).sum()
                            if count==1 and (max_count_race>3 or max_count_gender>3):
                                # One of the columns has fewer
                                idx = [k for k in range(len(num_race)) if pd.notnull(num_race[k]) and pd.notnull(num_gender[k]) and num_race[k]!=num_gender[k]][0]
                                col1 = self.col_map[race_col]
                                col2 = self.col_map[gender_col]
                                col1_val = self.df[self.col_map[race_col]].iloc[idx]
                                col2_val = self.df[self.col_map[gender_col]].iloc[idx]
                                print(f"Columns {col1} and {col2} contain demographics for multiple people per row and will be converted to columns "+
                                    f"{race_col} and {gender_col}. However, the number of data in row {idx} is not consistent: "+
                                    f"{col1_val} vs. {col2_val}")
                            else:
                                return
                    if age_col in self.col_map and max_count_age>0:
                        idx = num_gender.notnull() & num_age.notnull()
                        if (num_gender[idx]!=num_age[idx]).any():
                            return
                    
                if int(max_count_race>0) + int(max_count_age>0) + int(max_count_gender>0) + int(max_count_eth>0) > 1:
                    mult_data.is_mult = True
        elif race_col in self.col_map:
            num_race, mult_data.delim_race, max_count_race = _count_values(self.df[self.col_map[race_col]])
            if max_count_race>0:
                potential_mults = self.df[self.col_map[race_col]][num_race>1]
                # races = ["WHITE","BLACK","ASIAN"]  # Leaving out HISPANIC as items could be labeled race/ethnicity
                # r = ["W","B","H","A"]
                for x in potential_mults:
                    # Check for repeats
                    x_split = x.split(mult_data.delim_race)
                    if any([y>1 for y in Counter(x_split).values()]):
                        mult_data.is_mult = True
                        break
                    # else:
                    #     count = sum([y.upper() in races for y in x])
                    #     if count>0


    def standardize_age_range(self):
        for col in [defs.columns.AGE_RANGE_CIVILIAN, defs.columns.AGE_RANGE_OFFICER, defs.columns.AGE_RANGE_OFF_AND_CIV]:
            if col in self.col_map:
                self._standardize_age_range(col, self.no_id)


    def standardize_age(self):
        for col, mult in zip([defs.columns.AGE_CIVILIAN, defs.columns.AGE_OFFICER, defs.columns.AGE_OFF_AND_CIV],
                            [self.mult_civilian, self.mult_officer, self.mult_both]):
            if col in self.col_map:
                self._standardize_age(col, mult)
        

    def _standardize_age_range(self, col_name, no_id):
        no_id = no_id.lower()
        if no_id not in ["pass", "null", "error", "test"]:
            raise ValueError(f"no_id is {no_id}. It should be 'pass', 'null', or 'error'.")
        
        vals = self.df[self.col_map[col_name]].unique()
        map = {}

        p_plus = re.compile(r"(\d+)\+",re.IGNORECASE)
        p_over = re.compile(r"(OVER|>)\s?(\d+)",re.IGNORECASE)
        p_under = re.compile(r"(UNDER|<)\s?(\d+)",re.IGNORECASE)
        p_above = re.compile(r"(\d+) AND ABOVE",re.IGNORECASE)
        p_above = re.compile(r"(\d+) AND ABOVE",re.IGNORECASE)
        p_decade = re.compile(r"^(\d+)s$", re.IGNORECASE)
        for orig in vals:
            v = orig
            if type(v)==str:
                v = v.upper().strip()
            if type(v) == str and _p_age_range.search(v)!=None:
                map[orig] = _p_age_range.sub(r"\1-\3", v)
            elif type(v) == str and p_over.search(v)!=None:
                map[orig] = p_over.sub(r"\2-120", v)
            elif type(v) == str and p_plus.search(v)!=None:
                map[orig] = p_plus.sub(r"\1-120", v)
            elif type(v) == str and p_above.search(v)!=None:
                map[orig] = p_above.sub(r"\1-120", v)
            elif type(v) == str and p_under.search(v)!=None:
                map[orig] = p_under.sub(r"0-\2", v)
            elif type(v) == str and p_decade.search(v)!=None:
                decade = int(p_decade.search(v).group(1))
                map[orig] = f"{decade}-{decade+9}"
            elif pd.isnull(v) or v in ["","NR","UNKNOWN","-","N/A"] or "NO DATA" in v or v.replace(" ","")=="":
                map[orig] = ""
            elif self.source_name=="Cincinnati" and v=="ADULT":
                map[orig] = "18-120"
            elif self.source_name=="Cincinnati" and v=="JUVENILE":
                map[orig] = "0-17"
            elif v.isdigit():
                map[orig] = orig
            else:
                # At least 1 case was found where age range was auto-corrected to a month/year like
                # 10-17 to 17-Oct. Check for this before giving up.
                try:
                    map[orig] = datetime.strftime(datetime.strptime(v, "%d-%b"),"%m-%d")
                except:
                    if no_id=="pass" or (no_id=="test" and v in ["#VALUE!", "NOT AVAILABLE", "OTHER"]):
                        map[orig] = orig
                    elif no_id=="error":
                        raise TypeError(f"Unknown val {v} for age range")
                    else:
                        # no_id == "null"
                        map[orig] = ""

        col = self.df[self.col_map[col_name]].map(map)
        col.name = col_name
        if col_name in self.df.columns:
            self.df.drop(columns=col_name, inplace=True)
        # Concatentate to avoid fragmentation performance warnings
        self.df = pd.concat([self.df, col], axis=1)

        self.data_maps.append(DataMapping(orig_column_name=self.col_map[col_name], new_column_name=col_name,
                                orig_column=self.df[self.col_map[col_name]],
                                data_maps=map))


    def _standardize_age(self, col_name, mult_info):
        max_age = 120  # Somewhat conservative max age of a human
        val_counts = self.df[self.col_map[col_name]].value_counts()
        vals = [x for x in val_counts.index if type(x)==str]
        if mult_info.has_demo_col:
            def extract_ages(x):
                if type(x) == str:
                    items = x.split("(")
                    result = []
                    for i in items[1:]:
                        val = i.split(",")[2].strip()
                        if ")" in val:
                            val = val[:val.find(")")].strip()
                        if val=="":
                            result.append(np.nan)
                        else:
                            result.append(int(val))

                    return result
                else:
                    return np.nan

            self.df[col_name] = self.df[self.col_map[col_name]].apply(extract_ages)
        elif mult_info.is_mult:
            # This column contains multiple ages in each cell
            def convert_to_age_list(x, delim):
                if pd.isnull(x):
                    return np.nan
                else:
                    return [int(y) if (pd.notnull(y) and y.strip().isdigit() and 0<int(y)<=max_age) else np.nan for y in x.split(delim)]

            self.df[col_name] = self.df[self.col_map[col_name]].apply(convert_to_age_list, delim=mult_info.delim_age)
        else:
            try:
                col = pd.to_numeric(self.df[self.col_map[col_name]], errors="raise", downcast="integer")
            except Exception as e:
                # Not forcing all vals to have a match for _p_age_range because low ranges converted
                # to Month/Year at some point for at least 1 case (i.e. 10-17 to 17-Oct)
                if sum([val_counts[x] for x in vals if _p_age_range.search(x)!=None]) / len(self.df) > 0.6 or  \
                    all([x in ["UNKNOWN","ADULT","JUVENILE"] for x in vals]):
                    if "AGE" in col_name:
                        new_col_name = col_name.replace("AGE","AGE_RANGE")
                        if hasattr(defs.columns, new_col_name):
                            self.col_map[new_col_name] = self.col_map.pop(col_name)
                            return
                else:            
                    # Attempt to convert most values to numbers
                    col = pd.to_numeric(self.df[self.col_map[col_name]], errors="coerce", downcast="integer")
                    if pd.isnull(col).all():
                        logging.getLogger("opd").warn(f"Unable to convert column {self.col_map[col_name]} to an age")
                        self.col_map.pop(col_name)
                        return

                    test = [int(x)==y if ((isinstance(x,Number) and pd.notnull(x)) or (type(x)==str and x.strip().isdigit())) 
                            else False for x,y in zip(self.df[self.col_map[col_name]],col)]
                    sum_test = sum(test)
                    if sum_test / len(test) < 0.2:
                        logging.getLogger("opd").warn(f"Not converting {self.col_map[col_name]} to an age. If this is an age column only {sum(test) / len(test)*100:.1f}% of the data has a valid value")
                        self.col_map.pop(col_name)
                        return
                    elif sum_test / len(test) < 0.85 and not (sum_test>1 and len(test)-sum_test==1):
                        raise e
                    
            min_val = col.min()
            if min_val < -1000 and (col==min_val).sum()==1:
                col[col==min_val] = np.nan

            if col.min() < 0 or col.max() > max_age:
                # Parse the column name to be certain this is an age column
                name_parts = self.col_map[col_name].lower().split("_")
                name_parts = [x.replace("subject","").strip() for x in name_parts]
                if "age" not in name_parts:
                    raise ValueError("Age is outside expected range. Double check that this is an age column")
                else:
                    # Some age columns have lots of bad data
                    col.loc[col > max_age] = np.nan
                    col.loc[col < 0] = np.nan

            col = col.round()
            col[col == 0] = np.nan

            # Int64 rather than int64 allows NaNs to be used
            self.df[col_name] = col.astype("Int64")

        self.data_maps.append(DataMapping(orig_column_name=self.col_map[col_name], new_column_name=col_name,
                                orig_column=self.df[self.col_map[col_name]]))

    
class _case:
    def __init__(self, src, table_type, old_name, new_name, year=None):
        self.src = src
        self.table_type = table_type
        self.year = year
        self.old_name = old_name if type(old_name)==list else [old_name]
        self.new_name = new_name if type(new_name)==list else [new_name]

    def equals(self, src, table_type, year):
        tf = src==self.src and table_type==self.table_type
        if self.year!=None:
            if isinstance(self.year, numbers.Number):
                tf = tf and (year==self.year)
            else:
                tf = tf and (year in self.year)

        return tf

    def update_map(self, col_map):
        for k in range(len(self.old_name)):
            col_map[self.new_name[k]] = self.old_name[k]


def _convert_gender(col, source_name, gender_cats, std_map={}, mult_info=_MultData(), no_id="error"):
    vals = col.unique()
    if mult_info.has_demo_col:
        map = {}
        delims = ["(", ","]
        for x in vals:
            if type(x) == str:
                items = [x]
                for d in delims:
                    if d in x:
                        items = x.split(d)
                        break
                map_list = []
                if len(items[0])==0:
                    istart = 1
                else:
                    istart = 0
                for k, i in enumerate(items[istart:]):
                    if "," in i:
                        i = i.split(",")[1].strip()
                    elif "/" in i:
                        i = i.split("/")[1].strip()
                    elif i != "Unk":
                        raise NotImplementedError()
                    if i not in std_map:
                        std_map[i] = _create_gender_lut(i, gender_cats, source_name, no_id)
                    map_list.append(std_map[i])

                map[x] = map_list
            else:
                if x not in std_map:
                    std_map[x] = _create_gender_lut(x, gender_cats, source_name, no_id)
                    map[x] = std_map[x]

        new_col = col.map(map)
    elif mult_info.is_mult:
        map = {}
        for x in vals:
            if type(x) == str:
                items = x.split(mult_info.delim_gender)
                for k, i in enumerate(items):
                    i = i.strip()
                    if i not in std_map:
                        std_map[i] = _create_gender_lut(i, gender_cats, source_name, no_id)
                    items[k] = std_map[i]

                map[x] = items
            else:
                if x not in std_map:
                    std_map[x] = _create_gender_lut(x, gender_cats, source_name, no_id)
                    map[x] = std_map[x]

        def apply_map(x):
            if x in map:
                val = map[x]
                if isinstance(val, list):
                    val = val.copy()
                return val
            else:
                return x
        new_col = col.apply(apply_map)
    else:
        for x in vals:
            std_map[x] = _create_gender_lut(x, gender_cats, source_name, no_id)
        new_col = col.map(std_map)

    return new_col

def _create_gender_lut(x, gender_cats, source_name, no_id):
    no_id = no_id.lower()
    if no_id not in ["pass", "null", "error", "test"]:
        raise ValueError(f"no_id is {no_id}. It should be 'pass', 'null', or 'error'.")
    
    bad_data = ["BLACK","WHITE"]

    orig = x
    if pd.notnull(x) and (type(x) != str or x.isdigit()) and source_name in ["California", "Lincoln"]:
        default_cats = defs.get_gender_cats()
        if source_name == "California":
            # California stops data has a dictionary that can be applied. 
            # https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2023-01/RIPA%20Dataset%20Read%20Me%202021%20Final%20rev%20011223.pdf
            map_dict = {
                1 : default_cats[defs._gender_keys.MALE], 2 : default_cats[defs._gender_keys.FEMALE], 3 : default_cats[defs._gender_keys.TRANSGENDER_MALE], 
                4 : default_cats[defs._gender_keys.TRANSGENDER_FEMALE], 5 : default_cats[defs._gender_keys.GENDER_NONCONFORMING]
            }
        elif source_name == "Lincoln":
            # Lincoln data has a dictionary that can be applied.
            # https://ago-item-storage.s3.us-east-1.amazonaws.com/df19cb240d984564965969c568b855c6/LPD_code_tables.pdf?X-Amz-Security-Token=IQoJb3JpZ2luX2VjEFQaCXVzLWVhc3QtMSJHMEUCIQDzuBic65OoJxPSf0DueLmXhFj5uFdAe98fepapTY091AIgLoXQrkkBdRbDjEuwvyGQjpJJLUFgi3ygI8BtjfWuT%2BAq1QQI3f%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARAAGgw2MDQ3NTgxMDI2NjUiDMxtFJ8yzxImKjHOuSqpBGW%2FA1aH%2F%2FKSYJB1edlTnRu3JE4fzkqCbDBWZrWyKxGmnGWPqnY0hIhhMNS4GSpuwkDUkZANDRh1DUPqVlDZtVfez2tNZwJuCuisqAyofRFAYx%2FQNn18FsJcnXyOZ4hbnduu78jGF4yJU1faAxd1hGrWaKC4NWpYsXxupkdHT5ZDf9SuVGMz8I5L1hB3QLK8cjnxfhZbsXtU6EgBx14jyOdLzrzHBwOFzAkpz7SQJZofNtpRVyRY26xh8RSNL%2Fj1d9LcxjgxchTbiKedc7NZMfoXBWPdb8z%2F3RJMx%2FHVrc6a4NCGc8toWHypuH7lMg1JGxouo9cAc7JxkQrhF9ruPVU54y6EQcxw%2B1JjNfsezqNnk1WLKJ38FJ2zFstJFMfQWNTAqqLk63VjQ2TnnNModiOxlfWLJLfoLSviSqbCVwyQJDzQR6cplKGyICLbIasFPkk%2Fihh1xdH2HeC153E8stesc9FaX5yN09zSYP61TuNOXQicSD9uCAMPEuClxUc0D17iuC%2F9Kxs8Hs1G%2B5C6kD4FBEPYHlAQE4t4nN%2Fehnv7POOgQpYPVo%2Bi%2FI8RRG%2BLPaVI5PQP7ANBQ5ljqqpaIV%2BN0wvt9BdkYi1fsOEsTgg9N2IuN8EdfAXFTFeT%2FmXAZvfEFocvdQqPsV9cXY2cPE3ryEAH0KHzBo6%2Bpfnsay9H2RE23pW%2FvfuAND%2BktpTy0HBzogihKOGs7h4SJBvxUJjTDDBB5agDS6Mwu4SlnwY6qQHNEZx4%2Fa3r4RWoqz%2F%2FC9hkEGXmQNLZZB%2BlcGoE9iVTNLaqK0Wr6oKdgC1xwm5Hq0aUnatarAR5zL8TsG4JkzaCDCggx8Q36CzDsC0sjx1yaEKb4UC6g2MOpY%2Bc0NrIbL%2BQkByyqyreP6797Rx1lHrIvWOrlXaqoB4lMz3Kxxkqe20CCVE5UkTLCAC1X%2Fe5IqLIOzmgigv8tD5vLDE6FaCq3GXMUA%2BUifE3&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230212T204254Z&X-Amz-SignedHeaders=host&X-Amz-Expires=300&X-Amz-Credential=ASIAYZTTEKKEYIVNVE33%2F20230212%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=da74601aabea63766444b4260baaaec0697753cf9cddf8d70b37204b5126687b
            map_dict = {
                 1 : default_cats[defs._gender_keys.MALE], 2 : default_cats[defs._gender_keys.FEMALE]
            }

        x = int(x)
        if x not in map_dict:
            if no_id=="error":
                raise KeyError(f"Unknown gender value for {source_name} data: {orig}")
            else:
                return orig if no_id=="pass" else ""
        else:
            # Replace numerical code with default value
            x = map_dict[x]


    if type(x) == str:
        x = x.upper().replace("-","").replace("_","").replace(" ","").replace("'","")

        if source_name in ["New York City"]:
            # Handling dataset-specific codes
            if source_name == "New York City":
                # https://www.nyc.gov/assets/nypd/downloads/zip/analysis_and_planning/stop-question-frisk/SQF-File-Documentation.zip
                map_dict = {"Z":"Unknown"}

            if x.upper() in map_dict:
                x = map_dict[x.upper()].upper()

    if pd.notnull(x):
        # Check if value is part of gender_cats
        # This is particularly necessary for custom gender_cats dicts
        for k,v in gender_cats.items():
            # Key is always be a string
            if k.upper()==str(orig).upper() or k.upper()==str(x) or str(v)==str(orig).upper() or str(v)==str(x):
                return v

    has_unspecified = defs._gender_keys.UNSPECIFIED in gender_cats
    if pd.isnull(x):
        if has_unspecified:
            return gender_cats[defs._gender_keys.UNSPECIFIED]
        elif no_id=="error":
            raise ValueError(f"Null value found in gender column: {orig}")
        else:
            # Same result whether no_id is pass or null
            return ""
    if has_unspecified and x in ["MISSING", "UNSPECIFIED", "",",",'NOTSPECIFIED',"NOTRECORDED","NONE"] or \
        "NODATA" in x or "NOSEX" in x or "NULL" in x:
        return gender_cats[defs._gender_keys.UNSPECIFIED]
    elif defs._gender_keys.FEMALE in gender_cats and x in ["F", "FEMALE", "FEMAALE", "FFEMALE", "FEMAL"]:
        return gender_cats[defs._gender_keys.FEMALE]
    elif defs._gender_keys.MALE in gender_cats and x in ["M", "MALE", "MMALE"]:
        return gender_cats[defs._gender_keys.MALE]
    elif defs._gender_keys.OTHER in gender_cats and x in ["OTHER", "O"]:
        return gender_cats[defs._gender_keys.OTHER]
    elif defs._gender_keys.TRANSGENDER in gender_cats and x in ["TRANSGENDER"]:
        return gender_cats[defs._gender_keys.TRANSGENDER]
    elif defs._gender_keys.GENDER_NONBINARY in gender_cats and x in ["NONBINARY"]:
        return gender_cats[defs._gender_keys.GENDER_NONBINARY]
    elif defs._gender_keys.TRANSGENDER_OR_GENDER_NONCONFORMING in gender_cats and x in [
        "Gender Diverse (gender non-conforming and/or transgender)".upper().replace("-","").replace("_","").replace(" ",""),
        "GENDERNONCONFORMING"]:
        return gender_cats[defs._gender_keys.TRANSGENDER_OR_GENDER_NONCONFORMING]
    elif defs._gender_keys.TRANSGENDER_MALE in gender_cats and (x in ["TRANSGENDER MALE".replace(" ","")] or \
        "TRANSGENDERMAN" in x or \
        (source_name=="Los Angeles" and x=="T")):
        return gender_cats[defs._gender_keys.TRANSGENDER_MALE]
    elif defs._gender_keys.TRANSGENDER_FEMALE in gender_cats and \
        (x in ["TRANSGENDER FEMALE".replace(" ","")] or \
        "TRANSGENDERWOMAN" in x or \
        (source_name=="Los Angeles" and x=="W")):
        return gender_cats[defs._gender_keys.TRANSGENDER_FEMALE]
    elif defs._gender_keys.GENDER_NONCONFORMING in gender_cats and (source_name=="Los Angeles" and x=="C"):
        return gender_cats[defs._gender_keys.GENDER_NONCONFORMING]
    elif defs._gender_keys.UNKNOWN in gender_cats and \
        (x in ["U","UK", "UNABLE TO DETERMINE".replace(" ","")] or "UNK" in x):
        return gender_cats[defs._gender_keys.UNKNOWN]
    
    if no_id=="test":
        if "EXEMPT" in x or x in ["DATAPENDING", "NOTAPPLICABLE","N/A"] or ("BUSINESS" in x and source_name=="Cincinnati"):
            return orig
        elif x in bad_data or \
            (source_name=="New York City" and (x=="Z" or x.isdigit())) or \
            (source_name=="Baltimore" and x in ["Y","Z"]) or \
            (source_name in ["Seattle","New Orleans","Menlo Park"] and x in ["D","N"]) or \
            (source_name=="Los Angeles County" and x=="0") or \
            (x=="W" and source_name=="Cincinnati") or \
            (x=="P" and source_name=="Fayetteville") or \
            (x=="5" and source_name=="Lincoln") or \
            x in ["UNDISCLOSE","UNDISCLOSED","PREFER TO SELF DESCRIBE".replace(" ",""),'NONBINARY/THIRDGENDER',
                  "PREFER NOT TO SAY".replace(" ","")] or \
            source_name == "Buffalo":
            return orig
        else:
            raise ValueError(f"Unknown value in gender column: {orig}")
    if no_id=="error":
        raise ValueError(f"Unknown value in gender column: {orig}")
    else:
        return orig if no_id=="pass" else ""


def convert_off_or_civ(col, std_map=None):
    vals = col.unique()
    if std_map == None:
        std_map = {}

    for x in vals:
        orig = x
        if type(x) == str:
            x = x.upper()

        if pd.isnull(x) or x in ["MISSING"]:
            std_map[orig] = defs._roles.UNSPECIFIED
        elif x in ["OFFICER"]:
            std_map[orig] = defs._roles.OFFICER
        elif x in ["SUBJECT","CIVILIAN"]:
            std_map[orig] = defs._roles.CIVILIAN
        else:
            raise ValueError(f"Unknown person type {orig}")

    return col.map(std_map)

def _create_ethnicity_lut(x, source_name, eth_cats, no_id):
    no_id = no_id.lower()
    if no_id not in ["pass", "null", "error","test"]:
        raise ValueError(f"no_id is {no_id}. It should be 'pass', 'null', or 'error'.")
    # The below values is used in the Ferndale demographics data. Just use the data from the race column in that
    # case which includes if officer is Hispanic
    ferndale_eth_vals = ['NR', 'FRENCH/GERMAN', 'MEXICAN', 'HUNGARIAN', 'LEBANESE', 'POLISH/SCOTTISH', 'IRISH', 'SYRIAN', 'POLISH']
    orig = x
    if type(x) == str:
        x = x.upper().replace("-","").replace(" ", "")

    has_unspecified = defs._eth_keys.UNSPECIFIED in eth_cats
    has_nonlat = defs._eth_keys.NONLATINO in eth_cats
    has_latino = defs._eth_keys.LATINO in eth_cats

    if pd.notnull(x):
        # Check if value is part of eth_cats
        # This is particularly necessary for custom eth_cats dicts
        for k,v in eth_cats.items():
            # Key is always be a string
            if k.upper()==str(orig).upper() or k.upper()==str(x) or str(v)==str(orig).upper() or str(v)==str(x):
                return v
        
    if pd.isnull(x):
        if has_unspecified:
            return eth_cats[defs._eth_keys.UNSPECIFIED]
        elif no_id=="error":
            raise ValueError(f"Null value found in ethnicity column: {orig}")
        else:
            # Same result whether no_id is pass or null
            return ""
        
    if has_nonlat and (x in ["N", "NH"] or "NOTHISP" in x or "NONHISP" in x or "NONLATINO" in x):
        return eth_cats[defs._eth_keys.NONLATINO]
    if has_latino and (x == "H" or "HISPANIC" in x or "LATINO" in x):
        return eth_cats[defs._eth_keys.LATINO]
    if defs._eth_keys.UNKNOWN in eth_cats and x in ["U", "UNKNOWN"]:
        return eth_cats[defs._eth_keys.UNKNOWN]
    if defs._eth_keys.UNSPECIFIED in eth_cats and ("NODATA" in x or x in ["MISSING", "", "NOTREPORTED",'NOTAPPICABLE(NONINDIVIDUAL)']):
        return eth_cats[defs._eth_keys.UNSPECIFIED]
    if defs._eth_keys.MIDDLE_EASTERN and x in ["M"] and source_name == "Connecticut":
        return eth_cats[defs._eth_keys.MIDDLE_EASTERN]
    if no_id=="test":
        if "EXEMPT" in x:
            return orig
        elif x in ["W","A"] or x in ferndale_eth_vals:
            return eth_cats[defs._eth_keys.NONLATINO]
    elif no_id=="error":
        raise ValueError(f"Unknown value in ethnicity column: {orig}")
    else:
        return orig if no_id=="pass" else ""

    
def convert_ethnicity(col, source_name, eth_map_dict=None, mult_info=_MultData(),
                 eth_cats=defs.get_eth_cats(), no_id="pass"):
    if eth_map_dict == None:
        eth_map_dict = {}
    # Decode the race column
    val_counts = col.value_counts()
    vals = [x if type(x)==str else x  for x in val_counts.index]
    if mult_info.has_demo_col:  # Check if column is a demographics column containin a list of all demographics (i.e. race, gender, age)
        map = {}
        delims = ["(", ","]
        for x in vals:
            if type(x) == str:
                items = [x]
                for d in delims:
                    if d in x:
                        items = x.split(d)
                        break
                
                map_list = []
                if len(items[0])==0:
                    istart = 1
                else:
                    istart = 0
                for k, i in enumerate(items[istart:]):
                    if "," in i:
                        i = i.split(",")[0].strip()
                    else:
                        i = i.split("/")[0].strip()
                    if i not in eth_map_dict:
                        eth_map_dict[i] = _create_ethnicity_lut(i, source_name, eth_cats, no_id)
                    map_list.append(eth_map_dict[i])

                map[x] = map_list
            else:
                if x not in eth_map_dict:
                    eth_map_dict[x] = _create_ethnicity_lut(x, source_name, eth_cats, no_id)
                    map[x] = eth_map_dict[x]

        return col.map(map)
    elif mult_info.is_mult:
        map = {}
        for x in vals:
            if type(x) == str:
                items = x.split(mult_info.delim_race)
                for k, i in enumerate(items):
                    if i == "ISL":  # LA County code for AAPI is ASIAN-PACIFIC,ISL
                        continue
                    i = i.strip()
                    if i not in eth_map_dict:
                        eth_map_dict[i] = _create_ethnicity_lut(i, source_name, eth_cats, no_id)
                    items[k] = eth_map_dict[i]

                map[x] = items
            else:
                if x not in eth_map_dict:
                    eth_map_dict[x] = _create_ethnicity_lut(x, source_name, eth_cats, no_id)
                    map[x] = eth_map_dict[x]

        # Use apply instead of map to allow copying. Otherwise, values are references.
        return col.apply(lambda x: map[x].copy() if x in map else x)
    else:
        for x in vals:
            eth_map_dict[x] = _create_ethnicity_lut(x, source_name, eth_cats, no_id)

        return col.map(eth_map_dict)
    

def convert_race(col, source_name, race_map_dict=None, mult_info=_MultData(),
                 race_cats=defs.get_race_cats(), agg_cat=False, no_id="pass"):

    if race_map_dict == None:
        race_map_dict = {}
    # Decode the race column
    val_counts = col.value_counts()
    vals = [x if type(x)==str else x  for x in val_counts.index]
    if mult_info.has_demo_col:  # Check if column is a demographics column containin a list of all demographics (i.e. race, gender, age)
        map = {}
        delims = ["(", ","]
        for x in vals:
            if type(x) == str:
                items = [x]
                for d in delims:
                    if d in x:
                        items = x.split(d)
                        break
                
                map_list = []
                if len(items[0])==0:
                    istart = 1
                else:
                    istart = 0
                for k, i in enumerate(items[istart:]):
                    if "," in i:
                        i = i.split(",")[0].strip()
                    else:
                        i = i.split("/")[0].strip()
                    if i not in race_map_dict:
                        race_map_dict[i] = _create_race_lut(i, source_name, race_cats, agg_cat, no_id)
                    map_list.append(race_map_dict[i])

                map[x] = map_list
            else:
                if x not in race_map_dict:
                    race_map_dict[x] = _create_race_lut(x, source_name, race_cats, agg_cat, no_id)
                    map[x] = race_map_dict[x]

        return col.map(map)
    elif mult_info.is_mult:
        map = {}
        for x in vals:
            if type(x) == str:
                items = x.split(mult_info.delim_race)
                for k, i in enumerate(items):
                    if i == "ISL":  # LA County code for AAPI is ASIAN-PACIFIC,ISL
                        continue
                    i = i.strip()
                    if i not in race_map_dict:
                        race_map_dict[i] = _create_race_lut(i, source_name, race_cats, agg_cat, no_id, known_single=True)
                    items[k] = race_map_dict[i]

                map[x] = items
            else:
                if x not in race_map_dict:
                    race_map_dict[x] = _create_race_lut(x, source_name, race_cats, agg_cat, no_id)
                    map[x] = race_map_dict[x]

        # Use apply instead of map to allow copying. Otherwise, values are references.
        return col.apply(lambda x: map[x].copy() if x in map else x)
    else:
        for x in vals:
            race_map_dict[x] = _create_race_lut(x, source_name, race_cats, agg_cat, no_id)

        return col.map(race_map_dict)


def _create_race_lut(x, source_name, race_cats, agg_cat, no_id, known_single=False):
    no_id = no_id.lower()
    if no_id not in ["pass", "null", "error","test"]:
        raise ValueError(f"no_id is {no_id}. It should be 'pass', 'null', or 'error'.")

    has_unspecified = defs._race_keys.UNSPECIFIED in race_cats
    has_aapi = defs._race_keys.AAPI in race_cats
    has_asian = defs._race_keys.ASIAN in race_cats
    has_black = defs._race_keys.BLACK in race_cats
    has_indigenous = defs._race_keys.INDIGENOUS in race_cats
    has_me = defs._race_keys.MIDDLE_EASTERN in race_cats or defs._race_keys.MIDDLE_EASTERN_SOUTH_ASIAN in race_cats
    has_pi = defs._race_keys.PACIFIC_ISLANDER in race_cats
    has_multiple = defs._race_keys.MULTIPLE in race_cats
    has_south_asian = defs._race_keys.SOUTH_ASIAN in race_cats or defs._race_keys.MIDDLE_EASTERN_SOUTH_ASIAN in race_cats
    has_latino = defs._race_keys.LATINO in race_cats

    orig = x

    if (type(x) != str or x.isdigit()) and source_name in ["California", "Lincoln"]:
        default_cats = defs.get_race_cats(expand=True)
        if source_name == "California":
            # California stops data has a dictionary that can be applied. 
            # https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2023-01/RIPA%20Dataset%20Read%20Me%202021%20Final%20rev%20011223.pdf
            map_dict = {
                1 : default_cats[defs._race_keys.ASIAN], 2 : default_cats[defs._race_keys.BLACK], 3 : default_cats[defs._race_keys.LATINO], 
                4 : default_cats[defs._race_keys.MIDDLE_EASTERN_SOUTH_ASIAN], 5 : default_cats[defs._race_keys.INDIGENOUS],
                6 : default_cats[defs._race_keys.PACIFIC_ISLANDER], 7 : default_cats[defs._race_keys.WHITE],
                8 : default_cats[defs._race_keys.MULTIPLE]
            }
        elif source_name == "Lincoln":
            # Lincoln data has a dictionary that can be applied.
            # https://ago-item-storage.s3.us-east-1.amazonaws.com/df19cb240d984564965969c568b855c6/LPD_code_tables.pdf?X-Amz-Security-Token=IQoJb3JpZ2luX2VjEFQaCXVzLWVhc3QtMSJHMEUCIQDzuBic65OoJxPSf0DueLmXhFj5uFdAe98fepapTY091AIgLoXQrkkBdRbDjEuwvyGQjpJJLUFgi3ygI8BtjfWuT%2BAq1QQI3f%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARAAGgw2MDQ3NTgxMDI2NjUiDMxtFJ8yzxImKjHOuSqpBGW%2FA1aH%2F%2FKSYJB1edlTnRu3JE4fzkqCbDBWZrWyKxGmnGWPqnY0hIhhMNS4GSpuwkDUkZANDRh1DUPqVlDZtVfez2tNZwJuCuisqAyofRFAYx%2FQNn18FsJcnXyOZ4hbnduu78jGF4yJU1faAxd1hGrWaKC4NWpYsXxupkdHT5ZDf9SuVGMz8I5L1hB3QLK8cjnxfhZbsXtU6EgBx14jyOdLzrzHBwOFzAkpz7SQJZofNtpRVyRY26xh8RSNL%2Fj1d9LcxjgxchTbiKedc7NZMfoXBWPdb8z%2F3RJMx%2FHVrc6a4NCGc8toWHypuH7lMg1JGxouo9cAc7JxkQrhF9ruPVU54y6EQcxw%2B1JjNfsezqNnk1WLKJ38FJ2zFstJFMfQWNTAqqLk63VjQ2TnnNModiOxlfWLJLfoLSviSqbCVwyQJDzQR6cplKGyICLbIasFPkk%2Fihh1xdH2HeC153E8stesc9FaX5yN09zSYP61TuNOXQicSD9uCAMPEuClxUc0D17iuC%2F9Kxs8Hs1G%2B5C6kD4FBEPYHlAQE4t4nN%2Fehnv7POOgQpYPVo%2Bi%2FI8RRG%2BLPaVI5PQP7ANBQ5ljqqpaIV%2BN0wvt9BdkYi1fsOEsTgg9N2IuN8EdfAXFTFeT%2FmXAZvfEFocvdQqPsV9cXY2cPE3ryEAH0KHzBo6%2Bpfnsay9H2RE23pW%2FvfuAND%2BktpTy0HBzogihKOGs7h4SJBvxUJjTDDBB5agDS6Mwu4SlnwY6qQHNEZx4%2Fa3r4RWoqz%2F%2FC9hkEGXmQNLZZB%2BlcGoE9iVTNLaqK0Wr6oKdgC1xwm5Hq0aUnatarAR5zL8TsG4JkzaCDCggx8Q36CzDsC0sjx1yaEKb4UC6g2MOpY%2Bc0NrIbL%2BQkByyqyreP6797Rx1lHrIvWOrlXaqoB4lMz3Kxxkqe20CCVE5UkTLCAC1X%2Fe5IqLIOzmgigv8tD5vLDE6FaCq3GXMUA%2BUifE3&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230212T204254Z&X-Amz-SignedHeaders=host&X-Amz-Expires=300&X-Amz-Credential=ASIAYZTTEKKEYIVNVE33%2F20230212%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=da74601aabea63766444b4260baaaec0697753cf9cddf8d70b37204b5126687b
            map_dict = {
                1 : default_cats[defs._race_keys.WHITE], 2 : default_cats[defs._race_keys.BLACK], 3 : default_cats[defs._race_keys.LATINO], 
                4 : default_cats[defs._race_keys.ASIAN], 5 : default_cats[defs._race_keys.INDIGENOUS],
                6 : default_cats[defs._race_keys.OTHER]
            }

        x = int(x)
        if x not in map_dict:
            if no_id=="error":
                raise KeyError(f"Unknown race value for {source_name} data: {orig}")
            else:
                return orig if no_id=="pass" else ""
        else:
            # Replace numerical code with default value
            x = map_dict[x]

    if type(x) == str:
        if not known_single and "," in x: 
            # Treat this as a list of races
            race_list = []
            for v in orig.split(","):
                race_list.append(_create_race_lut(v, source_name, race_cats, agg_cat,no_id=no_id))

            if has_unspecified and agg_cat:
                num_unspecified = sum([x==race_cats[defs._race_keys.UNSPECIFIED] for x in race_list])
                if num_unspecified == len(race_list):
                    # All are unspecified. Just return unspecified rather than a list
                    return race_cats[defs._race_keys.UNSPECIFIED]
                else:
                    # Ignore the unspecifieds
                    race_list = [x for x in race_list if x!=race_cats[defs._race_keys.UNSPECIFIED]]

            if len(race_list)==1:
                return race_list[0]
            elif has_aapi and has_asian and has_pi and \
                set(race_list) == set([race_cats[defs._race_keys.ASIAN], race_cats[defs._race_keys.PACIFIC_ISLANDER]]):
                # Simplify to AAPI
                return race_cats[defs._race_keys.AAPI]
            elif has_aapi and has_asian and \
                set(race_list) == set([race_cats[defs._race_keys.ASIAN], race_cats[defs._race_keys.AAPI]]):
                return race_cats[defs._race_keys.AAPI]
            elif agg_cat and has_latino and race_cats[defs._race_keys.LATINO] in race_list:
                return race_cats[defs._race_keys.LATINO]
            elif agg_cat and has_multiple:
                return race_cats[defs._race_keys.MULTIPLE]
            else:
                return race_list
                 
        # Clean x
        x = x.upper().replace("_", " ").replace("*","").replace("-"," ").replace(".","")
        x = x.strip()

        if source_name in ["Austin", "Bloomington", "New York City"]:
            # Handling dataset-specific codes
            if source_name=="Austin":
                # Per email with Kruemcke, Adrian <Adrian.Kruemcke@austintexas.gov> on 2022-06-17
                map_dict = {"M":"Middle Eastern", "P":"Pacific Islander/Native Hawaiian", "N":"Native American/Alaskan", "O":"Other"}
            elif source_name=="Bloomington":
                # https://data.bloomington.in.gov/dataset/bloomington-police-department-employee-demographics
                map_dict = {"K":"African American, Hispanic", "L":"Caucasian, Hispanic", "N":"Indian/Alaskan Native, Hispanic",
                            "P":"Asian/Pacific Island, Hispanic"}
            elif source_name == "New York City":
                # https://www.nyc.gov/assets/nypd/downloads/zip/analysis_and_planning/stop-question-frisk/SQF-File-Documentation.zip
                map_dict = {"P":"Black-Hispanic", "Q":"White-Hispanic", "X":"Unknown","Z":"Other"}

            if x.upper() in map_dict:
                x = map_dict[x.upper()].upper()

    if pd.notnull(x):
        # Check if value is part of race_cats
        # This is particularly necessary for custom race_cats dicts
        for k,v in race_cats.items():
            # Key is always be a string
            if k.upper()==str(orig).upper() or k.upper()==str(x) or str(v)==str(orig).upper() or str(v)==str(x):
                return v
    
    if pd.isnull(x):
        if has_unspecified:
            return race_cats[defs._race_keys.UNSPECIFIED]
        elif no_id=="error":
            raise ValueError(f"Null value found in race column: {orig}")
        else:
            # Same result whether no_id is pass or null
            return ""
    if has_unspecified and (x in ["MISSING","NOT SPECIFIED", "", "NOT RECORDED","N/A", "NOT REPORTED", "NONE"] or \
        (type(x)==str and ("NO DATA" in x or "NOT APP" in x or "NO RACE" in x or "NULL" in x))):
        return race_cats[defs._race_keys.UNSPECIFIED]
    if defs._race_keys.WHITE in race_cats and x in ["W", "CAUCASIAN", "WN", "WHITE"]:  # WN = White-Non-Hispanic
        return race_cats[defs._race_keys.WHITE]
    if has_black and ((x in ["B", "AFRICAN AMERICAN"] or "BLAC" in x) and "HISPANIC" not in x):
        if x.count("BLACK") > 1:
            raise ValueError(f"The value of {x} likely contains the races for multiple people")
        return race_cats[defs._race_keys.BLACK]
    if has_south_asian and (x in ["SOUTH ASIAN"] or ("ASIAN" in x and "INDIAN" in x)):
        if defs._race_keys.SOUTH_ASIAN in race_cats:
             return race_cats[defs._race_keys.SOUTH_ASIAN]
        else:
            return race_cats[defs._race_keys.MIDDLE_EASTERN_SOUTH_ASIAN]
    if (has_asian or has_aapi) and (x in ["A"] or "ASIAN" in x):
        if has_aapi and ("PAC" in x or "HAWAII" in x):
            return race_cats[defs._race_keys.AAPI] 
        elif x in ["A", "ASIAN"]:
            return race_cats[defs._race_keys.ASIAN] if has_asian else race_cats[defs._race_keys.AAPI]
    if (has_pi or has_aapi) and ("HAWAII" in x or "PACIFICISL" in x.replace(" ","").replace("_","") or \
                                 "PACISL" in x.replace(" ","")):
        return race_cats[defs._race_keys.PACIFIC_ISLANDER] if has_pi else race_cats[defs._race_keys.AAPI]
    if has_latino and x in ["H", "WH", "HISPANIC", "LATINO", "HISPANIC OR LATINO", "LATINO OR HISPANIC", "HISPANIC/LATINO", "LATINO/HISPANIC"]:
        return race_cats[defs._race_keys.LATINO] 
    if has_indigenous and (x in ["I", "INDIAN", "ALASKAN NATIVE", "AI/AN"] or "AMERICAN IND" in x or \
        "NATIVE AM" in x or  "AMERIND" in x.replace(" ","") or "ALASKAN" in x or "AMIND" in x.replace(" ","")):
        return race_cats[defs._race_keys.INDIGENOUS] 
    if has_me and (x in ["ME","ARABIC"] or "MIDDLE EAST" in x):
        return race_cats[defs._race_keys.MIDDLE_EASTERN] if defs._race_keys.MIDDLE_EASTERN in race_cats else race_cats[defs._race_keys.MIDDLE_EASTERN_SOUTH_ASIAN]
    if has_multiple and ("OR MORE" in x or "MULTI" in x or \
        x.replace(" ","") in ["MIXED","BIRACIAL"]):
        return race_cats[defs._race_keys.MULTIPLE]
    if defs._race_keys.OTHER_UNKNOWN in race_cats and "UNK" in x and "OTH" in x:
        return race_cats[defs._race_keys.OTHER_UNKNOWN]
    if defs._race_keys.UNKNOWN in race_cats and  ("UNK" in x or x in ["U", "UK"]):
        return race_cats[defs._race_keys.UNKNOWN]
    if defs._race_keys.OTHER in race_cats and (x in ["O","OTHER"] or "OTHER UNCLASS" in x or "OTHER RACE" in x):
        return race_cats[defs._race_keys.OTHER]
    
    if agg_cat:
        if has_latino and (("HISP" in x and "NONHISP" not in x) or ("LATINO" in x and "NONLATINO" not in x)):
            return race_cats[defs._race_keys.LATINO] 
        elif has_black and x in ["EAST AFRICAN"]:
            return race_cats[defs._race_keys.BLACK]
        elif has_south_asian and "INDIAN" in x and "BURMESE" in x:
            if defs._race_keys.SOUTH_ASIAN in race_cats:
                return race_cats[defs._race_keys.SOUTH_ASIAN]
            else:
                return race_cats[defs._race_keys.MIDDLE_EASTERN]
        elif (has_asian or has_aapi) and (x in ["CAMBODIAN",'VIETNAMESE',"LAOTIAN","JAPANESE"] or "ASIAN" in x):
            return race_cats[defs._race_keys.ASIAN] if has_asian else race_cats[defs._race_keys.AAPI]
        elif (has_asian or has_aapi or has_pi) and x in ["FILIPINO"]:
            if has_aapi:
                return race_cats[defs._race_keys.AAPI]
            elif has_asian:
                # Asian or PI could be preferred here. Arbitrarily selecting Asian
                return race_cats[defs._race_keys.ASIAN]
            else:
                race_cats[defs._race_keys.PACIFIC_ISLANDER]
        elif (has_pi or has_aapi) and x in ["POLYNESIAN","SAMOAN"]:
            return race_cats[defs._race_keys.PACIFIC_ISLANDER] if has_pi else race_cats[defs._race_keys.AAPI]
        elif no_id=="error":
            raise ValueError(f"Unknown value in race column: {orig}")
        elif no_id=="test":
            if x in ["MALE","GIVING ANYTHING OF VALUE"] or \
                (source_name in ["Chapel Hill","Lansing","Fayetteville"] and x in ["M","S","P"]) or \
                (source_name in ["Cincinnati","San Diego"] and x == "F") or \
                (source_name in ["Columbia"] and x == "P") or \
                (source_name in ["New York City"] and x == "Q") or \
                (source_name in ["Detroit", "Fairfax County"] and x in ["N","SELECT"]) or \
                x in ["OTHER / MIXED RACE", "UNDISCLOSED", "OR SPANISH ORIGIN","PREFER NOT TO SAY","OTHERBLEND"] or \
                len(orig)>100:
                # This is meant to be temporary for testing
                return "BAD DATA"
            elif "EXEMPT" in x:
                return orig
            else:
                raise ValueError(f"Unknown value in race column: {orig}")
        else:
            return orig if no_id=="pass" else ""
    elif no_id=="error":
        raise ValueError(f"Unknown value in race column: {orig}")
    else:
        return orig if no_id=="pass" else ""
    

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
            elif any([x.lower()=="age" for x in _camel_case_split(w)]):
                match_cols.append(col_name)
                break

    return match_cols
    

def _race_validator(df, cols_test, source_name, mult_data=_MultData()):
    search_all = df.columns.equals(cols_test)
    match_cols = []
    for col_name in cols_test:
        # Check for unambiguous column names
        if col_name.lower()=="race":
            match_cols.append(col_name)
            continue

        # Check if column name is a descriptive term + race
        desc_terms = ["citizen","subject","suspect","civilian", "officer","deputy"]
        words = set(re.split(r"[^A-Za-z]+", col_name.lower()))
        if any([{x,"race"}==words for x in desc_terms]):
            match_cols.append(col_name)
            continue

        words = set(_camel_case_split(col_name))
        if any([{x,"race"}==words for x in desc_terms]):
            match_cols.append(col_name)
            continue
        
        col = df[col_name]
        try:
            if "address" in col_name.lower() or \
                (search_all and pd.api.types.is_numeric_dtype(col.dtype)):
                # Addresses are complicated and could trigger false alarms
                # Don't search numeric codes when searching all columns. Will lead to false matches
                continue

            race_cats = defs.get_race_cats()
            # Function for validating a column is a race column
            col = convert_race(col, source_name, race_cats=race_cats, mult_info=mult_data)
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
                other_found = False
                for k in race_cats.values():
                    if k not in [race_cats[defs._race_keys.ASIAN],race_cats[defs._race_keys.BLACK],race_cats[defs._race_keys.OTHER]] and \
                        k in counts.index:
                        other_found = True
                        
                if not other_found:
                    continue

            match_cols.append(col_name)
        except Exception as e:
            pass

    return match_cols

def _gender_validator(df, match_cols_test, source_name):
    match_cols = []
    for col_name in match_cols_test:
        # Check for unambiguous column names
        if col_name.lower()=="gender" or col_name.lower()=="sex":
            match_cols.append(col_name)
            continue

        # Check if column name is a descriptive term + race
        desc_terms = ["citizen","subject","suspect","civilian", "officer","deputy"]
        words = set(re.split(r"[^A-Za-z]+", col_name.lower()))
        if any([{x,"gender"}==words for x in desc_terms]) or \
            any([{x,"sex"}==words for x in desc_terms]):
            match_cols.append(col_name)
            continue

        words = set(_camel_case_split(col_name))
        if any([{x,"gender"}==words for x in desc_terms]) or \
            any([{x,"sex"}==words for x in desc_terms]):
            match_cols.append(col_name)
            continue
        
        try:
            col = df[col_name]
            # Verify gender column
            gender_cats = defs.get_gender_cats()
            col = _convert_gender(col, source_name, gender_cats)

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

def _camel_case_split(x):
    words = []
    cur = x[0]
    for l in x[1:]:
        if l.isupper() and cur[-1].islower():
            words.append(cur)
            cur = l
        else:
            cur+=l

    words.append(cur)
    return words
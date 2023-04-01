import logging
import numbers
import pandas as pd
import re
from collections import Counter, defaultdict
import numpy as np
from numbers import Number
import warnings

try:
    from . import datetime_parser
    from . import defs
    from . import _converters as convert
    from ._converters import _MultData, _p_age_range
    from ._preproc_utils import check_column, DataMapping
    from .utils import camel_case_split, split_words
except:
    import defs
    import datetime_parser
    import _converters as convert
    from _converters import _MultData, _p_age_range
    from _preproc_utils import check_column, DataMapping
    from utils import camel_case_split, split_words

_skip_tables = ["calls for service"]


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
    max_num_vals[col == "Marsy’s Law Exempt"] = np.nan

    return max_num_vals, delim, max_count


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

            if len(matches) > 0 and len(matches)!=len(select_cols):
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

        officer_terms = ["officer","deputy", "empl"]
        if known_col_name != None:
            if known_col_name not in self.df.columns:
                raise ValueError(f"Known column {known_col_name} is not in the DataFrame")
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
                new_matches = self._remove_excluded(new_matches, exclude_col_names, match_substr)
                if len(new_matches)>0:
                    if not officer_found and not civilian_found:
                        match_cols.extend(new_matches)
                    elif officer_found:
                        # Only keep columns with civilian terms
                        match_cols.extend([x for x in new_matches if any([y in x.lower() for y in civilian_terms])])
                    elif civilian_found:
                        # Only keep columns with officer terms
                        match_cols.extend([x for x in new_matches if any([y in x.lower() for y in officer_terms])])

                    all_raw = all([x.startswith("raw_") for x in match_cols])

                    # There are cases where there should be multiple matches for both officer and community member
                    # columns. On occasion, they are labeled with different terms 
                    officer_found = any([any([y in x.lower() for y in officer_terms]) for x in match_cols])
                    civilian_found = any([any([y in x.lower() for y in civilian_terms]) for x in match_cols])

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

            if always_validate or (multi_check and validator != None and len(match_cols) > 1) or \
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
            secondary_patterns = [("equals","date"),("contains","time"), ("contains", "cad"),  ("contains", "assigned"), 
                                  ("contains", "occurred"), ("contains", "offense")],
            # Calls for services often has multiple date/times with descriptive names for what it corresponds to.
            # Don't generalize by standardizing
            exclude_table_types=[defs.TableType.EMPLOYEE, defs.TableType.CALLS_FOR_SERVICE], 
            not_required_table_types=[defs.TableType.COMPLAINTS_OFFICERS, defs.TableType.COMPLAINTS_CIVILIANS,
                                 defs.TableType.USE_OF_FORCE_CIVILIANS_OFFICERS, defs.TableType.USE_OF_FORCE_CIVILIANS, 
                                 defs.TableType.USE_OF_FORCE_OFFICERS, 
                                 defs.TableType.SHOOTINGS_CIVILIANS, defs.TableType.SHOOTINGS_OFFICERS,
                                 defs.TableType.CRASHES_CIVILIANS, defs.TableType.CRASHES_VEHICLES],
            validator=datetime_parser.validate_date,
            always_validate=True)

        if len(match_cols) > 1:
            raise NotImplementedError()
        elif len(match_cols) == 1:
            self.col_map[defs.columns.DATE] = match_cols[0]
        
        secondary_patterns = []
        validator_args = []
        exclude_col_names = ["rankattimeofincident"]
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
                        new_col = convert.convert(convert.convert_off_or_civ, col)
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

            match_cols = self._find_col_matches(["race", "citizen_demographics","officer_demographics","ethnicity","re_grp"],
                validator=_race_validator, 
                validate_args=[self.source_name],
                search_data=True)  
            # TODO: Remove failures when column are not found
            race_cols, race_types = self._id_demographic_column(match_cols,
                defs.columns.RACE_CIVILIAN, defs.columns.RACE_OFFICER,
                defs.columns.RACE_OFF_AND_CIV,
                tables_to_exclude=[
                    ("Santa Rosa", defs.TableType.USE_OF_FORCE),
                    ("New York City", defs.TableType.CRASHES_CIVILIANS),
                    ("San Diego", defs.TableType.CRASHES_CIVILIANS),
                    ("Montgomery County", defs.TableType.COMPLAINTS),
                    ("Albany", defs.TableType.COMPLAINTS),
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
                    ],
                specific_cases=[_case("California", defs.TableType.USE_OF_FORCE_CIVILIANS_OFFICERS, "Race_Ethnic_Group", defs.columns.RACE_OFF_AND_CIV),
                                _case("Minneapolis", defs.TableType.STOPS, "race", defs.columns.RACE_CIVILIAN),
                                _case("Fairfax County", defs.TableType.ARRESTS, ["ArresteeRa","OfficerRac"], [defs.columns.RACE_CIVILIAN, defs.columns.RACE_OFFICER], year=range(2016,2021)),
                                _case("Lansing", defs.TableType.SHOOTINGS, ["Race_Sex","Officer"], [defs.columns.RACE_CIVILIAN, defs.columns.RACE_OFFICER])
                    ],
                adv_type_match=_find_race_col_type_advanced)

            # enthnicity is to deal with typo in Ferndale data. Consider using fuzzywuzzy in future for fuzzy matching
            match_cols = self._find_col_matches(["ethnic", "enthnicity"], exclude_col_names=race_cols,
                                                exclude_table_types=[defs.TableType.COMPLAINTS_ALLEGATIONS])
            self._id_ethnicity_column(race_types, match_cols, race_cols,
                specific_cases=[
                    _case("Fairfax County", defs.TableType.ARRESTS, "ArresteeEt", defs.columns.ETHNICITY_CIVILIAN)
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
                                _case("Fairfax County", defs.TableType.ARRESTS, "ArresteeAg", defs.columns.AGE_CIVILIAN)]
                )

            match_cols = self._find_col_matches(["agerange","age_range","age range","agegroup","age_group"])
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
                                _case("Fairfax County", defs.TableType.ARRESTS, ["ArresteeSe","OfficerSex"], [defs.columns.GENDER_CIVILIAN, defs.columns.GENDER_OFFICER])
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
            if c.equals(self.source_name, self.table_type, self.year) and c.update_map(self.col_map, self.df.columns):
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

        k = 0
        while k < len(validation_types):
            # Check if there is a corresponding race column (this is required)
            if validation_types[k] not in race_types:
                # Check if detected ethnicity column might actually be a race/ethnicity column
                try:
                    mult_data = _MultData()
                    num_race, mult_data.delim_race, max_count_race = _count_values(self.df[eth_cols[k]])
                    if max_count_race>0:
                        potential_mults = self.df[eth_cols[k]][num_race>1]
                        # races = ["WHITE","BLACK","ASIAN"]  # Leaving out HISPANIC as items could be labeled race/ethnicity
                        # r = ["W","B","H","A"]
                        for x in potential_mults:
                            # Check for repeats
                            x_split = x.split(mult_data.delim_race)
                            if any([y>1 for y in Counter(x_split).values()]):
                                mult_data.is_mult = True
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
            if c.equals(self.source_name, self.table_type, self.year) and c.update_map(self.col_map, self.df.columns):
                return col_names, []

        if len(col_names) == 0:
            if not required or \
                self.table_type in [defs.TableType.USE_OF_FORCE_INCIDENTS, defs.TableType.SHOOTINGS_INCIDENTS, 
                                    defs.TableType.CALLS_FOR_SERVICE, 
                                    defs.TableType.CRASHES, defs.TableType.CRASHES_INCIDENTS, defs.TableType.CRASHES_VEHICLES,
                                    defs.TableType.INCIDENTS, defs.TableType.COMPLAINTS_BACKGROUND,
                                    defs.TableType.COMPLAINTS_ALLEGATIONS, defs.TableType.COMPLAINTS_PENALTIES] or \
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
                        # TODO: Change multi-person columns to dicts
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
                            k = 0
                            for val in reversed(vals_reversed):
                                persons[k] = val
                                k+=1

                            return persons

                        self.df[types[0]] = self.df[col_names_new].apply(combine_col,axis=1)
                        self.data_maps.append(DataMapping(orig_column_name=col_names, new_column_name=types[0]))
                        for col in col_names:
                            self._cleanup_old_column(col)
                        col_names = [types[0]]
                        types = types[0:1]
                
                if len(set(types)) != len(types) and adv_type_match != None:
                    col_names, types = adv_type_match(self.df, self.source_name, col_names, types, self.col_map, civilian_col_name, officer_col_name)

                if len(set(types)) != len(types):
                    if self.source_name=="Winooski" and set(col_names)==set(['Perceived Race', 'Issued To Race']):
                        # Has columns for officer perceived and actual race. We don't code separately for this.
                        # Warn and do not create a standardized race column
                        warnings.warn(f"{self.source_name} has multiple race columns for civilians ({col_names}). " +
                                    "Neither will be standardized to avoid creating ambiguity.")
                        col_names = []
                        types = []
                    else:
                        # TODO: Convert to a warning with an informative message
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
                std_map = {}
                self.df[col] = convert.convert(convert._create_gender_lut, self.df[self.col_map[col]], self.source_name, self.gender_cats, std_map=std_map, 
                                                    mult_info=mult, delim=mult.delim_gender, no_id=self.no_id)

                self.data_maps.append(DataMapping(orig_column_name=self.col_map[col], new_column_name=col, data_maps=std_map,
                                                orig_column=self.df[self.col_map[col]]))


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
            self.df[col_name] = convert.convert(convert.convert_off_or_civ, self.df[col_name], self.source_name, 
                                                        std_map=std_map, no_id=self.no_id)

            self.data_maps.append(DataMapping(orig_column_name=self.col_map[col_name], new_column_name=col_name,
                                              data_maps=std_map,
                                              orig_column=self.df[self.col_map[col_name]]))


    def standardize_race(self):        
        for col,mult in zip([defs.columns.RACE_CIVILIAN, defs.columns.RACE_OFFICER, defs.columns.RACE_OFF_AND_CIV], 
                            [self.mult_civilian, self.mult_officer, self.mult_both]):
            if col in self.col_map:
                race_column = self.col_map[col]
                race_map_dict = {}
                self.df[col] = convert.convert(convert._create_race_lut, self.df[race_column], self.source_name, 
                                                        std_map=race_map_dict, delim=mult.delim_race,
                                                        mult_info=mult, cats=self.race_cats, agg_cat=self.agg_race_cat, 
                                                        no_id=self.no_id)

                self.data_maps.append(
                    DataMapping(orig_column_name=race_column, new_column_name=col,
                        data_maps=race_map_dict, orig_column=self.df[race_column])
                )

    
    def standardize_ethnicity(self):
        
        for col,mult in zip([defs.columns.ETHNICITY_CIVILIAN, defs.columns.ETHNICITY_OFFICER, defs.columns.ETHNICITY_OFF_AND_CIV], 
                            [ self.mult_civilian,  self.mult_officer,  self.mult_both]):
            if col in self.col_map:
                eth_column = self.col_map[col]

                eth_map_dict = {}
                self.df[col] = convert.convert(convert._create_ethnicity_lut, self.df[eth_column], 
                                                self.source_name, self.eth_cats, std_map=eth_map_dict, 
                                                delim=mult.delim_race, mult_info=mult,
                                                no_id=self.no_id)

                self.data_maps.append(
                    DataMapping(orig_column_name=eth_column, new_column_name=col,
                        data_maps=eth_map_dict, orig_column=self.df[eth_column])
                )

    
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
                if isinstance(x[race_col],dict):
                    return {k:f"{r} {e}" for k,r,e in zip(x[race_col].keys(), x[race_col].values(), x[eth_col].values())}
                else:
                    return f"{x[race_col]} {x[eth_col]}"
                
            f = concat
        elif type=="merge":
            if defs._eth_keys.NONLATINO not in self.eth_cats:
                raise KeyError(f"Unable to merge race and ethnicity columns without a value for self.eth_cats[{defs._eth_keys.NONLATINO}]")
            def merge(x):
                 if isinstance(x[race_col],dict):
                    return {k:(r if e==self.eth_cats[defs._eth_keys.NONLATINO] else e) for k,r,e in 
                            zip(x[race_col].keys(), x[race_col].values(), x[eth_col].values())}
                 else:
                    return x[race_col] if x[eth_col]==self.eth_cats[defs._eth_keys.NONLATINO] else x[eth_col]

            f = merge

        self.df[race_col] = self.df[[race_col, eth_col]].apply(f, axis=1)
        self.data_maps.append(
            DataMapping(orig_column_name=[race_col_orig, eth_col], new_column_name=race_col)
        )
        
    
    def check_for_multiple(self):        
        self._check_for_multiple(self.mult_civilian, defs.columns.RACE_CIVILIAN, defs.columns.AGE_CIVILIAN, 
                                 defs.columns.GENDER_CIVILIAN, defs.columns.ETHNICITY_CIVILIAN,
                                 defs.columns.AGE_RANGE_CIVILIAN)
        self._check_for_multiple(self.mult_officer, defs.columns.RACE_OFFICER, defs.columns.AGE_OFFICER, 
                                 defs.columns.GENDER_OFFICER, defs.columns.ETHNICITY_OFFICER,
                                 defs.columns.AGE_RANGE_OFFICER)
        self._check_for_multiple(self.mult_both, defs.columns.RACE_OFF_AND_CIV, defs.columns.AGE_OFF_AND_CIV, 
                                 defs.columns.GENDER_OFF_AND_CIV, defs.columns.ETHNICITY_OFF_AND_CIV,
                                 defs.columns.AGE_RANGE_OFF_AND_CIV)
    

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
                mult_data.is_mult=True
                mult_data.is_dict = True

                # Ensure that all dictionaries have the same length. If not, add empty vals
                lens = self.df[avail_cols].apply(lambda x: [len(y) for y in x])
                max_counts = lens.max(axis=1)
                for col in avail_cols:
                    needs_append = lens.index[lens[col] < max_counts]
                    for idx in needs_append:
                        val = self.df[col].loc[idx]
                        for k in range(lens[col].loc[idx], max_counts.loc[idx]):
                            val[k] = ""
                        self.df[col].loc[idx] = val

                return
            else:
                raise NotImplementedError("One or more but not all demographics columns are dictionaries")

        if self.table_type not in [defs.TableType.SHOOTINGS, defs.TableType.USE_OF_FORCE, defs.TableType.COMPLAINTS,
                defs.TableType.SHOOTINGS_CIVILIANS, defs.TableType.USE_OF_FORCE_CIVILIANS,
                defs.TableType.SHOOTINGS_OFFICERS, defs.TableType.USE_OF_FORCE_OFFICERS, defs.TableType.USE_OF_FORCE_CIVILIANS_OFFICERS]:
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
            # Look for count followed by race            
            race_count_re = re.compile("\d+\s?-\s?[A-Za-z]+")
            if self.df[self.col_map[race_col]].apply(lambda x: race_count_re.search(x) is not None if pd.notnull(x) else False).any():
                mult_data.is_mult = True
                mult_data.has_counts = True
                return
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
        for col,mult in zip([defs.columns.AGE_RANGE_CIVILIAN, defs.columns.AGE_RANGE_OFFICER, defs.columns.AGE_RANGE_OFF_AND_CIV],
                            [self.mult_civilian, self.mult_officer, self.mult_both]):
            if col in self.col_map:
                map = {}
                self.df[col] = convert.convert(convert._create_age_range_lut, self.df[self.col_map[col]],
                                               std_map=map, delim=mult.delim_age, mult_info=mult, no_id=self.no_id)
                
                # Check for age ranges
                validator = re.compile(r'\d+-\d+')
                val = self.df[col].apply(lambda x: isinstance(x,str) and validator.search(x) is not None)
                if not val.any():
                    # Check if values are numbers (i.e. ages)
                    try:
                        new_col = pd.to_numeric(self.df[col], errors="raise", downcast="integer").astype("Int64")
                        if new_col.notnull().mean() > 0.5:
                            # This appears to be an age column
                            new_col_name = col.replace("_RANGE","")
                            if hasattr(defs.columns, new_col_name):
                                self.col_map[new_col_name] = self.col_map.pop(col)
                                self.df.rename(columns={col:new_col_name}, inplace=True)
                                col = new_col_name
                                map = None
                        else:
                            raise NotImplementedError()
                    except:
                        pass

                self.data_maps.append(DataMapping(orig_column_name=self.col_map[col], new_column_name=col,
                        orig_column=self.df[self.col_map[col]],
                        data_maps=map))


    def standardize_age(self):
        for col, mult in zip([defs.columns.AGE_CIVILIAN, defs.columns.AGE_OFFICER, defs.columns.AGE_OFF_AND_CIV],
                            [self.mult_civilian, self.mult_officer, self.mult_both]):
            if col in self.col_map:
                self._standardize_age(col, mult)
        

    def _standardize_age(self, col_name, mult_info):
        max_age = 120  # Somewhat conservative max age of a human
        val_counts = self.df[self.col_map[col_name]].value_counts()
        vals = [x for x in val_counts.index if type(x)==str]
        if mult_info.is_dict:
            def converter(x):
                out = x.copy()
                for k,v in x.items():
                    if pd.isnull(v) or (isinstance(v,str) and len(v)==0):
                        out[k] = np.nan
                    else:
                        out[k] = int(v)
                    
                return out

            self.df[col_name] = self.df[self.col_map[col_name]].apply(converter)
        elif mult_info.has_demo_col:
            def extract_ages(x):
                if type(x) == str:
                    items = x.split("(")
                    result = {}
                    for k, i in enumerate(items[1:]):
                        val = i.split(",")[2].strip()
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
        elif mult_info.is_mult:
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
                    for k,y in enumerate(x.split(mult_info.delim_age)):
                        if k>0:
                            multi_found = True
                        if pd.notnull(y) and y.strip().isdigit() and 0<int(y)<=max_age:
                            cur_val[k] = int(y)
                        else:
                            if isinstance(y, str) and _p_age_range.search(y) and not contains_range:
                                contains_range = True
                                num_ranges+=1
                            elif not contains_range and not contains_na:
                                num_na+=1
                                contains_na = True
                                
                            cur_val[k] = int(np.nan)

                    new_ages.append(cur_val)

            if not multi_found:
                # Convert from dicts to scalars
                new_ages = [x[0] if isinstance(x,dict) else x for x in new_ages]
            if "AGE" in col_name and (num_ranges / len(self.df)>0.6 or \
                (num_ranges/len(self.df) > 0.05 and num_ranges+num_na==len(self.df))):
                # This appears to be an age range column not an age column
                new_col_name = col_name.replace("AGE","AGE_RANGE")
                if hasattr(defs.columns, new_col_name):
                    self.col_map[new_col_name] = self.col_map.pop(col_name)
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
                        throw = True
                        if check_column(self.col_map[col_name], "age"):
                            # See if all values are null or acceptable values                          
                            for x in self.df[self.col_map[col_name]]:
                                if pd.isnull(x) or (isinstance(x,str) and x.lower()=="unknown"):
                                    continue
                                elif isinstance(x,Number) or  (isinstance(x,str) and x.strip().isdigit()):
                                    x = int(x)
                                    if x<0 or x>max_age:
                                        break
                                else:
                                    break
                            else:
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

    def update_map(self, col_map, columns):
        for k in range(len(self.old_name)):
            if self.old_name[k] not in columns:
                return False
        for k in range(len(self.old_name)):
            col_map[self.new_name[k]] = self.old_name[k]
        return True
       

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
                                  delim=mult_data.delim_race, mult_info=mult_data)
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
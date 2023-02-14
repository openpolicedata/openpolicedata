from datetime import datetime
import numbers
import pandas as pd
import re
from collections import Counter
import numpy as np
import warnings

try:
    from . import datetime_parser
    from . import defs
except:
    import defs
    import datetime_parser

# Age range XX - YY
_p_age_range = re.compile(r"""
    ^                   # Start of string
    (\d+)               # Lower bound of age range. Capture for reuse.
    \s?(-|_|TO)\s?          # - or _ between lower and upper bound with optional spaces 
    (\d+)               # Upper bound of age range. Capture for reuse.
    (\s?[-_]\s?\1\s?[-_]\s?\3)?  # Optional mistaken repeat of same pattern. 
    $                   # End of string
    """, re.VERBOSE)

class DataMapping:
    old_column_name = None  # Original name of columns
    new_column_name = None  # Renamed columns
    data_maps = None        # Map(s) from old data to new data

    def __init__(self, old_column_name=None, new_column_name = None, data_maps=None):
        self.old_column_name = old_column_name
        self.new_column_name = new_column_name
        self.data_maps = data_maps

    def __repr__(self) -> str:
        return ',\n'.join("%s: %s" % item for item in vars(self).items())


def standardize(df, table_type, year, date_column=None, agency_column=None, source_name=None, keep_raw=True): 
    col_map = id_columns(df, table_type, year, date_column, agency_column, source_name=source_name)
    maps = []
    df = standardize_date(df, col_map, maps, keep_raw=keep_raw)
    df = standardize_time(df, col_map, maps, keep_raw=keep_raw)
    df = standardize_off_or_civ(df, col_map, maps, keep_raw=keep_raw)
    mult_civilian, mult_officer, mult_both = _check_for_multiple_all(df, col_map, table_type)
    df = standardize_race(df, col_map, maps, source_name, keep_raw=keep_raw, 
        mult_civilian=mult_civilian, mult_officer=mult_officer, mult_both=mult_both)
    # standardize_age needs to go before standardize_age_range as it can detect if what was ID'ed as 
    # an age column is actually an age range
    df = standardize_age(df, col_map, maps, keep_raw=keep_raw, 
        mult_civilian=mult_civilian, mult_officer=mult_officer, mult_both=mult_both)
    df = standardize_age_range(df, col_map, maps, keep_raw=keep_raw, source_name=source_name)
    df = standardize_gender(df, col_map, maps, source_name, keep_raw=keep_raw, 
        mult_civilian=mult_civilian, mult_officer=mult_officer, mult_both=mult_both)

    if defs.columns.AGENCY in col_map:
        df.rename({col_map[defs.columns.AGENCY] : defs.columns.AGENCY}, axis=1, inplace=True)
        maps.append(DataMapping(old_column_name=col_map[defs.columns.AGENCY], new_column_name=defs.columns.AGENCY))

    # Reorder columns so standardized columns are first
    reordered_cols = [x for x in col_map.keys() if x in df.columns]
    reordered_cols.extend([x for x in df.columns if x not in col_map.keys()])
    df = df[reordered_cols]
    
    return df, maps

def _count_values(col):
    delims = [",", "|", ";", "/"]
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

def _check_for_multiple(df, col_map, mult_data, race_col, age_col, gender_col, eth_col):
    # If only 1 of these columns, it will be handled by the standardize function
    if (race_col in col_map) + (age_col in col_map) + (gender_col in col_map) + (eth_col in col_map) > 1:
        cols_test = [race_col, age_col, gender_col]
        test = True
        for k in range(len(cols_test)):
            if cols_test[k] in col_map:
                for m in range(k+1,len(cols_test)):
                    if cols_test[m] in col_map:
                        test = test and col_map[cols_test[k]] == col_map[cols_test[m]]

        if test:
            # This is a column with all demographics that will be handled by standardize function
            mult_data.has_demo_col = True
            return
        else:
            max_count_race = 0
            max_count_eth = 0
            max_count_age = 0
            max_count_gender = 0
            if race_col in col_map:
                num_race, mult_data.delim_race, max_count_race = _count_values(df[col_map[race_col]])
                if max_count_race==0:
                    return

            if eth_col in col_map:
                num_eth, mult_data.delim_eth, max_count_eth = _count_values(df[col_map[eth_col]])
                has_any = max_count_eth!=0
                if not has_any and num_eth[num_race>1].notnull().any():
                    return
                if has_any and race_col in col_map and max_count_race>0:
                    idx = num_race.notnull() & num_eth.notnull()
                    if (num_race[idx]!=num_eth[idx]).any():
                        return

            if age_col in col_map:
                num_age, mult_data.delim_age, max_count_age = _count_values(df[col_map[age_col]])
                has_any = max_count_age!=0
                if not has_any and race_col in col_map and num_age[num_race>1].notnull().any():
                    return
                if has_any and race_col in col_map and max_count_race>0:
                    idx = num_race.notnull() & num_age.notnull()
                    if (num_race[idx]!=num_age[idx]).any():
                        return

            if gender_col in col_map:
                num_gender, mult_data.delim_gender, max_count_gender = _count_values(df[col_map[gender_col]])
                if max_count_gender==0:
                    return
                if race_col in col_map and max_count_race>0:
                    idx = num_race.notnull() & num_gender.notnull()
                    if (num_race[idx]!=num_gender[idx]).any():
                        count = (num_race[idx]!=num_gender[idx]).sum()
                        if count==1 and (max_count_race>3 or max_count_gender>3):
                            # One of the columns has fewer
                            idx = [k for k in range(len(num_race)) if pd.notnull(num_race[k]) and pd.notnull(num_gender[k]) and num_race[k]!=num_gender[k]][0]
                            col1 = col_map[race_col]
                            col2 = col_map[gender_col]
                            col1_val = df[col_map[race_col]].iloc[idx]
                            col2_val = df[col_map[gender_col]].iloc[idx]
                            print(f"Columns {col1} and {col2} contain demographics for multiple people per row and will be converted to columns "+
                                f"{race_col} and {gender_col}. However, the number of data in row {idx} is not consistent: "+
                                f"{col1_val} vs. {col2_val}")
                        else:
                            return
                if age_col in col_map and max_count_age>0:
                    idx = num_gender.notnull() & num_age.notnull()
                    if (num_gender[idx]!=num_age[idx]).any():
                        return
                
            if int(max_count_race>0) + int(max_count_age>0) + int(max_count_gender>0) + int(max_count_eth>0) > 1:
                mult_data.is_mult = True
    elif race_col in col_map:
        num_race, mult_data.delim_race, max_count_race = _count_values(df[col_map[race_col]])
        if max_count_race>0:
            potential_mults = df[col_map[race_col]][num_race>1]
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

    return

def _check_for_multiple_all(df, col_map, table_type):
    mult_civilian = _MultData()
    mult_officer = _MultData()
    mult_both = _MultData()
    if table_type in [defs.TableType.SHOOTINGS, defs.TableType.USE_OF_FORCE, defs.TableType.COMPLAINTS,
                    defs.TableType.SHOOTINGS_CIVILIANS, defs.TableType.USE_OF_FORCE_CIVILIANS,
                    defs.TableType.SHOOTINGS_OFFICERS, defs.TableType.USE_OF_FORCE_OFFICERS, defs.TableType.USE_OF_FORCE_CIVILIANS_OFFICERS]:
        _check_for_multiple(df, col_map, mult_civilian, defs.columns.RACE_CIVILIAN, defs.columns.AGE_CIVILIAN, defs.columns.GENDER_CIVILIAN, defs.columns.ETHNICITY_CIVILIAN)
        _check_for_multiple(df, col_map, mult_officer, defs.columns.RACE_OFFICER, defs.columns.AGE_OFFICER, defs.columns.GENDER_OFFICER, defs.columns.ETHNICITY_OFFICER)
        _check_for_multiple(df, col_map, mult_both, defs.columns.RACE_OFF_AND_CIV, defs.columns.AGE_OFF_AND_CIV, defs.columns.GENDER_OFF_AND_CIV, defs.columns.ETHNICITY_OFF_AND_CIV)
        
    return mult_civilian, mult_officer, mult_both

def _pattern_search(all_cols, select_cols, patterns, match_substr):
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

            matches = find_matches(all_cols, guess, match_substr, 1, pattern)
        else:
            raise ValueError("Unknown pattern type")

        if len(matches) > 0:
            break

    return matches

def _remove_excluded(match_cols, exclude_col_names, df, match_substr):
    for e in exclude_col_names:
        if type(e) == tuple:
            match_cols = _pattern_search(df.columns, match_cols, [e], match_substr[0])
        elif e in match_cols:
            match_cols.remove(e)

    return match_cols

def _find_col_matches(df, table_type, match_substr, 
    known_col_name=None, 
    only_table_types=None,
    exclude_table_types=[], 
    not_required_table_types="ALL", 
    exclude_col_names=[], 
    secondary_patterns=[], 
    validator=None,
    always_validate=False,
    validate_args=[],
    std_col_name = None):

    if table_type in exclude_table_types or \
        (only_table_types != None and table_type not in only_table_types):
        if std_col_name in df.columns:
            return [std_col_name]
        else:
            return []

    officer_terms = ["officer","deputy"]
    if known_col_name != None:
        if known_col_name not in df.columns:
            raise ValueError(f"{known_col_name} is not a column")
        return [known_col_name]
    else:
        if type(match_substr) == str:
            match_substr = [match_substr]

        match_cols = []
        civilian_terms = ["citizen","subject","suspect","civilian"]
        civilian_found = False
        officer_found = False
        for s in match_substr:
            new_matches = [x for x in df.columns if s.lower() in x.lower()]
            new_matches = _remove_excluded(new_matches, exclude_col_names, df, match_substr)
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
            match_cols = _pattern_search(df.columns, match_cols, secondary_patterns, match_substr[0])
            match_cols = _remove_excluded(match_cols, exclude_col_names, df, match_substr)
            
            
        if len(match_cols)==0:
            if not_required_table_types != "ALL" and table_type not in not_required_table_types:
                raise ValueError(f"Column not found with substring {match_substr}")
        elif len(match_cols)>1:
            new_matches = _pattern_search(df.columns, match_cols, secondary_patterns, match_substr[0])
            if len(new_matches)>0:
                match_cols = new_matches
                match_cols = _remove_excluded(match_cols, exclude_col_names, df, match_substr)
            elif len(match_cols)==0 and len(secondary_patterns)>0:
                raise NotImplementedError()

        multi_check = True
        if len(match_cols) == 2:
            # Check if one column is an officer column and one is not
            multi_check = sum([any([y.lower() in x.lower() for y in officer_terms]) for x in match_cols])!=1

        if multi_check and validator != None and (len(match_cols) > 1 or always_validate):
            match_cols_test = match_cols
            match_cols = []
            score = None
            for col in match_cols_test:
                try:
                    new_score = validator(df[col], *validate_args)                           
                except Exception as e:
                    pass
                else:
                    if score == new_score:
                        match_cols.append(col)
                    elif new_score != None and (score == None or new_score > score):
                        # Higher scoring item found. This now takes priority
                        score = new_score
                        match_cols = [col] 

        match_cols = _remove_excluded(match_cols, exclude_col_names, df, match_substr)

        return match_cols


def _id_demographic_column(table_type, year, col_map, col_names, source_name,
    civilian_col_name, officer_col_name, civ_officer_col_name,
    required=True,
    tables_to_exclude=[],
    sources_to_exclude=[],
    specific_cases=[],
    adv_type_match=None,
    df=None):

    for c in specific_cases:
        if c.equals(source_name, table_type, year):
            c.update_map(col_map)
            return col_names, []

    if len(col_names) == 0:
        if not required or \
            table_type == defs.TableType.USE_OF_FORCE_INCIDENTS or \
            table_type == defs.TableType.SHOOTINGS_INCIDENTS or \
            table_type == defs.TableType.CALLS_FOR_SERVICE or \
            source_name in sources_to_exclude or \
            any([(source_name, table_type)==x for x in tables_to_exclude]):
            return col_names, []
        else:
            raise NotImplementedError()

    if defs.columns.CIVILIAN_OR_OFFICER in col_map:
        if len(col_names) > 1:
            raise NotImplementedError()

        col_map[civ_officer_col_name] = col_names[0]
        return col_names, [civ_officer_col_name]
    else:     
        is_officer_table = table_type == defs.TableType.EMPLOYEE.value or \
            ("- OFFICERS" in table_type and "CIVILIANS" not in table_type)

        types = []
        for k in range(len(col_names)):
            if "off" in col_names[k].lower() or "deputy" in col_names[k].lower() or \
                "employee" in col_names[k].lower() or "ofcr" in col_names[k].lower() or \
                (is_officer_table and "suspect" not in col_names[k].lower() and "supsect" not in col_names[k].lower()):
                types.append(officer_col_name)
            else:
                types.append(civilian_col_name)

        if len(set(types)) != len(types):
            if adv_type_match != None:
                col_names, types = adv_type_match(df, source_name, col_names, types, col_map, civilian_col_name, officer_col_name)

            if len(set(types)) != len(types):
                raise NotImplementedError()

        for k in range(len(col_names)):
            col_map[types[k]] = col_names[k]

    return col_names, types

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


def _id_ethnicity_column(source_name, table_type, year, col_map, race_types, eth_cols, race_cols, specific_cases=[]):

    for c in specific_cases:
        if c.equals(source_name, table_type, year):
            c.update_map(col_map)
            return

    if len(eth_cols)==0:
        return

    eth_types = []
    validation_types = []
    if defs.columns.CIVILIAN_OR_OFFICER in col_map:
        if len(eth_cols) > 1:
            raise NotImplementedError()

        eth_types.append(defs.columns.ETHNICITY_OFF_AND_CIV)
        validation_types.append(defs.columns.RACE_OFF_AND_CIV)
    else:
        is_officer_table = table_type == defs.TableType.EMPLOYEE.value or \
                ("- OFFICERS" in table_type and "CIVILIANS" not in table_type)
        for k in range(len(eth_cols)):
            if "officer" in eth_cols[k].lower() or \
                "offcr" in eth_cols[k].lower() or is_officer_table or \
                (source_name=="Orlando" and table_type==defs.TableType.SHOOTINGS and eth_cols[k]=="ethnicity"):
                eth_types.append(defs.columns.ETHNICITY_OFFICER)
                validation_types.append(defs.columns.RACE_OFFICER)
            else:
                eth_types.append(defs.columns.ETHNICITY_CIVILIAN)
                validation_types.append(defs.columns.RACE_CIVILIAN)

    if len(set(eth_types)) != len(eth_types):
        raise NotImplementedError()

    if any([x not in race_types for x in validation_types]):
        raise NotImplementedError()

    for k in range(len(eth_cols)):
        if eth_types[k] == defs.columns.ETHNICITY_CIVILIAN and \
            "subject_race" in race_cols and eth_cols[k].startswith("raw"):
            # This is a raw column from Stanford data that has already been standardized into subject_race
            continue
        col_map[eth_types[k]] = eth_cols[k]


def id_columns(df, table_type, year, date_column=None, agency_column=None, source_name=None):
    col_map = {}

    match_cols = _find_col_matches(df, table_type, "date", known_col_name=date_column, 
        std_col_name=defs.columns.DATE,
        secondary_patterns = [("equals","date"), ("contains", "cad"),  ("contains", "assigned"), ("contains", "occurred")],
        # secondary_patterns = [("contains", "call"), ],
        not_required_table_types=[defs.TableType.USE_OF_FORCE_CIVILIANS_OFFICERS, defs.TableType.USE_OF_FORCE_CIVILIANS, 
            defs.TableType.USE_OF_FORCE_OFFICERS, defs.TableType.SHOOTINGS_CIVILIANS, defs.TableType.SHOOTINGS_OFFICERS,
            defs.TableType.TRAFFIC_ARRESTS],
        # Calls for services often has multiple date/times with descriptive names for what it corresponds to.
        # Don't generalize by standardizing
        exclude_table_types=[defs.TableType.EMPLOYEE, defs.TableType.CALLS_FOR_SERVICE], validator=datetime_parser.validate_date)

    if len(match_cols) > 1:
        raise NotImplementedError()
    elif len(match_cols) == 1:
        col_map[defs.columns.DATE] = match_cols[0]
    
    secondary_patterns = []
    if defs.columns.DATE in col_map:
        # Create a pattern from the format of the date column name
        # That might also be the pattern of the time column
        success = False
        if "date" in col_map[defs.columns.DATE].lower():
            match = re.search('date', col_map[defs.columns.DATE], re.IGNORECASE)
            if match != None:
                if match[0].islower():
                    secondary_patterns = [("equals", col_map[defs.columns.DATE].replace(match[0], "time"))]
                elif match[0].isupper():
                    secondary_patterns = [("equals", col_map[defs.columns.DATE].replace(match[0], "TIME"))]
                elif match[0].istitle():
                    secondary_patterns = [("equals", col_map[defs.columns.DATE].replace(match[0], "Time"))]
                else:
                    raise NotImplementedError()
                success = True

        if not success:
            segs = col_map[defs.columns.DATE].split("_")
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
        if defs.columns.DATE in col_map:
            exclude_col_names.append(col_map[defs.columns.DATE])
            validator_args.append(df[col_map[defs.columns.DATE]])
        
        match_cols = _find_col_matches(df, table_type, "time", 
            std_col_name=defs.columns.TIME,
            secondary_patterns=secondary_patterns, validator=datetime_parser.validate_time,
            validate_args=validator_args,
            exclude_col_names=exclude_col_names,
            exclude_table_types=[defs.TableType.CALLS_FOR_SERVICE],
            always_validate=True)

        if len(match_cols) > 1:
            raise NotImplementedError()
        elif len(match_cols) == 1:
            col_map[defs.columns.TIME] = match_cols[0]
        
    # Incidents tables shouldn't have demographics data
    if table_type not in [defs.TableType.SHOOTINGS_INCIDENTS, defs.TableType.USE_OF_FORCE_INCIDENTS]:    
        def race_validator(col):
            convert_race(col, source_name)
            return None

        def role_validator(col):
            new_col = convert_off_or_civ(col)
            vals = new_col.unique()

            if defs.person_types.CIVILIAN not in vals or defs.person_types.OFFICER not in vals:
                # California data, RAE = Race and Ethnicity
                # Check if it's possible this does not indicate civilian vs officer
                match_cols = _find_col_matches(df, table_type, ["race", "descent","rae_full","citizen_demographics","officer_demographics","ethnicity"],
                    validator=race_validator)

                off_type = False
                civ_type = False
                is_officer_table = table_type == defs.TableType.EMPLOYEE.value or \
                    ("- OFFICERS" in table_type and "CIVILIANS" not in table_type)
                for k in range(len(match_cols)):
                    if "off" in match_cols[k].lower() or "deputy" in match_cols[k].lower() or \
                        (is_officer_table and "suspect" not in match_cols[k].lower() and "supsect" not in match_cols[k].lower()):
                        off_type = True
                    else:
                        civ_type = True
                    
                if civ_type and off_type:
                    raise ValueError("Column is likely not a civilian or officer column")

            return None

        match_cols = _find_col_matches(df, table_type, ["Civilian_Officer","ROLE"], 
            only_table_types = [defs.TableType.USE_OF_FORCE, defs.TableType.USE_OF_FORCE_CIVILIANS_OFFICERS, defs.TableType.SHOOTINGS],
            validator=role_validator,
            always_validate=True)
        if len(match_cols) > 1:
            raise NotImplementedError()
        elif len(match_cols) == 1:
            col_map[defs.columns.CIVILIAN_OR_OFFICER] = match_cols[0]

        match_cols = _find_col_matches(df, table_type, ["race", "descent","rae_full","citizen_demographics","officer_demographics","ethnicity"],
            validator=race_validator)  
        race_cols, race_types = _id_demographic_column(table_type, year, col_map, match_cols, source_name,
            defs.columns.RACE_CIVILIAN, defs.columns.RACE_OFFICER,
            defs.columns.RACE_OFF_AND_CIV,
            tables_to_exclude=[
                ("Montgomery County", defs.TableType.COMPLAINTS),
                ("Albany", defs.TableType.COMPLAINTS),
                ("Denver", defs.TableType.STOPS),
                ("Lincoln", defs.TableType.VEHICLE_PURSUITS),
                ("Los Angeles", defs.TableType.STOPS),
                ("South Bend", defs.TableType.USE_OF_FORCE),
                ("State Police", defs.TableType.SHOOTINGS),
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
            adv_type_match=_find_race_col_type_advanced,
            df=df)

        # enthnicity is to deal with typo in Ferndale data. Consider using fuzzywuzzy in future for fuzzy matching
        match_cols = _find_col_matches(df, table_type, ["ethnic", "enthnicity"], exclude_col_names=race_cols)
        _id_ethnicity_column(source_name, table_type, year, col_map, race_types, match_cols, race_cols,
            specific_cases=[
                 _case("Fairfax County", defs.TableType.ARRESTS, "ArresteeEt", defs.columns.ETHNICITY_CIVILIAN, year=range(2016,2021))
            ])

        # Do not want the result to contain the word agency
        match_cols = _find_col_matches(df, table_type, ["age","citizen_demographics","officer_demographics"], 
            exclude_col_names=[("does not contain",["agenc","damage","group","range","at_hire","engagement"])])
        _id_demographic_column(table_type, year, col_map, match_cols, source_name, 
            defs.columns.AGE_CIVILIAN, defs.columns.AGE_OFFICER,
            defs.columns.AGE_OFF_AND_CIV,
            required=False,
            specific_cases=[_case("Norman", defs.TableType.COMPLAINTS, "Age", defs.columns.AGE_OFFICER, year=[2016,2017,2018]),
                            _case("Norman", defs.TableType.USE_OF_FORCE, "Age", defs.columns.AGE_OFFICER, year=[2016,2017]),
                            _case("Fairfax County", defs.TableType.ARRESTS, "ArresteeAg", defs.columns.AGE_CIVILIAN, year=range(2016,2021))]
            )

        match_cols = _find_col_matches(df, table_type, ["agerange","age_range","agegroup","age_group"])
        _id_demographic_column(table_type, year, col_map, match_cols, source_name,
            defs.columns.AGE_RANGE_CIVILIAN, defs.columns.AGE_RANGE_OFFICER,
            defs.columns.AGE_RANGE_OFF_AND_CIV,
            required=False)

        def gender_validator(col):
            _convert_gender(col, source_name)
            return None
        match_cols = _find_col_matches(df, table_type, ["gend", "sex","citizen_demographics","officer_demographics"],
            validator=gender_validator)        
        _id_demographic_column(table_type, year, col_map, match_cols, source_name,
            defs.columns.GENDER_CIVILIAN, defs.columns.GENDER_OFFICER, 
            defs.columns.GENDER_OFF_AND_CIV,
            required=False,
            specific_cases=[_case("California", defs.TableType.STOPS, "G_FULL", defs.columns.GENDER_CIVILIAN),
                            _case("Lansing", defs.TableType.SHOOTINGS, ["Race_Sex","Officer"], [defs.columns.GENDER_CIVILIAN, defs.columns.GENDER_OFFICER],
                            _case("Fairfax County", defs.TableType.ARRESTS, ["ArresteeSe","OfficerSex"], [defs.columns.GENDER_CIVILIAN, defs.columns.GENDER_OFFICER], year=range(2016,2021)))
                ],
            adv_type_match=_find_gender_col_type_advanced,
            df=df)

    match_cols = _find_col_matches(df, table_type, [], known_col_name=agency_column)
    if len(match_cols) > 1:
        raise NotImplementedError()
    elif len(match_cols) == 1:
        col_map[defs.columns.AGENCY] = match_cols[0]

    for key, value in col_map.items():
        if key == value:
            # Need to rename original column
            col_map[key] =_cleanup_old_column(df, value, keep_raw=True)

    return col_map
    
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

def _cleanup_old_column(df, col_name, keep_raw):
    if keep_raw:
        new_name = "RAW_"+col_name
        df.rename(columns={col_name : new_name}, inplace=True)
        return new_name
    else:
        df.drop(col_name, axis=1, inplace=True)
        return None
        
def standardize_date(df, col_map, maps, keep_raw=True):
    if defs.columns.DATE in col_map:
        date_data = df[col_map[defs.columns.DATE]]
        if col_map[defs.columns.DATE].lower() == "year":
            month_idx = [k for k,x in enumerate(df.columns) if x.lower() == "month"]
            if len(month_idx) > 0:
                date_cols = [col_map[defs.columns.DATE], df.columns[month_idx[0]]]

                day_idx = [k for k,x in enumerate(df.columns) if x.lower() == "day"]
                if len(day_idx) > 0:
                    date_cols.append(df.columns[day_idx[0]])
                    date_data = df[date_cols]
                else:
                    date_data = df[date_cols].copy()
                    date_data["day"] = [1 for _unused in range(len(df))]
                                
        s_date = datetime_parser.parse_date_to_datetime(date_data)
        s_date.name = defs.columns.DATE
        df = pd.concat([df, s_date], axis=1)
        _cleanup_old_column(df, col_map[defs.columns.DATE], keep_raw)

        maps.append(DataMapping(old_column_name=col_map[defs.columns.DATE], new_column_name=defs.columns.DATE))

    return df


def standardize_time(df, col_map, maps, keep_raw=False):
    if defs.columns.TIME in col_map:
        col = datetime_parser.parse_time(df[col_map[defs.columns.TIME]])
        col.name = defs.columns.TIME
        if defs.columns.TIME in df.columns:
            df.drop(columns=defs.columns.TIME, inplace=True)
        # Concatentate to avoid fragmentation performance warnings
        df = pd.concat([df, col], axis=1)
        _cleanup_old_column(df, col_map[defs.columns.TIME], keep_raw)

        maps.append(DataMapping(old_column_name=col_map[defs.columns.TIME], new_column_name=defs.columns.TIME))

    return df

def standardize_gender(df, col_map, maps, source_name, keep_raw=False, 
        mult_civilian=None, mult_officer=None, mult_both=None):
    if defs.columns.GENDER_CIVILIAN in col_map:
        df = _standardize_gender(df, col_map, defs.columns.GENDER_CIVILIAN, maps, source_name, keep_raw, mult_info=mult_civilian)

    if defs.columns.GENDER_OFFICER in col_map:
        df = _standardize_gender(df, col_map, defs.columns.GENDER_OFFICER, maps, source_name, keep_raw, mult_info=mult_officer)

    if defs.columns.GENDER_OFF_AND_CIV in col_map:
        df = _standardize_gender(df, col_map, defs.columns.GENDER_OFF_AND_CIV, maps, source_name, keep_raw, mult_info=mult_both)

    return df

def _standardize_gender(df, col_map, col_name, maps, source_name, keep_raw, mult_info):
    std_map = {}
    col = _convert_gender(df[col_map[col_name]], source_name, std_map=std_map, mult_info=mult_info)
    col.name = col_name
    if col_name in df.columns:
        df.drop(columns=col_name, inplace=True)
    # Concatentate to avoid fragmentation performance warnings
    df = pd.concat([df, col], axis=1)
    _cleanup_old_column(df, col_map[col_name], keep_raw)
    maps.append(DataMapping(old_column_name=col_map[col_name], new_column_name=col_name, data_maps=std_map))

    return df

def _convert_gender(col, source_name, std_map={}, mult_info=_MultData()):
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
                        _create_gender_lut(i, std_map, source_name)
                    map_list.append(std_map[i])

                map[x] = map_list
            else:
                if x not in std_map:
                    _create_gender_lut(x, std_map, source_name)
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
                        _create_gender_lut(i, std_map, source_name)
                    items[k] = std_map[i]

                map[x] = items
            else:
                if x not in std_map:
                    _create_gender_lut(x, std_map, source_name)
                    map[x] = std_map[x]

        new_col = col.apply(lambda x: map[x].copy() if x in map else x)
    else:
        for x in vals:
            _create_gender_lut(x, std_map, source_name)
        new_col = col.map(std_map)

    return new_col

def _create_gender_lut(x, std_map, source_name):
    bad_data = ["BLACK","WHITE"]

    ca_stops_dict = {
        1 : defs.genders.MALE, 2 : defs.genders.FEMALE, 3 : defs.genders.TRANSGENDER_MAN, 
        4 : defs.genders.TRANSGENDER_WOMAN, 5 : defs.genders.GENDER_NONCONFORMING
    }
    lincoln_stops_dict = {1: defs.genders.MALE, 2: defs.genders.FEMALE}

    orig = x
    if type(x) == str:
        x = x.upper().replace("-","").replace("_","").replace(" ","").replace("'","")

    if pd.notnull(x) and (type(x) != str or x.isdigit()) and source_name == "California":
        std_map[orig] = ca_stops_dict[int(x)]
    elif pd.notnull(x) and (type(x) != str or x.isdigit()) and source_name == "Lincoln":
        if int(x) not in lincoln_stops_dict:
            std_map[orig] = orig
        else:
            std_map[orig] = lincoln_stops_dict[int(x)]
    elif pd.isnull(x) or x in ["MISSING", "UNSPECIFIED", "",",",'NOTSPECIFIED',"NOTRECORDED","NONE"] or \
        "NODATA" in x or "NOSEX" in x:
        std_map[orig] = defs.genders.UNSPECIFIED
    elif x in ["F", "FEMALE", "FEMAALE", "FFEMALE", "FEMAL"]:
        std_map[orig] = defs.genders.FEMALE
    elif x in ["M", "MALE", "MMALE"]:
        std_map[orig] = defs.genders.MALE
    elif x in ["OTHER", "O"]:
        std_map[orig] = defs.genders.OTHER
    elif x in ["TRANSGENDER"]:
        std_map[orig] = defs.genders.TRANSGENDER
    elif x in ["NONBINARY"]:
        std_map[orig] = defs.genders.GENDER_NONBINARY
    elif x in ["Gender Diverse (gender non-conforming and/or transgender)".upper().replace("-","").replace("_","").replace(" ",""),
        "GENDERNONCONFORMING"]:
        std_map[orig] = defs.genders.TRANSGENDER_OR_GENDER_NONCONFORMING
    elif x in ["TRANSGENDER MALE".replace(" ","")] or \
        "TRANSGENDERMAN" in x or \
        (source_name=="Los Angeles" and x=="T"):
        std_map[orig] = defs.genders.TRANSGENDER_MAN
    elif x in ["TRANSGENDER FEMALE".replace(" ","")] or \
        "TRANSGENDERWOMAN" in x or \
        (source_name=="Los Angeles" and x=="W"):
        std_map[orig] = defs.genders.TRANSGENDER_WOMAN
    elif (source_name=="Los Angeles" and x=="C"):
        std_map[orig] = defs.genders.GENDER_NONCONFORMING
    elif x in ["U","UK", "UNABLE TO DETERMINE".replace(" ","")] or "UNK" in x:
        std_map[orig] = defs.genders.UNKNOWN
    elif "EXEMPT" in x:
        std_map[orig] = defs.genders.EXEMPT
    elif x in ["DATAPENDING", "NOTAPPLICABLE","N/A"] or ("BUSINESS" in x and source_name=="Cincinnati"):
        std_map[orig] = orig
    elif x in bad_data or \
        (source_name=="New York City" and x=="Z") or \
        (source_name=="Baltimore" and x=="Y") or \
        (source_name in ["Seattle","New Orleans"] and x=="N") or \
        (source_name=="Los Angeles County" and x=="0") or \
        (x=="W" and source_name=="Cincinnati") or \
        source_name == "Buffalo":
        std_map[orig] = f"{defs.genders.BAD_DATA}: {orig}"
    else:
        raise ValueError(f"Unknown gender {orig}")


def standardize_age_range(df, col_map, maps, keep_raw=False, source_name=None):
    if defs.columns.AGE_RANGE_CIVILIAN in col_map:
        df = _standardize_age_range(df, col_map, defs.columns.AGE_RANGE_CIVILIAN, maps, keep_raw, source_name)

    if defs.columns.AGE_RANGE_OFFICER in col_map:
        df = _standardize_age_range(df, col_map, defs.columns.AGE_RANGE_OFFICER, maps, keep_raw, source_name)

    if defs.columns.AGE_RANGE_OFF_AND_CIV in col_map:
        df = _standardize_age_range(df, col_map, defs.columns.AGE_RANGE_OFF_AND_CIV, maps, keep_raw, source_name)

    return df

def standardize_age(df, col_map, maps, keep_raw=False, 
        mult_civilian=None, mult_officer=None, mult_both=None):
    if defs.columns.AGE_CIVILIAN in col_map:
        df = _standardize_age(df, col_map, defs.columns.AGE_CIVILIAN, maps, keep_raw, mult_info=mult_civilian)

    if defs.columns.AGE_OFFICER in col_map:
       df =  _standardize_age(df, col_map, defs.columns.AGE_OFFICER, maps, keep_raw, mult_info=mult_officer)

    if defs.columns.AGE_OFF_AND_CIV in col_map:
        df = _standardize_age(df, col_map, defs.columns.AGE_OFF_AND_CIV, maps, keep_raw, mult_info=mult_both)

    return df

def _standardize_age_range(df, col_map, col_name, maps, keep_raw, source_name):
    vals = df[col_map[col_name]].unique()
    map = {}

    p_plus = re.compile(r"(\d+)\+",re.IGNORECASE)
    p_over = re.compile(r"OVER (\d+)",re.IGNORECASE)
    p_under = re.compile(r"(UNDER|<)\s?(\d+)",re.IGNORECASE)
    p_above = re.compile(r"(\d+) AND ABOVE",re.IGNORECASE)
    for orig in vals:
        v = orig
        if type(v)==str:
            v = v.upper()
        if type(v) == str and _p_age_range.search(v)!=None:
            map[orig] = _p_age_range.sub(r"\1-\3", v)
        elif type(v) == str and p_over.search(v)!=None:
            map[orig] = p_over.sub(r"\1-120", v)
        elif type(v) == str and p_plus.search(v)!=None:
            map[orig] = p_plus.sub(r"\1-120", v)
        elif type(v) == str and p_above.search(v)!=None:
            map[orig] = p_above.sub(r"\1-120", v)
        elif type(v) == str and p_under.search(v)!=None:
            map[orig] = p_under.sub(r"0-\2", v)
        elif pd.isnull(v) or v in ["","NR","UNKNOWN","-","N/A"] or "NO DATA" in v or v.replace(" ","")=="":
            map[orig] = ""
        elif source_name=="Cincinnati" and v=="ADULT":
            map[orig] = "18-120"
        elif source_name=="Cincinnati" and v=="JUVENILE":
            map[orig] = "0-17"
        elif v.isdigit():
            map[orig] = orig
        else:
            # At least 1 case was found where age range was auto-corrected to a month/year like
            # 10-17 to 17-Oct. Check for this before giving up.
            try:
                map[orig] = datetime.strftime(datetime.strptime(v, "%d-%b"),"%m-%d")
            except:
                raise TypeError(f"Unknown val {v} for age range")

    col = df[col_map[col_name]].map(map)
    col.name = col_name
    if col_name in df.columns:
        df.drop(columns=col_name, inplace=True)
    # Concatentate to avoid fragmentation performance warnings
    df = pd.concat([df, col], axis=1)

    _cleanup_old_column(df, col_map[col_name], keep_raw)

    maps.append(DataMapping(old_column_name=col_map[col_name], new_column_name=col_name))

    return df

def _standardize_age(df, col_map, col_name, maps, keep_raw, mult_info):
    max_age = 120  # Somewhat conservative max age of a human
    vals = df[col_map[col_name]].unique()
    vals = [x for x in vals if type(x)==str]
    if mult_info.has_demo_col:
        def extract_ages(x):
            if type(x) == str:
                items = x.split("(")
                result = []
                for i in items[1:]:
                    val = i.split(",")[2]
                    if val[:val.find(")")].strip()=="":
                        result.append(np.nan)
                    else:
                        result.append(int(val[:val.find(")")]))

                return result
            else:
                return np.nan

        df[col_name] = df[col_map[col_name]].apply(extract_ages)
    elif mult_info.is_mult:
        # This column contains multiple ages in each cell
        def convert_to_age_list(x, delim):
            if pd.isnull(x):
                return np.nan
            else:
                return [int(y) if (pd.notnull(y) and y.isdigit() and 0<int(y)<=max_age) else np.nan for y in x.split(delim)]

        df[col_name] = df[col_map[col_name]].apply(convert_to_age_list, delim=mult_info.delim_age)
    else:
        try:
            col = pd.to_numeric(df[col_map[col_name]], errors="raise", downcast="integer")
        except Exception as e:
            # Not forcing all vals to have a match for _p_age_range because low ranges converted
            # to Month/Year at some point for at least 1 case (i.e. 10-17 to 17-Oct)
            if sum([_p_age_range.search(x)!=None for x in vals]) / len(vals) > 0.9 or  \
                all([x in ["UNKNOWN","ADULT","JUVENILE"] for x in vals]):
                if "AGE" in col_name:
                    new_col_name = col_name.replace("AGE","AGE_RANGE")
                    if hasattr(defs.columns, new_col_name):
                        col_map[new_col_name] = col_map.pop(col_name)
                        return df
            else:            
                # Attempt to convert most values to numbers
                col = pd.to_numeric(df[col_map[col_name]], errors="coerce", downcast="integer")
                if pd.isnull(col).all():
                    warnings.warn(f"Unable to convert column {col_map[col_name]} to an age")
                    col_map.pop(col_name)
                    return df

                test = [int(x)==y if (type(x)==str and x.isdigit()) else False for x,y in zip(df[col_map[col_name]],col)]
                sum_test = sum(test)
                if sum_test / len(test) < 0.2:
                    warnings.warn(f"Not converting {col_map[col_name]} to an age. If this is an age column only {sum(test) / len(test)*100:.1f}% of the data has a valid value")
                    col_map.pop(col_name)
                    return df
                elif sum_test / len(test) < 0.85 and not (sum_test>1 and len(test)-sum_test==1):
                    raise e

        if col.min() < 0 or col.max() > max_age:
            # Parse the column name to be certain this is an age column
            name_parts = col_map[col_name].lower().split("_")
            name_parts = [x.replace("subject","").strip() for x in name_parts]
            if "age" not in name_parts:
                raise ValueError("Age is outside expected range. Double check that this is an age column")
            else:
                # Some age columns have lots of bad data
                col.loc[col > max_age] = np.nan
                col.loc[col < 0] = np.nan

        col = col.round()
        col[col == 0] = np.nan
        col.name = col_name
        if col_name in df.columns:
            df.drop(columns=col_name, inplace=True)
        # Concatentate to avoid fragmentation performance warnings
        df = pd.concat([df, col], axis=1)

    if col_map[col_name] not in ["citizen_demographics","officer_demographics"]:  # These contain additional information that still needs to be used
        _cleanup_old_column(df, col_map[col_name], keep_raw)

    maps.append(DataMapping(old_column_name=col_map[col_name], new_column_name=col_name))

    return df

def convert_off_or_civ(col, std_map=None):
    vals = col.unique()
    if std_map == None:
        std_map = {}

    for x in vals:
        orig = x
        if type(x) == str:
            x = x.upper()

        if pd.isnull(x) or x in ["MISSING"]:
            std_map[orig] = defs.person_types.UNSPECIFIED
        elif x in ["OFFICER"]:
            std_map[orig] = defs.person_types.OFFICER
        elif x in ["SUBJECT","CIVILIAN"]:
            std_map[orig] = defs.person_types.CIVILIAN
        else:
            raise ValueError(f"Unknown person type {orig}")

    return col.map(std_map)

def standardize_off_or_civ(df, col_map, maps, keep_raw):
    if defs.columns.CIVILIAN_OR_OFFICER in col_map:
        col_name = defs.columns.CIVILIAN_OR_OFFICER
        std_map = {}
        df[col_name] = convert_off_or_civ(df[col_map[col_name]], std_map)

        _cleanup_old_column(df, col_map[col_name], keep_raw)

        maps.append(DataMapping(old_column_name=col_map[col_name], new_column_name=col_name))

    return df

def standardize_race(df, col_map, maps, source_name=None, keep_raw=False,
        mult_civilian=None, mult_officer=None, mult_both=None):
    if defs.columns.RACE_CIVILIAN in col_map:
        df = _standardize_race(df, col_map, maps, source_name=source_name, keep_raw=keep_raw,
            new_race_column=defs.columns.RACE_CIVILIAN,
            new_ethnicity_column=defs.columns.ETHNICITY_CIVILIAN,
            mult_info=mult_civilian)

    if defs.columns.RACE_OFFICER in col_map:
        df = _standardize_race(df, col_map,  maps, source_name=source_name, keep_raw=keep_raw,
            new_race_column=defs.columns.RACE_OFFICER,
            new_ethnicity_column=defs.columns.ETHNICITY_OFFICER,
            mult_info=mult_officer)

    if defs.columns.RACE_OFF_AND_CIV in col_map:
        df = _standardize_race(df, col_map,  maps, source_name=source_name, keep_raw=keep_raw,
            new_race_column=defs.columns.RACE_OFF_AND_CIV,
            new_ethnicity_column=defs.columns.ETHNICITY_OFF_AND_CIV,
            mult_info=mult_both)

    return df

def _standardize_race(df, col_map, maps, source_name, keep_raw, new_race_column, new_ethnicity_column, mult_info):
    race_column = col_map[new_race_column]
    if new_ethnicity_column in col_map and new_race_column in col_map and \
        col_map[new_ethnicity_column] != col_map[new_race_column]:
        ethnicity_column = col_map[new_ethnicity_column]
    else:
        ethnicity_column = None

    race_map_dict = {}
    col = convert_race(df[race_column], source_name, race_map_dict=race_map_dict, mult_info=mult_info)
    col.name = new_race_column
    if new_race_column in df.columns:
        df.drop(columns=new_race_column, inplace=True)
    # Concatentate to avoid fragmentation performance warnings
    df = pd.concat([df, col], axis=1)

    maps.append(
        DataMapping(old_column_name=race_column, new_column_name=new_race_column,
            data_maps=race_map_dict)
    )

    if race_column not in ["Race_Sex", "Officer", "citizen_demographics","officer_demographics"]:  # These contain additional information that still needs to be used
        _cleanup_old_column(df, race_column, keep_raw)

    if ethnicity_column != None:      
        vals = [x if type(x)==str else x for x in df[ethnicity_column].unique()]

        eth_map_dict = {}
        if mult_info.is_mult:
            for k in range(len(df)):
                x = df[ethnicity_column][k]
                y = df[new_race_column][k]
                if type(x) == str:
                    items = x.split(mult_info.delim_eth)
                    if len(y) == len(items):
                        for m in range(len(y)):
                            i = items[m].strip()
                            if i not in eth_map_dict:
                                _create_ethnicity_lut(i, eth_map_dict, source_name)
                            if i in eth_map_dict:
                                df[new_race_column][k][m] = eth_map_dict[i]
        else:
            for x in vals:
                _create_ethnicity_lut(x, eth_map_dict, source_name)

            def update_race(x):
                if x[ethnicity_column] in eth_map_dict:
                    return eth_map_dict[x[ethnicity_column]]
                else:
                    return x[new_race_column]

            df[new_race_column] = df.apply(update_race, axis=1)

        maps[-1].old_column_name = [maps[-1].old_column_name, ethnicity_column]
        maps[-1].data_maps = [maps[-1].data_maps, eth_map_dict]

        _cleanup_old_column(df, ethnicity_column, keep_raw)

    return df

def _create_ethnicity_lut(x, eth_map_dict, source_name):
    # The below values is used in the Ferndale demographics data. Just use the data from the race column in that
    # case which includes if officer is Hispanic
    ferndale_eth_vals = ['NR', 'FRENCH/GERMAN', 'MEXICAN', 'HUNGARIAN', 'LEBANESE', 'POLISH/SCOTTISH', 'IRISH', 'SYRIAN', 'POLISH']
    bad_data = ["R"]
    orig = x
    if type(x) == str:
        x = x.upper().replace("-","").replace(" ", "")
        
    if pd.isnull(x):
        pass
    elif x == "N" or x == 'NONHISPANIC' or x == "NH" or "NOTHISP" in x or \
        "EXEMPT" in x or x in ["W","A"] or \
        x in ferndale_eth_vals:
        pass  # Just use race value
    elif x == "H" or "HISPANIC" in x or "LATINO" in x:
        if "NON" in x:
            raise ValueError(x)
        eth_map_dict[orig] = defs.races.LATINO
    elif x in ["U", "UNKNOWN"]:
        eth_map_dict[orig] = defs.races.UNKNOWN
    elif "NODATA" in x.replace(" ","") or \
        x in ["MISSING", "", "NOTREPORTED",'NOTAPPICABLE(NONINDIVIDUAL)']:
        eth_map_dict[orig] = defs.races.UNSPECIFIED
    elif x in ["M"] and source_name == "Connecticut":
        eth_map_dict[orig] = defs.races.MIDDLE_EASTERN
    elif x in bad_data:
        eth_map_dict[orig] = f"{defs.races.BAD_DATA}: {orig}"
    else:
        raise ValueError(f"Unknown value {x}")

def convert_race(col, source_name, race_map_dict=None, mult_info=_MultData()):

    if race_map_dict == None:
        race_map_dict = {}
    # Decode the race column
    val_counts = col.value_counts()
    vals = [x if type(x)==str else x  for x in val_counts.index]
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
                        i = i.split(",")[0].strip()
                    else:
                        i = i.split("/")[0].strip()
                    if i not in race_map_dict:
                        _create_race_lut(i, race_map_dict, source_name)
                    map_list.append(race_map_dict[i])

                map[x] = map_list
            else:
                if x not in race_map_dict:
                    _create_race_lut(x, race_map_dict, source_name)
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
                        _create_race_lut(i, race_map_dict, source_name)
                    items[k] = race_map_dict[i]

                map[x] = items
            else:
                if x not in race_map_dict:
                    _create_race_lut(x, race_map_dict, source_name)
                    map[x] = race_map_dict[x]

        # Use apply instead of map to allow copying. Otherwise, values are references.
        return col.apply(lambda x: map[x].copy() if x in map else x)
    else:
        for x in vals:
            _create_race_lut(x, race_map_dict, source_name, val_counts=val_counts)

        return col.map(race_map_dict)

def _create_race_lut(x, race_map_dict, source_name, val_counts=None):
    # Austin values below are based on email with dataset owner who said they were from some source. 
    #   Do these values apply to other datasets using the same values?
    bad_data = ["MALE","GIVING ANYTHING OF VALUE"]

    ca_stops_dict = {
        1 : defs.races.ASIAN, 2 : defs.races.BLACK, 3 : defs.races.LATINO, 
        4 : defs.races.MIDDLE_EASTERN_SOUTH_ASIAN, 5 : defs.races.NATIVE_AMERICAN,
        6 : defs.races.HAWAIIAN, 7 : defs.races.WHITE,
        8 : defs.races.MULTIPLE
    }
    lincoln_stops_dict = {
        1 : defs.races.WHITE, 2 : defs.races.BLACK, 3 : defs.races.LATINO, 
        4 : defs.races.ASIAN, 5 : defs.races.NATIVE_AMERICAN,
        6 : defs.races.OTHER
    }

    orig = x
    if type(x) == str:
        if "," in orig: 
            # This may be a list of races
            race_list = []
            for v in orig.split(","):
                tmp_map = {}
                _create_race_lut(v, tmp_map, source_name)
                race_list.append(tmp_map[v])

            num_unspecified = sum([x==defs.races.UNSPECIFIED for x in race_list])
            if num_unspecified == len(race_list):
                race_map_dict[orig] = defs.races.UNSPECIFIED
            elif num_unspecified == len(race_list)-1:
                race_map_dict[orig] = [x for x in race_list if x!=defs.races.UNSPECIFIED][0]
            elif len(set(race_list)) != len(race_list):
                raise ValueError(f"Is this a multi-value column? - {orig}")
            elif set(race_list) == set([defs.races.ASIAN, defs.races.HAWAIIAN]):
                race_map_dict[orig] = defs.races.AAPI 
            else:
                race_map_dict[orig] = defs.races.MULTIPLE 
                return            

        x = x.upper().replace("_", " ").replace("*","").replace("-"," ").replace(".","")
        x = x.strip()
        
    if pd.isnull(x) or x in ["MISSING","NOT SPECIFIED","NULL VALUE", "", "NOT RECORDED","N/A", "NOT REPORTED", "NONE"] or \
        (type(x)==str and ("NO DATA" in x or "NOT APP" in x or "NO RACE" in x)):
        race_map_dict[orig] = defs.races.UNSPECIFIED
    elif (type(x) != str or x.isdigit()) and source_name == "California":
        race_map_dict[orig] = ca_stops_dict[int(x)]
    elif (type(x) != str or x.isdigit()) and source_name == "Lincoln":
        x_int = int(x)
        race_map_dict[orig] = lincoln_stops_dict[x_int]
    elif x in ["W", "CAUCASIAN", "WN", "WHITE"]:
        # WN = White-Non-Hispanic
        race_map_dict[orig] = defs.races.WHITE
    elif x in ["B", "AFRICAN AMERICAN", "EAST AFRICAN"] or "BLAC" in x:
        if x.count("BLACK") > 1:
            raise ValueError(f"The value of {x} likely contains the races for multiple people")
        race_map_dict[orig] = defs.races.BLACK
    elif x == "ASIAN INDIAN":
        race_map_dict[orig] = defs.races.ASIAN_INDIAN
    elif x == "SOUTH ASIAN" or ("INDIAN" in x and "BURMESE" in x):
        race_map_dict[orig] = defs.races.SOUTH_ASIAN        
    elif x in ["A", "CAMBODIAN",'VIETNAMESE'] or "ASIAN" in x:
        if "INDIAN" in x:
            raise ValueError("INDIAN should be picked up by ASIAN INDIAN condition")
        elif "PACIFIC" in x or "HAWAII" in x:
            race_map_dict[orig] = defs.races.AAPI
        else:
            race_map_dict[orig] = defs.races.ASIAN
    elif x in ["FILIPINO"]:
        race_map_dict[orig] = defs.races.AAPI
    elif "HAWAII" in x or "PACIFICISL" in x.replace(" ","").replace("_","") or \
        x in ["POLYNESIAN"] or \
        (source_name=="Austin" and x=="P"):
        race_map_dict[orig] = defs.races.HAWAIIAN
    elif x in ["H", "WH"] or ("HISP" in x and "NONHISP" not in x) or "LATINO" in x or \
        (source_name == "New York City" and (x in ["P", "Q"])) or \
        (source_name == "Bloomington" and (x in ["L", "N", "P"])): # This should stay below other races to not catch non-Hispanic
        # WH = White Hispanic
        # NYC and Bloomington are codes for Hispanic and a race
        race_map_dict[orig] = defs.races.LATINO
    elif x in ["I", "INDIAN", "ALASKAN NATIVE", "AI/AN"] or "AMERICAN IND" in x or \
        "NATIVE AM" in x or \
        "AMER IND" in x or "ALASKAN" in x or \
            (source_name=="Austin" and x=="N"):
        race_map_dict[orig] = defs.races.NATIVE_AMERICAN
    elif x in ["ME","ARABIC"] or "MIDDLE EAST" in x or \
        (source_name=="Austin" and x=="M"):
        race_map_dict[orig] = defs.races.MIDDLE_EASTERN
    elif "OR MORE" in x or "MULTI" in x or \
        x.replace(" ","") in ["MIXED","BIRACIAL"]:
        race_map_dict[orig] = defs.races.MULTIPLE
    elif "UNK" in x and "OTH" in x:
        race_map_dict[orig] = defs.races.OTHER_UNKNOWN
    elif "UNK" in x or x in ["U", "UK"]:
        race_map_dict[orig] = defs.races.UNKNOWN
    elif x in ["O","OTHER", "OTHER RACE"] or "OTHER UNCLASS" in x or \
        (source_name == "New York City" and x=="Z"):
        race_map_dict[orig] = defs.races.OTHER
    elif x in bad_data or \
        (source_name in ["Chapel Hill","Lansing"] and x == "M") or \
        (source_name in ["Cincinnati","San Diego"] and x == "F") or \
        (source_name in ["Columbia"] and x == "P") or \
        (source_name == "Detroit" and x in ["N","SELECT"]):
        race_map_dict[orig] = f"{defs.races.BAD_DATA}: {orig}"
    elif "EXEMPT" in x:
        race_map_dict[orig] = defs.races.EXEMPT
    else:
        if source_name in ["Fairfax County","Minneapolis"] and val_counts is not None and \
            val_counts.loc[orig]/val_counts.sum()*100 < 7:
            warnings.warn(f"Value of {orig} in civilian or officer race column is unknown and cannot be standardized. " +
                f"Cases where value is {orig} will be left unchanged. Value of {orig} occurs in {val_counts.loc[orig]} out of {val_counts.sum()} rows.")
        else:
            raise ValueError(f"Unknown value {orig}")
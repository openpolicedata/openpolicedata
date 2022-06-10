from pandas.api.types import is_datetime64_any_dtype as is_datetime
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta

try:
    from . import datetime_parser
    from . import defs
except:
    import defs
    import datetime_parser

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


def standardize(df, table_type, date_column=None, agency_column=None, source_name=None, keep_raw=True): 
    col_map = id_columns(df, table_type, date_column, agency_column, source_name=source_name)
    maps = []
    standardize_date(df, col_map, maps, keep_raw=keep_raw)
    standardize_race(df, col_map, maps, source_name, keep_raw=keep_raw)
    
    return maps

def _pattern_search(all_cols, select_cols, patterns, match_substr):
    matches = []
    for p in patterns:
        if p[0].lower() == "equals":
            matches = [x for x in select_cols if x.lower() == p[1].lower()]
        elif p[0].lower() == "contains":
            matches = [x for x in select_cols if p[1].lower() in x.lower()]
        elif p[0].lower() == "format":
            if type(match_substr) != str:
                raise TypeError("match_substr should be a string")
            guess = match_substr[0].lower()
            pattern = p[1]
            
            def find_matches(columns, guess, match_substr, idx, pattern):
                matches_nocase = [x for x in columns if x.lower() == pattern.format(guess).lower()]
                if len(matches_nocase)>0:
                    return [x for x in columns if x.lower() in matches_nocase]

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


def _find_col_matches(df, table_type, match_substr, known_col_name=None, 
    exclude_table_types=[], not_required_table_types="ALL", exclude_col_names=[], secondary_patterns=[], validator=None):

    if table_type in exclude_table_types:
        return []

    if known_col_name != None:
        if known_col_name not in df.columns:
            raise ValueError(f"{known_col_name} is not a column")
        return [known_col_name]
    else:
        if type(match_substr) == str:
            match_substr = [match_substr]

        match_cols = []
        for s in match_substr:
            match_cols = [x for x in df.columns if s.lower() in x.lower()]
            if len(match_cols)>0:
                break

        if len(match_cols)==0 and len(match_substr)>0:
            match_cols = _pattern_search(df.columns, match_cols, secondary_patterns, match_substr[0])
            
        if len(match_cols)==0:
            if not_required_table_types != "ALL" and table_type not in not_required_table_types:
                raise ValueError(f"Column not found with substring {match_substr}")
        elif len(match_cols)>1:
            new_matches = _pattern_search(df.columns, match_cols, secondary_patterns, match_substr[0])
            if len(new_matches)>0:
                match_cols = new_matches
            elif len(match_cols)==0 and len(secondary_patterns)>0:
                raise NotImplementedError()

            if len(match_cols) > 1:
                if validator != None:
                    match_cols_test = match_cols
                    match_cols = []
                    for col in match_cols_test:
                        try:
                            validator(df[col])
                            match_cols.append(col)
                        except:
                            pass

        for e in exclude_col_names:
            if e in match_cols:
                match_cols.remove(e)

        return match_cols


def _id_date_column(df, table_type, date_column, col_map):
    
    if date_column != None:
        if not is_datetime(df[date_column].dtype):
            raise TypeError(f"Date column {date_column} should contain a datetime")

        col_map[defs.columns.DATE] = date_column
    else:
        date_cols = [x for x in df.columns if "date" in x.lower()]
        if len(date_cols)==0:
            if table_type not in \
            [defs.TableType.USE_OF_FORCE_CIVILIANS_OFFICERS, defs.TableType.USE_OF_FORCE_CIVILIANS, 
            defs.TableType.EMPLOYEE, defs.TableType.USE_OF_FORCE_OFFICERS]:
                raise NotImplementedError("Find the date")
        elif len(date_cols)>1:
            date_exact = [x for x in date_cols if x.lower() == "date"]
            date_assigned = [x for x in date_cols if "assigned" in x.lower()]
            if len(date_exact) > 0:
                date_cols = date_exact
            elif len(date_assigned) > 0:
                date_cols = date_assigned
            else:
                raise NotImplementedError()

            if len(date_cols)>1:
                raise NotImplementedError()
        else:
            col_map[defs.columns.DATE] = date_cols[0]


def _id_time_column(df, col_map):
    time_cols = [x for x in df.columns if "time" in x.lower()]
    if len(time_cols) == 0 and defs.columns.DATE in col_map:
        segs = col_map[defs.columns.DATE].split("_")
        time_str = "time"
        date_str = "date"
        for k, seg in enumerate(segs):
            if seg[0].lower() == date_str[0].lower():
                idx = 1
                use_idx = [0]
                caps = [seg[0] == date_str[0].upper()]
                found = True
                for x in seg[1:].lower():
                    found = False
                    for m, y in enumerate(date_str[idx:]):
                        if x.lower() == y.lower():
                            use_idx.append(idx+m)
                            caps.append(x == y.upper())
                            idx += m+1
                            found = True
                            break

                    if not found:
                        break

                if found:
                    # Attempt to find in columns
                    col_name = ""
                    for j in range(len(segs)):
                        if j == k:
                            s = ""
                            for m, x in enumerate(use_idx):
                                val = time_str[x].lower()
                                if caps[m]:
                                    val = val.upper()
                                s+=val
                        else:
                            s = segs[j]

                        col_name += s
                        if j < len(segs)-1:
                            col_name += "_"

                    if col_name in df.columns:
                        time_cols = [col_name]
                        break
            

    if len(time_cols)>1:
        is_time = []
        for k, col in enumerate(time_cols):
            try:
                datetime_parser.parse_time_to_sec(df[col])
                is_time.append(True)
            except:
                is_time.append(False)

        time_idx = [k for k,x in enumerate(is_time) if x ]
        if len(time_idx) == 0:
            raise NotImplementedError()
        elif len(time_idx) > 1:
            raise NotImplemented()

        time_cols = time_cols[time_idx[0]:time_idx[0]+1]
                
                
    if len(time_cols) == 1:
        if defs.columns.DATE not in col_map or col_map[defs.columns.DATE] != time_cols[0]:
            col_map[defs.columns.TIME] = time_cols[0]
            if defs.columns.DATE not in col_map:
                    raise NotImplementedError()


def _id_race_column(df, table_type, source_name, col_map, race_cols):
    # Need to determine if race column is for officers or civilians

    race_found = True
    types = []
    if len(race_cols) == 0:
        known_tables_wo_race = [
            ("Montgomery County", defs.TableType.COMPLAINTS),
            ("Denver", defs.TableType.STOPS),
            ("Bloomington", defs.TableType.CALLS_FOR_SERVICE),
            ("Asheville", defs.TableType.CALLS_FOR_SERVICE),
            ("San Francisco", defs.TableType.CALLS_FOR_SERVICE),
            ("Cincinnati", defs.TableType.CALLS_FOR_SERVICE),
            ("Lincoln", defs.TableType.VEHICLE_PURSUITS)
        ]
        if table_type == defs.TableType.USE_OF_FORCE_INCIDENTS or \
            any([(source_name, table_type)==x for x in known_tables_wo_race]):
            race_found = False
        else:
            raise NotImplementedError()
            
    if race_found:
        if " - CIVILIANS/OFFICERS" in table_type:
            if "Race_Ethnic_Group" in race_cols:
                race_cols = ["Race_Ethnic_Group"]
                types = [defs.columns.RACE_OFF_AND_CIV]
            else:
                raise NotImplementedError()
        else:
            is_officer_table = table_type == defs.TableType.EMPLOYEE.value or \
                ("- OFFICERS" in table_type and "CIVILIANS" not in table_type)

            types = []
            for k in range(len(race_cols)):
                if "off" in race_cols[k].lower() or is_officer_table:
                    types.append(defs.columns.RACE_OFFICER)
                else:
                    types.append(defs.columns.RACE_CIVILIAN)

        if len(set(types)) != len(types):
            if len(race_cols)==2 and race_cols[0] == "subject_race" and "raw" in race_cols[1]:
                # Stanford data
                race_cols = race_cols[0:1]
                types = types[0:1]
            else:
                is_race_col = []
                for col in race_cols:
                    vals = [x.lower() if type(x)==str else x for x in df[col].unique()]
                    exp_vals = ["black","white","hispanic","latino","asian","b","w","h","a"]
                    if len([1 for x in vals if x in exp_vals]) > 1:
                        is_race_col.append(True)
                    else:
                        is_race_col.append(False)

                race_cols = [x for x,y in zip(race_cols, is_race_col) if y]
                types = [x for x,y in zip(types, is_race_col) if y]
                if len(set(types)) != len(types):
                    raise NotImplementedError()

        for k in range(len(race_cols)):
            col_map[types[k]] = race_cols[k]

    return types


def _id_ethnicity_column(table_type, col_map, race_types, eth_cols):
    if len(eth_cols)==0:
        return

    eth_types = []
    validation_types = []
    is_officer_table = table_type == defs.TableType.EMPLOYEE.value or \
            ("- OFFICERS" in table_type and "CIVILIANS" not in table_type)
    for k in range(len(eth_cols)):
        if "officer" in eth_cols[k].lower() or \
            "offcr" in eth_cols[k].lower() or is_officer_table:
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
        col_map[eth_types[k]] = eth_cols[k]


def _id_agency_column(agency_column, col_map):
    if agency_column != None:
        col_map[defs.columns.AGENCY] = agency_column


def id_columns(df, table_type, date_column=None, agency_column=None, source_name=None):
    col_map = {}

    match_cols = _find_col_matches(df, table_type, "date", known_col_name=date_column, 
        secondary_patterns = [("equals","date"), ("contains", "assigned")],
        not_required_table_types=[defs.TableType.USE_OF_FORCE_CIVILIANS_OFFICERS, defs.TableType.USE_OF_FORCE_CIVILIANS, 
            defs.TableType.USE_OF_FORCE_OFFICERS],
        exclude_table_types=[defs.TableType.EMPLOYEE])

    if len(match_cols) > 1:
        raise NotImplementedError()
    elif len(match_cols) == 1:
        col_map[defs.columns.DATE] = match_cols[0]

    # _id_date_column(df, table_type, date_column, col_map)
    
    check_for_time = True
    if defs.columns.DATE in col_map:
        dts = df[col_map[defs.columns.DATE]][pd.notnull(df[col_map[defs.columns.DATE]])]
        if hasattr(dts[0], "year"):
            dts = dts.dt.time.unique()
            if len(dts) > 1:
                if len(dts)==2:
                    dts = np.sort(dts)
                    # Check difference to see if it's just a UTC offset difference between standard and daylight savings time 
                    dt = datetime.combine(date.today(), dts[1]) - datetime.combine(date.today(), dts[0])
                    if dt != timedelta(hours=1):
                        check_for_time = False
                else:
                    # Time already in date
                    check_for_time = False

    if check_for_time:
        # _id_time_column(df, col_map)
        secondary_patterns = []
        if defs.columns.DATE in col_map:
            # Create a pattern from the format of the date column name
            # That might also be the pattern of the time column
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

        exclude_col_names = []
        if defs.columns.DATE in col_map:
            exclude_col_names.append(col_map[defs.columns.DATE])
        
        match_cols = _find_col_matches(df, table_type, "time", 
            secondary_patterns=secondary_patterns, validator=datetime_parser.parse_time_to_sec,
            exclude_col_names=exclude_col_names)

        if len(match_cols) > 1:
            raise NotImplementedError()
        elif len(match_cols) == 1:
            col_map[defs.columns.TIME] = match_cols[0]
        
    # California data, RAE = Race and Ethnicity
    match_cols = _find_col_matches(df, table_type, ["race", "rae_full"])
    race_types = _id_race_column(df, table_type, source_name, col_map, match_cols)

    # enthnicity is to deal with typo in Ferndale data. Consider using fuzzywuzzy in future for fuzzy matching
    match_cols = _find_col_matches(df, table_type, ["ethnicity", "enthnicity"])
    _id_ethnicity_column(table_type, col_map, race_types, match_cols)

    # _id_agency_column(agency_column, col_map)
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
                                
        df[defs.columns.DATE] = datetime_parser.parse_date_to_datetime(date_data)
        _cleanup_old_column(df, col_map[defs.columns.DATE], keep_raw)

        maps.append(DataMapping(old_column_name=col_map[defs.columns.DATE], new_column_name=defs.columns.DATE))

        if defs.columns.TIME in col_map:
            df[defs.columns.DATE] = datetime_parser.combine_date_and_time(df[defs.columns.DATE], df[col_map[defs.columns.TIME]])
            _cleanup_old_column(df, col_map[defs.columns.TIME], keep_raw)


def standardize_race(df, col_map, maps, source_name=None, keep_raw=False):
    if defs.columns.RACE_CIVILIAN in col_map:
        _standardize_race(df, col_map, maps, civilian=True, source_name=source_name, keep_raw=keep_raw)

    if defs.columns.RACE_OFFICER in col_map:
        _standardize_race(df, col_map,  maps, civilian=False, source_name=source_name, keep_raw=keep_raw)


def _standardize_race(df, col_map, maps, civilian, source_name, keep_raw):
    if civilian:
        new_race_column = defs.columns.RACE_CIVILIAN
        new_ethnicity_column = defs.columns.ETHNICITY_CIVILIAN
    else:
        new_race_column = defs.columns.RACE_OFFICER
        new_ethnicity_column = defs.columns.ETHNICITY_OFFICER

    race_column = col_map[new_race_column]
    if new_ethnicity_column in col_map:
        ethnicity_column = col_map[new_ethnicity_column]
    else:
        ethnicity_column = None

    # Decode the race column
    vals = [x.upper() if type(x)==str else x  for x in df[race_column].unique()]
    race_map_dict = {}
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
    for x in vals:
        if type(x) == str:
            x = x.replace("_", " ").replace("*","")
            x = x.strip()
        if pd.isnull(x) or x == "NOT SPECIFIED" or x=="":
            race_map_dict[x] = defs.races.UNSPECIFIED
        elif type(x) != str and source_name == "California":
            race_map_dict[x] = ca_stops_dict[x]
        elif x == "W" or x == "WHITE" or x=="WN":
            # WN = White-Non-Hispanic
            race_map_dict[x] = defs.races.WHITE
        elif x == "B" or "BLACK" in x:
            race_map_dict[x] = defs.races.BLACK
        elif x == "U" or x == "UNKNOWN":
            race_map_dict[x] = defs.races.UNKNOWN
        elif x == "ASIAN INDIAN":
            race_map_dict[x] = defs.races.ASIAN_INDIAN
        elif x == "SOUTH ASIAN" or ("INDIAN" in x and "BURMESE" in x):
            race_map_dict[x] = defs.races.SOUTH_ASIAN
        elif "PACIFIC ISLAND" in x:
            race_map_dict[x] = defs.races.AAPI
        elif x == "A" or "ASIAN" in x:
            if "INDIAN" in x:
                raise ValueError("INDIAN should be picked up by ASIAN INDIAN condition")
            elif "PACIFIC" in x:
                race_map_dict[x] = defs.races.AAPI
            else:
                race_map_dict[x] = defs.races.ASIAN
        elif "HAWAIIAN" in x:
            race_map_dict[x] = defs.races.HAWAIIAN
        elif x == "H" or "HISPANIC" in x or "LATINO" in x or x== "WH" or \
            (source_name == "New York City" and (x=="P" or x=="Q")) or \
            (source_name == "Bloomington" and (x == "L" or x=="N" or x=="P")): # This should stay below other races to not catch non-Hispanic
            # WH = White Hispanic
            # NYC and Bloomington are codes for Hispanic and a race
            race_map_dict[x] = defs.races.LATINO
        elif x == "I" or x == "NATIVE AMERICAN" or "AMERICAN IND" in x \
            or x == "ALASKAN NATIVE" or "AMER IND" in x:
            race_map_dict[x] = defs.races.NATIVE_AMERICAN
        elif x == "ME" or "MIDDLE EAST" in x:
            race_map_dict[x] = defs.races.MIDDLE_EASTERN
        elif x == "2 OR MORE":
            race_map_dict[x] = defs.races.MULTIPLE
        elif "UNK" in x and "OTH" in x:
            race_map_dict[x] = defs.races.OTHER_UNKNOWN
        elif x == "OTHER" or "OTHER UNCLASS" in x or \
            (source_name == "New York City" and x=="Z"):
            race_map_dict[x] = defs.races.OTHER
        elif source_name == "Lincoln":
            x_int = int(x)
            race_map_dict[x] = lincoln_stops_dict[x_int]
        else:
            raise ValueError(f"Unknown value {x}")

    maps.append(
        DataMapping(old_column_name=race_column, new_column_name=new_race_column,
            data_maps=race_map_dict)
    )

    df[new_race_column] = df[race_column].map(race_map_dict)
    _cleanup_old_column(df, race_column, keep_raw)

    if ethnicity_column != None:        
        vals = [x.upper() if type(x)==str else x for x in df[ethnicity_column].unique()]
        # The below values is used in the Ferndale demographics data. Just use the data from the race column in that
        # case which includes if officer is Hispanic
        ferndale_eth_vals = ['NR', 'FRENCH/GERMAN', 'MEXICAN', 'HUNGARIAN', 'LEBANESE', 'POLISH/SCOTTISH', 'IRISH', 'SYRIAN', 'POLISH']
        eth_map_dict = {}
        for x in vals:
            if pd.isnull(x):
                pass
            elif x == "N" or x == 'NON-HISPANIC' or x == "NH" or \
                x in ferndale_eth_vals:
                pass  # Just use race value
            elif x == "H" or "HISPANIC" in x or "LATINO" in x:
                eth_map_dict[x] = defs.races.LATINO
            elif x == "U" or x == "UNKNOWN":
                eth_map_dict[x] = defs.races.UNKNOWN
            else:
                raise ValueError(f"Unknown value {x}")

        maps[-1].old_column_name = [maps[-1].old_column_name, ethnicity_column]
        maps[-1].data_maps = [maps[-1].data_maps, eth_map_dict]

        def update_race(x):
            if x[ethnicity_column] in eth_map_dict:
                return eth_map_dict[x[ethnicity_column]]
            else:
                return x[new_race_column]

        df[new_race_column] = df.apply(update_race, axis=1)
        _cleanup_old_column(df, ethnicity_column, keep_raw)


import pandas as pd
import re
from collections import Counter

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
    df = standardize_date(df, col_map, maps, keep_raw=keep_raw)
    standardize_time(df, col_map, maps, keep_raw=keep_raw)
    standardize_race(df, col_map, maps, source_name, keep_raw=keep_raw, table_type=table_type)
    
    return df, maps

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
        officer_terms = ["officer","deputy"]
        civilian_terms = ["citizen","subject","suspect","civilian"]
        civilian_found = False
        officer_found = False
        for s in match_substr:
            new_matches = [x for x in df.columns if s.lower() in x.lower()]
            if len(new_matches)>0:
                if not officer_found and not civilian_found:
                    match_cols.extend(new_matches)
                elif officer_found:
                    # Only keep columns with civilian terms
                    match_cols.extend([x for x in new_matches if any([y in x for y in civilian_terms])])
                elif civilian_found:
                    # Only keep columns with officer terms
                    match_cols.extend([x for x in new_matches if any([y in x for y in officer_terms])])

                # There are cases where there should be multiple matches for both officer and community member
                # columns. On occasion, they are labeled with different terms 
                officer_found = any([any([y in x for y in officer_terms]) for x in match_cols])
                civilian_found = any([any([y in x for y in civilian_terms]) for x in match_cols])

                if civilian_found == officer_found:  # i.e. Both found or not found
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
                    score = None
                    for col in match_cols_test:
                        try:
                            new_score = validator(df[col])                           
                        except Exception as e:
                            pass
                        else:
                            if score == new_score:
                                match_cols.append(col)
                            elif new_score != None and (score == None or new_score > score):
                                # Higher scoring item found. This now takes priority
                                score = new_score
                                match_cols = [col] 

        for e in exclude_col_names:
            if e in match_cols:
                match_cols.remove(e)

        return match_cols

def _id_race_column(df, table_type, source_name, col_map, race_cols):
    # Need to determine if race column is for officers or civilians

    race_found = True
    types = []
    if len(race_cols) == 0:
        known_tables_wo_race = [
            ("Montgomery County", defs.TableType.COMPLAINTS),
            ("Denver", defs.TableType.STOPS),
            ("Lincoln", defs.TableType.VEHICLE_PURSUITS),
            ("Tuscon", defs.TableType.SHOOTINGS_INCIDENTS),
            ("Los Angeles", defs.TableType.STOPS),
            ("Austin", defs.TableType.SHOOTINGS_INCIDENTS),
            ("South Bend", defs.TableType.USE_OF_FORCE),
            ("State Police", defs.TableType.SHOOTINGS)
        ]
        if table_type == defs.TableType.USE_OF_FORCE_INCIDENTS or \
            table_type == defs.TableType.CALLS_FOR_SERVICE or \
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
                if "off" in race_cols[k].lower() or "deputy" in race_cols[k].lower() or \
                    (is_officer_table and "suspect" not in race_cols[k].lower() and "supsect" not in race_cols[k].lower()):
                    types.append(defs.columns.RACE_OFFICER)
                else:
                    types.append(defs.columns.RACE_CIVILIAN)

        if len(set(types)) != len(types):
            if len(race_cols)==2 and race_cols[0] == "subject_race" and "raw" in race_cols[1]:
                # Stanford data
                race_cols = race_cols[0:1]
                types = types[0:1]
            elif len(race_cols)==2 and all([x in ['tcole_race_ethnicity', 'standardized_race'] for x in race_cols]):
                # Austin data
                race_cols = ['tcole_race_ethnicity']
                types = types[0:1]
            else:
                orig_race_cols = race_cols
                orig_type = types
                race_cols = []
                types = []
                vals = []
                for k, col in enumerate(orig_race_cols):
                    new_col = convert_race(df[col], source_name, table_type=table_type)
                    found = False
                    for j, t in enumerate(types):
                        if t == orig_type[k] and new_col.equals(vals[j]):
                            found = True
                            break

                    if not found:
                        race_cols.append(col)
                        types.append(orig_type[k])
                        vals.append(new_col)
                # is_race_col = []
                # # for col in race_cols:
                # #     vals = [x.lower() if type(x)==str else x for x in df[col].unique()]
                # #     exp_vals = ["black","white","hispanic","latino","asian","b","w","h","a"]
                # #     if len([1 for x in vals if x in exp_vals]) > 1:
                # #         is_race_col.append(True)
                # #     else:
                # #         is_race_col.append(False)

                # race_cols = [x for x,y in zip(race_cols, is_race_col) if y]
                # types = [x for x,y in zip(types, is_race_col) if y]
                if len(set(types)) != len(types):
                    raise NotImplementedError()

        for k in range(len(race_cols)):
            col_map[types[k]] = race_cols[k]

    return race_cols, types


def _id_ethnicity_column(source_name, table_type, col_map, race_types, eth_cols):
    if len(eth_cols)==0:
        return

    eth_types = []
    validation_types = []
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
        col_map[eth_types[k]] = eth_cols[k]


def id_columns(df, table_type, date_column=None, agency_column=None, source_name=None):
    col_map = {}

    match_cols = _find_col_matches(df, table_type, "date", known_col_name=date_column, 
        secondary_patterns = [("equals","date"), ("contains", "call"), ("contains", "assigned"), ("contains", "occurred")],
        not_required_table_types=[defs.TableType.USE_OF_FORCE_CIVILIANS_OFFICERS, defs.TableType.USE_OF_FORCE_CIVILIANS, 
            defs.TableType.USE_OF_FORCE_OFFICERS, defs.TableType.SHOOTINGS_CIVILIANS, defs.TableType.SHOOTINGS_OFFICERS],
        exclude_table_types=[defs.TableType.EMPLOYEE], validator=datetime_parser.validate_date)

    if len(match_cols) > 1:
        raise NotImplementedError()
    elif len(match_cols) == 1:
        col_map[defs.columns.DATE] = match_cols[0]

    # _id_date_column(df, table_type, date_column, col_map)
    
    check_for_time = True
    # if defs.columns.DATE in col_map:
    #     dts = df[col_map[defs.columns.DATE]][pd.notnull(df[col_map[defs.columns.DATE]])]
    #     if hasattr(dts[0], "year"):
    #         dts = dts.dt.time.unique()
    #         if len(dts) > 1:
    #             if len(dts)==2:
    #                 dts = np.sort(dts)
    #                 # Check difference to see if it's just a UTC offset difference between standard and daylight savings time 
    #                 dt = datetime.combine(date.today(), dts[1]) - datetime.combine(date.today(), dts[0])
    #                 if dt != timedelta(hours=1):
    #                     check_for_time = False
    #             else:
    #                 # Time already in date
    #                 check_for_time = False

    if check_for_time:
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

        exclude_col_names = []
        if defs.columns.DATE in col_map:
            exclude_col_names.append(col_map[defs.columns.DATE])
        
        match_cols = _find_col_matches(df, table_type, "time", 
            secondary_patterns=secondary_patterns, validator=datetime_parser.validate_time,
            exclude_col_names=exclude_col_names)

        if len(match_cols) > 1:
            raise NotImplementedError()
        elif len(match_cols) == 1:
            col_map[defs.columns.TIME] = match_cols[0]
        
    # California data, RAE = Race and Ethnicity
    def race_validator(col):
        convert_race(col, source_name, table_type=table_type)
        return None

    match_cols = _find_col_matches(df, table_type, ["race", "descent","rae_full","citizen_demographics","officer_demographics","ethnicity"],
        validator=race_validator)  
    race_cols, race_types = _id_race_column(df, table_type, source_name, col_map, match_cols)

    # enthnicity is to deal with typo in Ferndale data. Consider using fuzzywuzzy in future for fuzzy matching
    match_cols = _find_col_matches(df, table_type, ["ethnic", "enthnicity"], exclude_col_names=race_cols)
    _id_ethnicity_column(source_name, table_type, col_map, race_types, match_cols)

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
                                
        s_date = datetime_parser.parse_date_to_datetime(date_data)
        s_date.name = defs.columns.DATE
        df = pd.concat([df, s_date], axis=1)
        _cleanup_old_column(df, col_map[defs.columns.DATE], keep_raw)

        maps.append(DataMapping(old_column_name=col_map[defs.columns.DATE], new_column_name=defs.columns.DATE))

    return df


def standardize_time(df, col_map, maps, keep_raw=False):
    if defs.columns.TIME in col_map:
        df[defs.columns.TIME] = datetime_parser.parse_time(df[col_map[defs.columns.TIME]])
        _cleanup_old_column(df, col_map[defs.columns.TIME], keep_raw)

        maps.append(DataMapping(old_column_name=col_map[defs.columns.TIME], new_column_name=defs.columns.TIME))
    # Commenting this out. Trying to keep time column as local time to enable day vs. night analysis.
    # Date column is often in UTC but it's not easy to tell when that is the case nor what the local timezone is 
    # if UTC needs converted
    # We are assuming that the time column is already local
    # elif defs.columns.DATE in col_map:
    #     tms = df[defs.columns.DATE].dt.time
    #     # Check if time information is just a UTC offset which can have 2 values due to daylight savings time
    #     if len(tms.unique()) > 3: 
    #         # Generate time column from date
    #         df[defs.columns.TIME] = tms


def standardize_race(df, col_map, maps, source_name=None, keep_raw=False, table_type=None):
    if defs.columns.RACE_CIVILIAN in col_map:
        _standardize_race(df, col_map, maps, civilian=True, source_name=source_name, keep_raw=keep_raw, table_type=table_type)

    if defs.columns.RACE_OFFICER in col_map:
        _standardize_race(df, col_map,  maps, civilian=False, source_name=source_name, keep_raw=keep_raw, table_type=table_type)


def _standardize_race(df, col_map, maps, civilian, source_name, keep_raw, table_type):
    if civilian:
        new_race_column = defs.columns.RACE_CIVILIAN
        new_ethnicity_column = defs.columns.ETHNICITY_CIVILIAN
    else:
        new_race_column = defs.columns.RACE_OFFICER
        new_ethnicity_column = defs.columns.ETHNICITY_OFFICER

    race_column = col_map[new_race_column]
    if new_ethnicity_column in col_map and new_race_column in col_map and \
        col_map[new_ethnicity_column] != col_map[new_race_column]:
        ethnicity_column = col_map[new_ethnicity_column]
    else:
        ethnicity_column = None

    race_map_dict = {}
    df[new_race_column] = convert_race(df[race_column], source_name, race_map_dict=race_map_dict, table_type=table_type)
    
    maps.append(
        DataMapping(old_column_name=race_column, new_column_name=new_race_column,
            data_maps=race_map_dict)
    )

    if race_column not in ["citizen_demographics","officer_demographics"]:  # These contain additional information that still needs to be used
        _cleanup_old_column(df, race_column, keep_raw)

    if ethnicity_column != None:        
        vals = [x if type(x)==str else x for x in df[ethnicity_column].unique()]
        # The below values is used in the Ferndale demographics data. Just use the data from the race column in that
        # case which includes if officer is Hispanic
        ferndale_eth_vals = ['NR', 'FRENCH/GERMAN', 'MEXICAN', 'HUNGARIAN', 'LEBANESE', 'POLISH/SCOTTISH', 'IRISH', 'SYRIAN', 'POLISH']
        eth_map_dict = {}
        for x in vals:
            orig = x
            if type(x) == str:
                x = x.upper().replace("-","").replace(" ", "")
                
            if pd.isnull(x):
                pass
            elif x == "N" or x == 'NONHISPANIC' or x == "NH" or "NOTHISP" in x or \
                x in ferndale_eth_vals:
                pass  # Just use race value
            elif x == "H" or "HISPANIC" in x or "LATINO" in x:
                if "NON" in x:
                    raise ValueError(x)
                eth_map_dict[orig] = defs.races.LATINO
            elif x in ["U", "UNKNOWN"]:
                eth_map_dict[orig] = defs.races.UNKNOWN
            elif "NODATA" in x.replace(" ","") or \
                "MARSY’SLAWEXEMPT" in x.replace(" ",""):
                eth_map_dict[orig] = defs.races.UNSPECIFIED
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

def _is_demo_list(vals):
    check_gender = ['male' in x.lower() for x in vals if type(x)==str]
    return sum(check_gender) > 1

def convert_race(col, source_name, race_map_dict=None, table_type=None):

    if race_map_dict == None:
        race_map_dict = {}
    # Decode the race column
    vals = [x if type(x)==str else x  for x in col.unique()]

    delims = [",", "|", ";"]
    max_idx = -1
    delims_count = 0
    repeat_count = -1
    is_demo_list = False
    if table_type in [defs.TableType.SHOOTINGS, defs.TableType.SHOOTINGS_CIVILIANS, defs.TableType.SHOOTINGS_OFFICERS,
        defs.TableType.SHOOTINGS_INCIDENTS, defs.TableType.USE_OF_FORCE, defs.TableType.USE_OF_FORCE_INCIDENTS,
        defs.TableType.USE_OF_FORCE_CIVILIANS_OFFICERS, defs.TableType.USE_OF_FORCE_INCIDENTS, 
        defs.TableType.USE_OF_FORCE_CIVILIANS]: 
        # Evaluate to see if values are for a single person or multiple
        def count_delims(x, d):
            return sum([1 for y in x if y==d])
        def count_repeats(x, d):
            ind_vals = x.split(d)
            ind_vals = [y.strip() for y in ind_vals if type(y) == str]
            counts = Counter(ind_vals)
            return sum([c for c in counts.values() if c>1])

        # Check if vals are lists of tuples
        is_demo_list = _is_demo_list(vals)
        if not is_demo_list:
            for k,d in enumerate(delims):
                total_delims = sum([count_delims(x,d) for x in vals if type(x) == str])
                if total_delims > delims_count:
                    max_idx = k
                    delims_count = total_delims
                    repeat_count = sum([count_repeats(x, d) for x in vals if type(x) == str])  

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
    # Austin values below are based on email with dataset owner who said they were from some source. 
    #   Do these values apply to other datasets using the same values?
    bad_data = ["MALE"]

    if is_demo_list:
        map = {}
        for x in vals:
            if type(x) == str:
                items = x.split("(")
                map_list = []
                for k, i in enumerate(items[1:]):
                    i = i.split(",")[0].strip()
                    if i not in race_map_dict:
                        _create_race_lut(i, race_map_dict, source_name, ca_stops_dict, lincoln_stops_dict, bad_data)
                    map_list.append(race_map_dict[i])

                map[x] = map_list
            else:
                if x not in race_map_dict:
                    _create_race_lut(x, race_map_dict, source_name, ca_stops_dict, lincoln_stops_dict, bad_data)
                    map[x] = race_map_dict[x]

        return col.map(map)
    elif repeat_count > 0 or delims_count > 0.75*len(vals):
        map = {}
        for x in vals:
            if type(x) == str:
                items = x.split(delims[max_idx])
                for k, i in enumerate(items):
                    if i == "ISL":  # LA County code for AAPI is ASIAN-PACIFIC,ISL
                        continue
                    i = i.strip()
                    if i not in race_map_dict:
                        _create_race_lut(i, race_map_dict, source_name, ca_stops_dict, lincoln_stops_dict, bad_data)
                    items[k] = race_map_dict[i]

                map[x] = items
            else:
                if x not in race_map_dict:
                    _create_race_lut(x, race_map_dict, source_name, ca_stops_dict, lincoln_stops_dict, bad_data)
                    map[x] = race_map_dict[x]

        return col.map(map)
    else:
        for x in vals:
            _create_race_lut(x, race_map_dict, source_name, ca_stops_dict, lincoln_stops_dict, bad_data)

        return col.map(race_map_dict)

def _create_race_lut(x, race_map_dict, source_name, ca_stops_dict, lincoln_stops_dict, bad_data):
    orig = x
    if type(x) == str:
        x = x.upper().replace("_", " ").replace("*","").replace("-"," ").replace(".","")
        x = x.strip()
    if pd.isnull(x) or x in ["NOT SPECIFIED","NULL VALUE", "", 'MARSY’S LAW EXEMPT', ""] or \
        (type(x)==str and ("NO DATA" in x or "NOT APPLICABLE" in x)) or x in bad_data:
        race_map_dict[orig] = defs.races.UNSPECIFIED
    elif type(x) != str and source_name == "California":
        race_map_dict[orig] = ca_stops_dict[x]
    elif x in ["W", "CAUCASIAN", "WN"] or x.replace(", OTHER", "") == "WHITE":
        # WN = White-Non-Hispanic
        race_map_dict[orig] = defs.races.WHITE
    elif x in ["B", "AFRICAN AMERICAN"] or "BLACK" in x:
        if x.count("BLACK") > 1:
            raise ValueError(f"The value of {x} likely contains the races for multiple people")
        race_map_dict[orig] = defs.races.BLACK
    elif x in ["U", "UNKNOWN", "UNK", "RACE UNKNOWN", "UK"]:
        race_map_dict[orig] = defs.races.UNKNOWN
    elif x.replace(", OTHER", "") == "ASIAN INDIAN":
        race_map_dict[orig] = defs.races.ASIAN_INDIAN
    elif x == "SOUTH ASIAN" or ("INDIAN" in x and "BURMESE" in x):
        race_map_dict[orig] = defs.races.SOUTH_ASIAN        
    elif x == "A" or "ASIAN" in x:
        if "INDIAN" in x:
            raise ValueError("INDIAN should be picked up by ASIAN INDIAN condition")
        elif "PACIFIC" in x:
            race_map_dict[orig] = defs.races.AAPI
        else:
            race_map_dict[orig] = defs.races.ASIAN
    elif "HAWAIIAN" in x or "PACIFICISLAND" in x.replace(" ","").replace("_","") or \
        x in ["FILIPINO"] or \
        (source_name=="Austin" and x=="P"):
        race_map_dict[orig] = defs.races.HAWAIIAN
    elif x in ["H", "WH"] or ("HISPANIC" in x and "NONHISPANIC" not in x) or "LATINO" in x or \
        (source_name == "New York City" and (x in ["P", "Q"])) or \
        (source_name == "Bloomington" and (x in ["L", "N", "P"])): # This should stay below other races to not catch non-Hispanic
        # WH = White Hispanic
        # NYC and Bloomington are codes for Hispanic and a race
        race_map_dict[orig] = defs.races.LATINO
    elif x in ["I", "NATIVE AMERICAN", "INDIAN", "ALASKAN NATIVE"] or "AMERICAN IND" in x \
        or "AMER IND" in x or \
            (source_name=="Austin" and x=="N"):
        race_map_dict[orig] = defs.races.NATIVE_AMERICAN
    elif x == "ME" or "MIDDLE EAST" in x or \
        (source_name=="Austin" and x=="M"):
        race_map_dict[orig] = defs.races.MIDDLE_EASTERN
    elif x in ["2 OR MORE", "MULTI DESCENTS", "TWO OR MORE RACES"] or \
        x.replace(" ","") == "BIRACIAL":
        race_map_dict[orig] = defs.races.MULTIPLE
    elif "UNK" in x and "OTH" in x:
        race_map_dict[orig] = defs.races.OTHER_UNKNOWN
    elif x in ["O","OTHER"] or "OTHER UNCLASS" in x or \
        (source_name == "New York City" and x=="Z"):
        race_map_dict[orig] = defs.races.OTHER
    elif source_name == "Lincoln":
        x_int = int(x)
        race_map_dict[orig] = lincoln_stops_dict[x_int]
    elif source_name == "Chapel Hill" and x == "M":
        race_map_dict[orig] = orig
    else:
        raise ValueError(f"Unknown value {x}")
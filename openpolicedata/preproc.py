from pandas.api.types import is_datetime64_any_dtype as is_datetime
import pandas as pd

try:
    from . import defs
except:
    import defs

class DataMapping:
    old_column_name = None  # Original name of columns
    new_column_name = None  # Renamed columns
    raw_column_name = None  # Utilized if orignal column was copied to a new column (mostly if mapping is not reversible)
    data_maps = None        # Map(s) from old data to new data

    def __init__(self, old_column_name=None, new_column_name = None, raw_column_name=None, data_maps=None):
        self.old_column_name = old_column_name
        self.new_column_name = new_column_name
        self.raw_column_name = raw_column_name
        self.data_maps = data_maps

    def __str__(self) -> str:
        p = f"Old Column Name(s): {self.old_column_name}\n" + \
            f"New Column Name(s): {self.new_column_name}\n" + \
            f"Raw Column Name(s): {self.raw_column_name}\n" + \
            f"Data map(s): {self.data_maps}\n"

        return p

def standardize(df, table_type, date_column=None, agency_column=None, source_name=None): 
    col_map = id_columns(df, table_type, date_column, agency_column)
    maps = []
    standardize_date(df, col_map, maps)
    standardize_race(df, col_map, maps, source_name)
    
    return maps

def id_columns(df, table_type, date_column=None, agency_column=None):
    col_map = {}
    if date_column != None:
        if not is_datetime(df[date_column].dtype):
            raise TypeError(f"Date column {date_column} should contain a datetime")

        col_map[defs.columns.DATE] = date_column
    else:
        raise NotImplementedError("Find the date")

    race_cols = [x for x in df.columns if "race" in x.lower()]

    if len(race_cols) == 0:
        raise NotImplementedError()

    if " - CIVILIANS/OFFICERS" in table_type:
        raise NotImplementedError()

    is_officer_table = table_type == defs.TableType.EMPLOYEE.value or \
        ("- OFFICERS" in table_type and "CIVILIANS" not in table_type)

    types = []
    for k in range(len(race_cols)):
        if "officer" in race_cols[k].lower() or \
            "offcr" in race_cols[k].lower() or is_officer_table:
            types.append(defs.columns.OFFICER_RACE)
        else:
            types.append(defs.columns.CIVILIAN_RACE)

    if len(set(types)) != len(types):
        raise NotImplementedError()

    for k in range(len(race_cols)):
        col_map[types[k]] = race_cols[k]

    eth_cols = [x for x in df.columns if "ethnicity" in x.lower()]
    eth_types = []
    validation_types = []
    for k in range(len(eth_cols)):
        if "officer" in eth_cols[k].lower() or \
            "offcr" in eth_cols[k].lower() or is_officer_table:
            eth_types.append(defs.columns.OFFICER_ETHNICITY)
            validation_types.append(defs.columns.OFFICER_RACE)
        else:
            eth_types.append(defs.columns.CIVILIAN_ETHNICITY)
            validation_types.append(defs.columns.CIVILIAN_RACE)

    if len(set(eth_types)) != len(eth_types):
        raise NotImplementedError()

    if any([x not in types for x in validation_types]):
        raise NotImplementedError()

    for k in range(len(eth_cols)):
        col_map[eth_types[k]] = eth_cols[k]

    if agency_column != None:
        col_map[defs.columns.AGENCY] = agency_column

    return col_map

def standardize_date(df, col_map, maps):
    if defs.columns.DATE in col_map:
        df.rename(columns={col_map[defs.columns.DATE] : defs.columns.DATE}, inplace=True)

        maps.append(DataMapping(old_column_name=col_map[defs.columns.DATE], new_column_name=defs.columns.DATE))

def standardize_race(df, col_map, maps, source_name=None):
    if defs.columns.CIVILIAN_RACE in col_map:
        _standardize_race(df, col_map, maps, civilian=True, source_name=source_name)

    if defs.columns.OFFICER_RACE in col_map:
        _standardize_race(df, col_map,  maps, civilian=False, source_name=source_name)

def _standardize_race(df, col_map, maps, civilian, source_name):
    if civilian:
        new_race_column = defs.columns.CIVILIAN_RACE
        new_ethnicity_column = defs.columns.CIVILIAN_ETHNICITY
    else:
        new_race_column = defs.columns.OFFICER_RACE
        new_ethnicity_column = defs.columns.OFFICER_ETHNICITY

    race_column = col_map[new_race_column]
    if new_ethnicity_column in col_map:
        ethnicity_column = col_map[new_ethnicity_column]
    else:
        ethnicity_column = None

    # Decode the race column
    vals = [x.upper() for x in df[race_column].unique()]
    race_map_dict = {}
    for x in vals:
        x = x.replace("_", " ")
        if x == "W" or x == "WHITE":
            race_map_dict[x] = defs.races.WHITE
        elif x == "B" or "BLACK" in x:
            race_map_dict[x] = defs.races.BLACK
        elif x == "U" or x == "UNKNOWN":
            race_map_dict[x] = defs.races.UNKNOWN
        elif x == "ASIAN INDIAN":
            race_map_dict[x] = defs.races.ASIAN_INDIAN
        elif x == "A" or "ASIAN" in x:
            if "INDIAN" in x:
                raise ValueError("INDIAN should be picked up by ASIAN INDIAN condition")
            elif "PACIFIC" in x:
                race_map_dict[x] = defs.races.AAPI
            else:
                race_map_dict[x] = defs.races.ASIAN
        elif "HAWAIIAN" in x:
            race_map_dict[x] = defs.races.HAWAIIAN
        elif x == "H" or "HISPANIC" in x or "LATINO" in x or \
            (source_name == "Bloomington" and (x == "L" or x=="N" or x=="P")): # This should stay below other races to not catches non-Hispanic
            race_map_dict[x] = defs.races.LATINO
        elif x == "I" or "INDIAN" in x or x == "NATIVE AMERICAN":
            race_map_dict[x] = defs.races.NATIVE_AMERICAN
        elif x == "ME" or x == "MIDDLE EASTERN":
            race_map_dict[x] = defs.races.MIDDLE_EASTERN
        elif x == "NOT SPECIFIED":
            race_map_dict[x] = defs.races.UNSPECIFIED
        elif x == "2 OR MORE":
            race_map_dict[x] = defs.races.MULTIPLE
        elif "UNK" in x and "OTH" in x:
            race_map_dict[x] = defs.races.OTHER_UNKNOWN
        elif x == "OTHER":
            race_map_dict[x] = defs.races.OTHER
        else:
            raise ValueError(f"Unknown value {x}")

    maps.append(
        DataMapping(old_column_name=race_column, new_column_name=new_race_column,
            data_maps=race_map_dict)
    )
    orig_col = df[race_column].copy()
    df.rename(columns={race_column : new_race_column}, inplace=True)
    df[new_race_column] = df[new_race_column].map(race_map_dict)

    if ethnicity_column != None:        
        vals = [x.upper() if type(x)==str else x for x in df[ethnicity_column].unique()]
        eth_map_dict = {}
        for x in vals:
            if pd.isnull(x):
                pass
            elif x == "N" or x == 'NON-HISPANIC':
                pass  # Just use race value
            elif x == "H" or "HISPANIC" in x or "LATINO" in x:
                eth_map_dict[x] = defs.races.LATINO
            elif x == "U" or x == "UNKNOWN":
                eth_map_dict[x] = defs.races.UNKNOWN
            else:
                raise ValueError(f"Unknown value {x}")

        raw_name = "RAW_" + new_race_column
        maps[-1].raw_column_name = [raw_name]
        df[raw_name] = orig_col

        raw_eth_name = "RAW_" + new_ethnicity_column
        maps[-1].old_column_name = [maps[-1].old_column_name, ethnicity_column]
        maps[-1].raw_column_name.append(raw_eth_name)
        maps[-1].data_maps = [maps[-1].data_maps, eth_map_dict]

        df[raw_eth_name] = df[ethnicity_column]
        df.drop(columns=ethnicity_column, inplace=True)
        def update_race(x):
            if x[raw_eth_name] in eth_map_dict:
                return eth_map_dict[x[raw_eth_name]]
            else:
                return x[new_race_column]

        df[new_race_column] = df.apply(update_race, axis=1)


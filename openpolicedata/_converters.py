from datetime import datetime
from numbers import Number
import re
import pandas as pd

try:
    from . import defs
    from . import utils
    from._preproc_utils import MultType
except:
    import defs
    from _preproc_utils import MultType
    import utils


# Age range XX - YY
_p_age_range = re.compile(r"""
    ^                   # Start of string
    (\d+)               # Lower bound of age range. Capture for reuse.
    \s?(-|_|TO)-?\s?          # - or _ between lower and upper bound with optional spaces 
    (\d+)               # Upper bound of age range. Capture for reuse.
    (\s?[-_]\s?\1\s?[-_]\s?\3)?  # Optional mistaken repeat of same pattern. 
    $                   # End of string
    """, re.VERBOSE)

_p_nonlatino = re.compile(r'[\s,]*NON\-?(HISPANIC|LATINO)(\s|$)+')


def convert(converter, col, source_name="", cats=None, std_map=None, delim=None, mult_type=None, item_num=None, 
            no_id="keep", agg_cat=False):
    std_map = {} if std_map is None else std_map
    no_id = no_id.lower()
    if no_id not in ["keep", "null", "error","test"]:
        raise ValueError(f"no_id is {no_id}. It should be 'keep', 'null', or 'error'.")
    
    is_boolean_cols = False
    if len(col.shape)==2: # More than 1 column was passed in
        is_boolean_cols = (col.isnull() | col.isin(['True','False','true','false', True, False,'Yes','No'])).all().all()
        if is_boolean_cols and mult_type!=MultType.SINGLE:
            raise NotImplementedError(f"Multi-column input is not possible for {mult_type} case. "+
                                      "Please submit an issue: https://github.com/openpolicedata/openpolicedata/issues")
        if not is_boolean_cols:
            if col.shape[1]==2:
                # 2 columns are to be merged because 1 or the other is always null
                col_new = col[col.columns[0]].copy()
                is_null = col_new.isnull()
                col_new[is_null] = col[col.columns[1]][is_null]
                col = col_new
            else:
                raise ValueError(f"Unknown multi-column input to convert with columns {col.columns}")
    
    # value_counts handles more data types when getting unique values than .unique()
    counts = col.value_counts(dropna=False)
    vals = counts.index
    if mult_type == MultType.DICT:
        return std_dict(col, std_map, converter, no_id, source_name, cats, agg_cat)
    elif mult_type == MultType.DEMO_COL:
        return std_demo_col(col, vals, std_map, item_num, converter, no_id, source_name, cats, agg_cat)
    elif mult_type == MultType.COUNTS:
        return std_counts(col, vals, std_map, delim, converter, no_id, source_name, cats, agg_cat)
    elif mult_type == MultType.DELIMITED:
        return std_list(col, vals, std_map, delim, converter, no_id, source_name, cats, agg_cat, known_single=True)
    elif mult_type == MultType.WITH_NAME:
        return std_with_names(col, vals, std_map, item_num, converter, no_id, source_name, cats, agg_cat)
    elif mult_type == MultType.WITH_COUNTS:
        return std_with_counts(col, vals, std_map, delim, converter, no_id, source_name, cats, agg_cat)
    else:
        if is_boolean_cols:
            # This currently has only been tested on and needed for Bloomington use of force data
            col_names = col.columns
            vals = []
            for c in col_names:
                words = utils.split_words(c)
                last = ""
                v = []
                is_latino = False
                for w in words:
                    w = w.lower()
                    if last=='non':
                        last = ''
                    elif w=='non':
                        last = w
                    elif w in ['hispanic','latino']:
                        is_latino = True
                    elif w not in ['suspect', 'race', 'gender','perceived']:
                        v.append(w)

                if len(v)==0 and not is_latino:
                    raise ValueError(f"Unable to parse column name {c}")
                v = ' '.join(v)
                if is_latino:
                    v = v +", Latino" if len(v) else 'Latino'
                vals.append(v)

        for x in vals:
            std_map[x] = converter(x, no_id, source_name, cats, agg_cat)
            if isinstance(std_map[x], list):
                std_map[x].sort()
                std_map[x] = ", ".join(std_map[x])
        
        if is_boolean_cols:
            std_map = {c:std_map[v] for c,v in zip(col_names, vals)}
            def convert_bools(x):
                out = set()
                for c in x.index:
                    if x[c] in ['true','True', 'Yes', 'yes'] or x[c]==True:
                        if isinstance(std_map[c],list):
                            out.update(std_map[c])
                        else:
                            out.add(std_map[c])

                out = list(out)
                if len(out)==1:
                    return out[0]
                elif len(out)>1:
                    if agg_cat and defs.get_race_cats()[defs._race_keys.LATINO] in out:
                        return defs.get_race_cats()[defs._race_keys.LATINO]
                    else:
                        return ", ".join(out)
                else:
                    return defs._race_keys.UNSPECIFIED

            return col.apply(convert_bools, axis=1)
        else:
            return col.map(std_map)
    

def convert_off_or_civ(x, no_id, *args, **kwargs):
    orig = x
    if isinstance(x,str):
        x = x.upper()

    if pd.isnull(x) or x in ["MISSING"]:
        return defs._roles.UNSPECIFIED
    elif x in ["OFFICER"]:
        return defs._roles.OFFICER
    elif x in ["SUBJECT","CIVILIAN",'CITIZEN']:
        return defs._roles.SUBJECT
    elif no_id in ["error","test"]:
        raise ValueError(f"Unknown person type {orig}")
    else:
        return orig if no_id=="keep" else ""
    

def _create_age_range_lut(x, no_id, source_name, *args, **kwargs):
    if isinstance(x,str) and "," in x:
        # Look for ages of multiple people
        out = {}
        for k,y in enumerate(x.split(',')):
            try:
                out[k] = _create_age_range_lut(y, "error", source_name)
            except Exception as e:
                break
        else:
            return out

    p_plus = re.compile(r"(\d+)\+\s?-?\s?(\1\+)?",re.IGNORECASE)
    p_plus2 = re.compile(r"(\d+)\s*plus",re.IGNORECASE)
    p_over = re.compile(r"(OVER|>)\s?(\d+)",re.IGNORECASE)
    p_under = re.compile(r"(UNDER|<|BELOW)\s?(\d+)",re.IGNORECASE)
    p_under2 = re.compile(r"(\d+) (AND|&) (UNDER|YOUNGER)",re.IGNORECASE)
    p_above = re.compile(r"(\d+) (AND|&) (ABOVE|OLDER)",re.IGNORECASE)
    p_decade = re.compile(r"^(\d+)s$", re.IGNORECASE)
    p_range = re.compile(r"^(\d+)\s?<(=?)\s?\w+\s?<(=?)\s?(\d+)?$", re.IGNORECASE)

    orig=x
    if type(x)==str:
        x = x.upper().strip()
        x = re.sub(r'[A-Z] \- ','', x)  # Oakland Stops data includes a category label up front such as: 'B - 18-29'. Remove this.
    if isinstance(x,str) and _p_age_range.search(x)!=None:
        return _p_age_range.sub(r"\1-\3", x)
    elif isinstance(x,str) and p_over.search(x)!=None:
        return p_over.sub(r"\2-120", x)
    elif isinstance(x,str) and p_plus.search(x)!=None:
        return p_plus.sub(r"\1-120", x)
    elif isinstance(x,str) and p_plus2.search(x)!=None:
        return p_plus2.sub(r"\1-120", x)
    elif isinstance(x,str) and p_above.search(x)!=None:
        return p_above.sub(r"\1-120", x)
    elif isinstance(x,str) and p_under.search(x)!=None:
        return p_under.sub(r"0-\2", x)
    elif isinstance(x,str) and p_under2.search(x)!=None:
        return p_under2.sub(r"0-\1", x)
    elif isinstance(x,str) and p_decade.search(x)!=None:
        decade = int(p_decade.search(x).group(1))
        return f"{decade}-{decade+9}"
    elif isinstance(x,str) and p_range.search(x)!=None:
        results = p_range.search(x).groups()
        start = int(results[0]) if results[1]=="=" else int(results[0])+1
        stop = int(results[3]) if results[2]=="=" else int(results[3])-1
        return f"{start}-{stop}"
    elif pd.isnull(x) or (isinstance(x,str) and (x.replace(" ","") in ["","NR","-"] or "NO DATA" in x)):
        return ""
    elif x=="ADULT":
        return "18-120"
    elif x=="JUVENILE":
        return "0-17"
    elif isinstance(x,Number) or x.isdigit() or x in ["UNKNOWN","N/A"]:
        return orig
    else:
        # At least 1 case was found where age range was auto-corrected to a month/year like
        # 10-17 to 17-Oct. Check for this before giving up.
        try:
            return datetime.strftime(datetime.strptime(x, "%d-%b"),"%m-%d")
        except:
            if no_id=="keep" or (no_id=="test" and x in ["#VALUE!", "NOT AVAILABLE", "OTHER"]):
                return orig
            elif no_id=="error":
                raise ValueError(f"Unknown val {x} for age range")
            else:
                # no_id == "null"
                return ""
    

def _create_ethnicity_lut(x, no_id, source_name, eth_cats, *args, **kwargs):
    # The below values is used in the Ferndale demographics data. Just use the data from the race column in that
    # case which includes if officer is Hispanic
    ferndale_eth_vals = ['NR', 'FRENCH/GERMAN', 'MEXICAN', 'HUNGARIAN', 'LEBANESE', 'POLISH/SCOTTISH', 'IRISH', 'SYRIAN', 'POLISH']
    orig = x
    if isinstance(x,str):
        # Look for {Full name} {- or =} {Abbreviation_Initial}
        abbrev_full_match = re.search(r"^([\w\s/\.-]+)\s?[-]\s?([\w\s/\.])$",x)
        if abbrev_full_match and any([len(x)==1 for x in abbrev_full_match.groups()]):
            x = [x for x in abbrev_full_match.groups() if len(x)>1][0]

        x = x.upper().replace("-","").replace(" ", "").strip()

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
            # Same result whether no_id is keep or null
            return ""
        
    if has_nonlat and (x in ["N", "NH", "NHIS",'NONE'] or "NOTHISP" in x or "NONHIS" in x or "NONLATINO" in x):
        return eth_cats[defs._eth_keys.NONLATINO]
    if has_latino and (x in ["H","HIS","LAT"] or "HISPANIC" in x or "LATINO" in x):
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
        return orig if no_id=="keep" else ""


def _create_race_lut(x, no_id, source_name, race_cats=defs.get_race_cats(), agg_cat=False, known_single=False):
    if race_cats is None:
        race_cats = defs.get_race_cats()

    # Create flags to indicate which keys are in race_cats
    has_unspecified = defs._race_keys.UNSPECIFIED in race_cats
    has_aapi = defs._race_keys.AAPI in race_cats
    has_asian = defs._race_keys.ASIAN in race_cats
    has_black = defs._race_keys.BLACK in race_cats
    has_indigenous = defs._race_keys.INDIGENOUS in race_cats
    has_me = defs._race_keys.MIDDLE_EASTERN in race_cats or defs._race_keys.MIDDLE_EASTERN_SOUTH_ASIAN in race_cats
    has_me_or_sa = defs._race_keys.MIDDLE_EASTERN_SOUTH_ASIAN in race_cats
    has_pi = defs._race_keys.PACIFIC_ISLANDER in race_cats
    has_multiple = defs._race_keys.MULTIPLE in race_cats
    has_south_asian = defs._race_keys.SOUTH_ASIAN in race_cats or defs._race_keys.MIDDLE_EASTERN_SOUTH_ASIAN in race_cats
    has_latino = defs._race_keys.LATINO in race_cats
    has_white = defs._race_keys.WHITE in race_cats

    orig = x

    if ((not isinstance(x, str) and pd.notnull(x)) or (isinstance(x, str) and x.isdigit())) and source_name in ["California", "Lincoln"]:
        # Translate datasets with numeric codes for race
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
                return orig if no_id=="keep" else ""
        else:
            # Replace numerical code with default value
            x = map_dict[x]

    if pd.isnull(x):
        if has_unspecified:
            return race_cats[defs._race_keys.UNSPECIFIED]
        elif no_id=="error":
            raise ValueError(f"Null value found in race column: {orig}")
        else:
            # Same result whether no_id is keep or null
            return ""
                 
    x = x.strip().upper()

    if source_name in ["Austin", "Bloomington", "New York City", "St. John", "Louisville", "Charleston", 
                        "Los Angeles", "Dallas","Chicago", 'Cedar Lake', 'Griffith']:
        # Handling dataset-specific letter codes
        if source_name=="Austin":
            # Per email with Kruemcke, Adrian <Adrian.Kruemcke@austintexas.gov> on 2022-06-17
            map_dict = {"M":"Middle Eastern", "P":"Pacific Islander/Native Hawaiian", "N":"Native American/Alaskan", "O":"Other"}
        elif source_name=="Bloomington":
            # https://data.bloomington.in.gov/Police/Citizen-Complaints/kit3-8bua/about_data
            map_dict = {
                "A": "Asian/Pacific Island, Non-Hispanic", 
                "B": "African American, Non-Hispanic", 
                "C": "Hawaiian/Other Pacific Island, Hispanic", 
                "H": "Hawaiian/Other Pacific Island, Non-Hispanic", 
                "I": "Indian/Alaskan Native, Non-Hispanic", 
                "K": "African American, Hispanic", 
                "L": "Caucasian, Hispanic", 
                "N": "Indian/Alaskan Native, Hispanic", 
                "P": "Asian/Pacific Island, Hispanic", 
                "S": "Asian, Non-Hispanic", 
                "T": "Asian, Hispanic", 
                "U": "Unknown", 
                "W": "Caucasian, Non-Hispanic"
            } 
        elif source_name == "New York City":
            # https://www.nyc.gov/assets/nypd/downloads/zip/analysis_and_planning/stop-question-frisk/SQF-File-Documentation.zip
            map_dict = {"P":"Black-Hispanic", "Q":"White-Hispanic", "X":"Unknown","Z":"Other"}
        elif source_name in ["St. John",'Cedar Lake', 'Griffith']:
            # https://www.openpolicedata.com/StJohnIN/Arrests/2014Arrests.php
            map_dict = {"K":"Black/Hispanic", "L":"White/Hispanic/Latin","M":"Multiracial","N":"Asian Indian"}
        elif source_name == "Louisville":
            map_dict = {"A":"Asian/Pacific Islander", "U":"Undeclared", "IB":"Indian/India/Burmese", "M":"Middle Eastern Descent","AN":"Alaskan Native"}
        elif source_name=="Charleston":
            # Based on correspondance with City of Charleston Ombudsman
            map_dict = {"A":"Asian or Pacific Islander", "AI": "Alaskan or American Indian", "AP":"Asian or Pacific Islander",
                        "BK":"Black","MR":"Multi-Racial","AO":"Other"}
        elif source_name=="Los Angeles":
            # https://data.lacity.org/Public-Safety/Traffic-Collision-Data-from-2010-to-Present/d5tf-ez2w
            map_dict = {"A":"Other Asian", "B":"Black", "C":"Chinese", "D":"Cambodian", "F":"Filipino", 
                        "G":"Guamanian", "H":"Hispanic/Latin/Mexican", "I":"American Indian/Alaskan Native", 
                        "J":"Japanese", "K":"Korean", "L":"Laotian", "O":"Other", "P":"Pacific Islander", 
                        "S":"Samoan", "U":"Hawaiian", "V":"Vietnamese", "W":"White", "X":"Unknown", "Z":"Asian Indian"}
        elif source_name=="Dallas":
            # Based on Bloomington and St. John usage as well as names associated with usage
            map_dict = {"L":"Caucasian, Hispanic"}
        elif source_name=="Chicago":
            # https://home.chicagopolice.org/wp-content/uploads/2020/08/ISR-Data-Dictionary.csv
            map_dict = {
                'BLK' : 'BLACK',
                'WHI' : 'WHITE',
                'API' : 'ASIAN/PACIFIC ISLANDER',
                'WBH' : 'BLACK HISPANIC',
                'WWH' : 'WHITE HISPANIC',
                'I' : 'AMER IND/ALASKAN NATIVE',
                'U' : 'UNKNOWN',
                'P' : 'NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER',
                'WHT' : 'WHITE'
            }

        if x in map_dict:
            x = map_dict[x].upper()

    if _p_nonlatino.search(x) and len(m:=_p_nonlatino.sub('', x))>0:
        x = m

    if isinstance(x,str):
        # Look for code: {Race_Abbreviation_Initial} {- or =} {Full name}
        abbrev_full_match = re.search(r"^([\w\s/\.]+)\s?[-=]\s?([\w\s/\.]+)$",x)
        if abbrev_full_match and any([len(x)==1 for x in abbrev_full_match.groups()]):
            x = [x for x in abbrev_full_match.groups() if len(x)>1][0].strip()

        delims = [" and ", ",",'|','/',';']
        delim = [d for d in delims if d in x]
        # Look for delimited multi-race data
        if not known_single and len(delim)>0 and \
            not x.lower().startswith("some other") and \
            x.lower().replace(" ","") not in ["hawaiian/pacific", "middleeastern/southasian", 'n/a',
                                              'asian/pacis','unk/oth','oth/unk', 'black/africanamerican','hispanic/latino',
                                              "americanindian/alaskanative",'a/indian'] and \
            not any([x.lower() in ['unknown','other'] for x in x.split(delim[0])]):
            # Treat this as a list of races
            delim = delim[0]
            race_list = []
            for v in x.split(delim):
                if (v=="INDIAN" and "BURMESE" in x) or v=='N':  # N is non-Latino, which can be ignored
                    # Handle special case to prevent setting someone from country of India to Indigenous
                    continue
                new_val = _create_race_lut(v, no_id, source_name, race_cats, agg_cat, known_single=True)
                if isinstance(new_val, list):
                    race_list.extend(new_val)
                else:
                    race_list.append(new_val)

            if has_unspecified and agg_cat:
                num_unspecified = sum([x==race_cats[defs._race_keys.UNSPECIFIED] for x in race_list])
                if num_unspecified == len(race_list):
                    # All are unspecified. Just return unspecified rather than a list
                    return race_cats[defs._race_keys.UNSPECIFIED]
                else:
                    # Ignore the unspecifieds
                    race_list = [x for x in race_list if x!=race_cats[defs._race_keys.UNSPECIFIED]]

            if len(race_list)==1 or all([x==race_list[0] for x in race_list]): # All are the same
                return race_list[0]
            elif has_aapi and has_asian and has_pi and \
                set(race_list) == set([race_cats[defs._race_keys.ASIAN], race_cats[defs._race_keys.PACIFIC_ISLANDER]]):
                # Simplify to AAPI
                return race_cats[defs._race_keys.AAPI]
            elif has_aapi and has_asian and \
                set(race_list) == set([race_cats[defs._race_keys.ASIAN], race_cats[defs._race_keys.AAPI]]):
                # Simplify to AAPI
                return race_cats[defs._race_keys.AAPI]
            elif agg_cat and has_latino and race_cats[defs._race_keys.LATINO] in race_list:
                # Category is Latino of all races
                return race_cats[defs._race_keys.LATINO]
            else:
                return race_list

    # Clean x
    x = x.upper().replace("_", " ").replace("*","").replace("-","").replace(".","").strip()

    if pd.notnull(x):
        # Check if value is part of race_cats
        # This is particularly necessary for custom race_cats dicts
        for k,v in race_cats.items():
            # Key is always be a string
            if k.upper()==str(orig).upper() or k.upper()==str(x) or str(v)==str(orig).upper() or str(v)==str(x):
                return v
        
    def is_latino(x):
        if isinstance(x,str):
            x = x.upper().replace("-","").replace(" ","")
            return "HISPANIC" in x or "LATINO" in x
        else:
            return False

    x_no_space = x.replace(" ","")
    if has_unspecified and (x in ["MISSING","NOT SPECIFIED", "", "NOT RECORDED","N/A", "NOT REPORTED", "NONE"] or \
        (type(x)==str and ("NO DATA" in x or ("NOT APP" in x and x not in ["NOT APPLICABLE (NONUS)",'NOT APPLICABLE (NON US)']) or "NO RACE" in x or "NULL" in x))):
        return race_cats[defs._race_keys.UNSPECIFIED]
    if has_white and x_no_space in ["W", "CAUCASIAN", "WN", "WHITE", "WHTE", "WHITE,OTHER", 'WHT']:  # WN = White-Non-Hispanic
        return race_cats[defs._race_keys.WHITE]
    if has_black and (x in ["B", "AFRICAN AMERICAN", "BLCK", "BLK", "BLACE",'AFR AMERICAN'] or \
                      x_no_space in ["AFRICANAMERICAN"] or \
                      re.search("BLACK?($|[^A-Za-z])",x)) \
                      and not is_latino(x):
        if x.count("BLACK") > 1:
            raise ValueError(f"The value of {x} likely contains the races for multiple people")
        return race_cats[defs._race_keys.BLACK]
    if has_south_asian and ((x in ["SOUTH ASIAN"] or ("ASIAN" in x and "INDIAN" in x)) or \
                            x in ["EAST INDIAN"]):
        if defs._race_keys.SOUTH_ASIAN in race_cats:
             return race_cats[defs._race_keys.SOUTH_ASIAN]
        else:
            return race_cats[defs._race_keys.MIDDLE_EASTERN_SOUTH_ASIAN] 
    if has_me and (x in ["ME","ARABIC"] or "MIDDLE EAST" in x) and "AFRICA" not in x:  # Ignore Middle Eastern or North African
        if "SOUTH ASIAN" in x:
            if has_me_or_sa:
                return race_cats[defs._race_keys.MIDDLE_EASTERN_SOUTH_ASIAN]
        else:
            return race_cats[defs._race_keys.MIDDLE_EASTERN] if defs._race_keys.MIDDLE_EASTERN in race_cats else race_cats[defs._race_keys.MIDDLE_EASTERN_SOUTH_ASIAN]
    if (has_asian or has_aapi) and (x in ["A", 'ORIENTAL', 'AA', 'ASN'] or "ASIAN" in x.replace("CAUCASIAN","")) and x not in ["SOUTHWEST ASIAN"] and "INDIAN" not in x:
        if has_aapi and ("PAC" in x or "HAWAI" in x):
            return race_cats[defs._race_keys.AAPI] 
        else:
            return race_cats[defs._race_keys.ASIAN] if has_asian else race_cats[defs._race_keys.AAPI]
    if (has_pi or has_aapi) and ("HAWAI" in x or "PACIFIC" in x_no_space or \
                                 "PACISL" in x_no_space or x in ['PI', 'NHPI']): # NHPI = Native Hawaiian, and Pacific Islander
        return race_cats[defs._race_keys.PACIFIC_ISLANDER] if has_pi else race_cats[defs._race_keys.AAPI]
    if has_latino and x in ["H", "WH", "HISPANIC", "LATINO", "HISPANIC OR LATINO", "LATINO OR HISPANIC", 
                            "HISPANIC/LATINO", "LATINO/HISPANIC",'HISPANIC/LATIN/MEXICAN','HISP']:
        return race_cats[defs._race_keys.LATINO] 
    if has_indigenous and (x in ["I", "INDIAN", "ALASKAN NATIVE", "AN", "AI", "AL NATIVE","A/INDIAN", "NAT AM"] or "AMERICAN IND" in x.replace("II","I") or \
        "NATIVE AM" in x or  "AMERIND" in x_no_space or "ALASK" in x or "AMIND" in x_no_space or \
        "NAT AMER" in x):
        return race_cats[defs._race_keys.INDIGENOUS] 
    if has_multiple and ("OR MORE" in x or "MULTI" in x or \
        x_no_space in ["MIXED","BIRACIAL","MIXEDRACE","MORE THAN TWO RACES".replace(" ",""),
                              "MORE THAN 2 RACES".replace(" ","")]):
        return race_cats[defs._race_keys.MULTIPLE]
    if defs._race_keys.OTHER_UNKNOWN in race_cats and "UNK" in x and "OTH" in x:
        return race_cats[defs._race_keys.OTHER_UNKNOWN]
    if defs._race_keys.UNKNOWN in race_cats and  ("UNK" in x or x in ["U", "UK"]):
        return race_cats[defs._race_keys.UNKNOWN]
    if defs._race_keys.OTHER in race_cats and (x in ["O","OTHER","OTH"] or "OTHER UNCLASS" in x or "OTHER RACE" in x):
        return race_cats[defs._race_keys.OTHER]
    
    if agg_cat:
        # This attempts to categorize more cases based on U.S. Census definitions:
        # https://www.census.gov/programs-surveys/decennial-census/decade/2020/planning-management/release/faqs-race-ethnicity.html
        # Cases include here are based on data values that have been observed
        if has_latino and (("HISP" in x and "NONHISP" not in x_no_space) or \
                           ("LATIN" in x and "NONLATINO" not in x_no_space) or \
                            x in ["MEXICAN"]):
            # agg_cat forces Latino of all races
            return race_cats[defs._race_keys.LATINO] 
        elif has_black and x in ["EAST AFRICAN"]:
            return race_cats[defs._race_keys.BLACK]
        elif has_white and x in ["BOSNIAN"]:
            return race_cats[defs._race_keys.WHITE]
        elif has_me and x in ["SOUTHWEST ASIAN"]:
            return race_cats[defs._race_keys.MIDDLE_EASTERN] if defs._race_keys.MIDDLE_EASTERN in race_cats else race_cats[defs._race_keys.MIDDLE_EASTERN_SOUTH_ASIAN]
        elif has_south_asian and (("INDIAN" in x and "BURMESE" in x) or x in ["INDIA","BURMESE"]):
            if defs._race_keys.SOUTH_ASIAN in race_cats:
                return race_cats[defs._race_keys.SOUTH_ASIAN]
            else:
                return race_cats[defs._race_keys.MIDDLE_EASTERN_SOUTH_ASIAN]
        elif (has_asian or has_aapi) and (x in ["CAMBODIAN",'VIETNAMESE',"LAOTIAN","JAPANESE","KOREAN","CHINESE","HMONG","MIEN","FILIPINO"] or "ASIAN" in x):
            return race_cats[defs._race_keys.ASIAN] if has_asian else race_cats[defs._race_keys.AAPI]
        elif (has_pi or has_aapi) and x in ["POLYNESIAN","SAMOAN","GUAMANIAN"]:
            return race_cats[defs._race_keys.PACIFIC_ISLANDER] if has_pi else race_cats[defs._race_keys.AAPI]
        elif no_id=="error":
            raise ValueError(f"Unknown value in race column: {orig}")
        elif no_id=="test":
            if x in ["MALE","FEMALE","GIVING ANYTHING OF VALUE","REFUSED", "NA","M","F","OTHER/NOT REPORTED", 
                     'UNOCCUPIED VEHICLE','MIDDLE EASTERN OR NORTH AFRICAN','P','PAKISTANI','DUPLICATE',
                     'NOT AVAILABLE'] or \
                (source_name in ["Chapel Hill","Lansing","Fayetteville"] and x in ["S","P"]) or \
                (source_name=="Burlington" and x in ["EXPUNGED"]) or \
                (source_name=="Minneapolis" and x in ["NO CONTACT"]) or \
                (source_name in ["Cincinnati","San Diego"] and x in ["F","S","P"]) or \
                (source_name in ["Columbia"] and x in ["M","P",'34']) or \
                (source_name in ["Urbana"] and x in ["BUSINESS OR OTHER"]) or \
                (source_name in ["Bloomington","Beloit"] and x in ["M"]) or \
                (source_name in ["Beloit"] and x in ["L"]) or \
                (source_name in ["St. John"] and x in ["K","P"]) or \
                (source_name in ["Rutland"] and x in ["M","R"]) or \
                (source_name in ["Dallas"] and x in ["NA"]) or \
                (source_name in ["Sacramento"] and x in ["CUBAN","CARRIBEAN"]) or \
                (source_name in ["New York City"] and x in ["SOUTHWEST"]) or \
                (source_name in ["Wallkill"] and x in ["IND",'N','M']) or \
                (x=="UN" and source_name=="State Police") or \
                (x=="P" and source_name=="Pittsfield") or \
                (x=="PENDING RELEASE" and source_name=="Portland") or \
                (x=="IRAK" and source_name=="Chattanooga") or \
                (x=="N" and source_name=="Bremerton") or \
                (x=="W\nW" and source_name=="Sparks") or \
                (x in ['UI'] and source_name=="Norwich") or \
                (x in ['E','P'] and source_name=="Tucson") or \
                ("DOG" in x) or \
                (source_name in ["New Orleans"] and "NOT APPLICABLE (NON" in x) or \
                (source_name in ["Detroit", "Fairfax County"] and x in ["N","SELECT","UNVERIFIED",'240','160','180','P','120']) or \
                x in ["OTHER / MIXED RACE", "UNDISCLOSED", "OR SPANISH ORIGIN","PREFER NOT TO SAY","OTHERBLEND","UNDECLARED"] or \
                len(orig)>100:
                # This is meant to be temporary for testing
                return "BAD DATA"
            elif "EXEMPT" in x:
                return orig
            else:
                raise ValueError(f"Unknown value in race column: {orig}")
        else:
            return orig if no_id=="keep" else ""
    elif no_id=="error":
        raise ValueError(f"Unknown value in race column: {orig}")
    else:
        return orig if no_id=="keep" else ""


def _create_gender_lut(x, no_id, source_name, gender_cats, *args, **kwargs):

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
                return orig if no_id=="keep" else ""
        else:
            # Replace numerical code with default value
            x = map_dict[x]


    if isinstance(x,str):
        # Look for {Abbreviation_Initial} {- or =} {Full name}
        abbrev_full_match = re.search(r"^([\w\s/\.]+)\s?[-=]\s?([\w\s/\.]+)$",x)
        if abbrev_full_match and any([len(x)==1 for x in abbrev_full_match.groups()]):
            x = [x for x in abbrev_full_match.groups() if len(x)>1][0].strip()
        x = x.upper().replace("-","").replace("_","").replace(" ","").replace("'","")

        if source_name in ["New York City", "Los Angeles", "Chicago",'New York']:
            # Handling dataset-specific codes
            if source_name == "New York City":
                # https://www.nyc.gov/assets/nypd/downloads/zip/analysis_and_planning/stop-question-frisk/SQF-File-Documentation.zip
                map_dict = {"Z":"Unknown"}
            elif source_name == "New York":
                # https://data.ny.gov/Transportation/Traffic-Tickets-Issued-Four-Year-Window/q4hy-kbtf/about_data
                map_dict = {"C":"Organization"}
            elif source_name in ["Los Angeles",'Chicago']:
                # https://data.lacity.org/Public-Safety/Traffic-Collision-Data-from-2010-to-Present/d5tf-ez2w
                # https://home.chicagopolice.org/wp-content/uploads/2020/08/ISR-Data-Dictionary.csv
                map_dict = {"X":"Unknown"}

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
            # Same result whether no_id is keep or null
            return ""
    if has_unspecified and x in ["MISSING", "UNSPECIFIED", "",",",'NOTSPECIFIED',"NOTRECORDED","NONE"] or \
        "NODATA" in x or "NOSEX" in x or "NULL" in x:
        return gender_cats[defs._gender_keys.UNSPECIFIED]
    elif defs._gender_keys.FEMALE in gender_cats and x in ["F", "FEMALE", "FEMAALE", "FFEMALE", "FEMAL", "FEMALE/WOMAN","WOMAN"]:
        return gender_cats[defs._gender_keys.FEMALE]
    elif defs._gender_keys.MALE in gender_cats and x in ["M", "MALE", "MMALE", "MALE/MAN","MAN", "MLE"]:
        return gender_cats[defs._gender_keys.MALE]
    elif defs._gender_keys.OTHER in gender_cats and x in ["OTHER", "O"]:
        return gender_cats[defs._gender_keys.OTHER]
    elif defs._gender_keys.TRANSGENDER in gender_cats and x in ["TRANSGENDER","TRANSGENDERED"]:
        return gender_cats[defs._gender_keys.TRANSGENDER]
    elif defs._gender_keys.GENDER_NONBINARY in gender_cats and x in ["NONBINARY"]:
        return gender_cats[defs._gender_keys.GENDER_NONBINARY]
    elif defs._gender_keys.TRANSGENDER_OR_GENDER_NONCONFORMING in gender_cats and (x in [
        "Gender Diverse (gender non-conforming and/or transgender)".upper().replace("-","").replace(" ",""),
        "GENDERNONCONFORMING",'NONCONFORMING','GENDERDIVERSE'] or "TGNC" in x):
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
    elif defs._gender_keys.UNSPECIFIED_OR_ANOTHER in gender_cats and x in 'X':
        # https://www.state.gov/x-gender-marker-available-on-u-s-passports-starting-april-11/
        return gender_cats[defs._gender_keys.UNSPECIFIED_OR_ANOTHER]
    
    if no_id=="test":
        bad_data = ["BLACK","WHITE",'HISPANIC','ASIAN','FEMALE10','DUPLICATE']
        if "EXEMPT" in x or x in ["DATAPENDING", "NOTAPPLICABLE","N/A",'NA','NOTAVAILABLE'] or ("BUSINESS" in x and source_name=="Cincinnati"):
            return orig
        elif x in bad_data or \
            (source_name=="New York City" and (x=="Z" or x.isdigit())) or \
            (source_name=="New York" and x in ["Organization".upper()]) or \
            (source_name=="Baltimore" and x in ["Y","Z"]) or \
            (source_name=="Urbana" and x in ["."]) or \
            (source_name=="Greensboro" and x in ["ASIAN"]) or \
            (source_name=="Columbia" and x in ["B"]) or \
            (source_name=="Burlington" and x in ["EXPUNGED"]) or \
            (source_name=="Fairfax County" and x in ['O']) or \
            (source_name=='Albemarle County' and x in ['N']) or \
            (source_name in ["Seattle","New Orleans","Menlo Park","Rutland"] and x in ["D","N"]) or \
            (source_name=="Los Angeles County" and x=="0") or \
            (x in ["W","NA"]) or \
            (x in ['L'] and source_name=='Bloomington') or \
            (x in ['GROUP'] and source_name=='Asheville') or \
            (x in ['UI'] and source_name=="Norwich") or \
            (x=="MA" and source_name in ["Lincoln"]) or \
            (x=="P" and source_name=="Fayetteville") or \
            (x in ['B'] and source_name in ["Louisville",'Griffith']) or \
            (x=="5" and source_name=="Lincoln") or \
            (source_name=="Minneapolis" and x in ["NOCONTACT"]) or \
            (x in ["MALE,MALE"] and source_name=="Chattanooga") or \
            (x=="PENDINGRELEASE" and source_name=="Portland") or \
            (x in ["N","H"] and source_name=="Los Angeles") or \
            (source_name=='Bremerton' and x in ['B', 'W', 'A','I']) or \
            "DOG" in x or \
            x in ["UNDISCLOSE","UNDISCLOSED","PREFER TO SELF DESCRIBE".replace(" ",""),'NONBINARY/THIRDGENDER',
                  "PREFER NOT TO SAY".replace(" ",""),"TGNC/OTHER","REFUSED",'UNVERIFIED','NONBINARY/OTHERX','UNOCCUPIEDVEHICLE'] or \
            source_name == "Buffalo":
            return orig
        else:
            raise ValueError(f"Unknown value in gender column: {orig}")
    if no_id=="error":
        raise ValueError(f"Unknown value in gender column: {orig}")
    else:
        return orig if no_id=="keep" else ""
    

def _create_injury_lut(x, no_id, source_name, cats, *args, **kwargs):
    if pd.isnull(x):
        return "UNSPECIFIED"
    elif isinstance(x,Number) or pd.api.types.is_bool(x) or x.isdigit():  # is_bool handles numpy.bool_ type
        x = int(x)
        if x<0:
            return x
        else:
            return "INJURED" if x>0 else "NO INJURY"
    orig = x
    x = re.sub(r'^\d+\s*\-\s*','',x)
    x = x.upper().replace('-',' ').replace('*','').replace('SUBJECT','').replace('  ',' ').strip()
    x = re.sub(r'OF[FI]{2}CER','', x).strip()  # Handle misspelling officer
    x = re.sub(r'\s{2,10}',' ',x)

    if len(x)==0:
        return "UNSPECIFIED"
    elif len(words:=x.split('\n'))>1:
        words = [_create_injury_lut(y, no_id, source_name, cats) for y in words]
        for m in ['FATAL','INJURED']:
            if m in words:
                return m

    contains_yes = x.replace(',',' ').startswith('YES ')
    nonfatal_words = ["INJURED", 'NON FATAL', 'NON FATAL','NON FATAL INJURY', 'INJURY'] 
    contains_nonfatal = any([y in x for y in nonfatal_words])
    fatal_words = ["FATAL","KILLED",'DECEASED',"DEATH", 'FATAL INJURY']
    contains_fatal = any([y in x for y in fatal_words])
    is_fatal = contains_yes and contains_fatal and not contains_nonfatal

    if is_fatal or x in fatal_words:
        return "FATAL"
    elif x.startswith('NO INJUR') or x.startswith('NONE') or x.startswith('NO COMPLAINT') or\
        x in ["NOT INJURED",'NEITHER','NO','N', "MISS", 'SHOOT AND MISS','FALSE','NO VISIBLE INJURY','UNINJURED', 'SHOW OF FORCE']:
        return "NO INJURY"
    elif contains_yes or x in nonfatal_words or x in ['Y','YES','TRUE'] or \
        any([x.startswith(y) for y in ['COMPLAINED OF','COMPLAINT OF']]) or \
        any([y in x for y in ['WOUND','PAIN', "BLEEDING",'SWELLING','SCRAPE','PUNCTURE','LACERATION','BRUIS',
                              'BROKEN','UNCONSCIOUS', 'FIRST AID', 'SHOT (INJURED ONLY)', 'INJURED INCIDENTALLY',
                              'DISLOCATED','FRACTURED','ABBRASION','ABRASION','ABRAISON','ABRASSION','BUSTED','PULLED OUT','REDNESS','LOSS','RASH',
                              'SCRATCH','NUMBNESS','BREATHING','CUT','STUN', 'MARKS', 'EYE', 'PEELING', 'HURT', 'ELBOW', 'KNEE',
                              'SOFT TISSUE','BLOOD','HEAD','SORE','SHOULDER', 'MINOR INJUR', 'FINGER', 'IMPACT', 'FACE', 'ARM',
                              'MOUTH', 'BACK','RIB', 'THUMB','SHIN',' EAR', 'ACHILLES', 'STRUCK', 'LEG', 'SERIOUS',
                              'CONCUSSION','FRACTURE','CANINE BITE', 'MARK','BURN', 'MINOR', 'DISABL', 'PHYSICAL INJURY',
                              'TREATED','TAKEN TO HOSPITAL']]):
        return "INJURED"
    elif 'NALOXONE' in x:
        return orig
    elif x in ['SELF INFLICTED FATAL', 'DECEASED (SELF INFLICTED)','KILLED (SELF INFLICTED)', 'FATAL SUICIDE']:
        return 'SELF-INFLICTED FATAL'
    elif x in ['UNKNOWN','UNKNWON']:
        return 'UNKNOWN'
    elif x in ['UNSPECIFIED', "NOT AVAILABLE", 'NOT SPECIFIED','(MISSING)','NA']:
        return 'UNSPECIFIED'
    elif x=='OTHER':
        return 'OTHER'
    elif no_id=='test':
        if source_name not in ['Indianapolis', 'Northampton','Minneapolis','Rutland'] and \
            (x not in ['UI'] or source_name!="Norwich"):
            raise ValueError(f"Unknown value in injury column: {orig}")
    elif no_id=='error':
        raise ValueError(f"Unknown value in injury column: {orig}")
    else:
        return orig if no_id=="keep" else ""
    
def _create_fatal_lut(x, no_id, source_name, cats, *args, **kwargs):
    if pd.isnull(x):
        return "UNSPECIFIED"
    elif isinstance(x,Number) or x.isdigit():
        x = int(x)
        if x<0:
            return x
        elif x>1:
            if no_id=='error':
                raise ValueError(f"Unknown value in fatal column: {orig}")
            else:
                return orig if no_id=="keep" else ""
        else:
            return "YES" if x>0 else "NO"
    orig = x
    x = x.upper().strip()
    if x in ["FATAL","YES", "Y",'DECEASED']:
        return "YES"
    elif x in ["NON-FATAL","NON FATAL", "NO","N",'NO CONTACT']:
        return "NO"
    elif x in ["SELF-INFLICTED"] or (source_name=='Louisville' and x=='YS'):
        return "SELF-INFLICTED FATAL"
    elif (source_name=='Louisville' and x=='CONTINUED'):
        # Used in Louisville table to indicate where a single shooting applies to multiple officers in different rows
        return orig
    elif no_id in ['test', 'error']:
        raise ValueError(f"Unknown value in fatal column: {orig}")
    else:
        return orig if no_id=="keep" else ""
    

def _create_firearm_lut(x, no_id, source_name, cats, *args, **kwargs):
    if pd.isnull(x):
        return "UNSPECIFIED"
    elif isinstance(x,Number) or x.isdigit():
        x = int(x)
        if x<0:
            return x
        else:
            return "YES" if x>0 else "NO"
    orig = x
    x = x.upper().strip()
    if x in ["YES", "Y",'TRUE','FIREARM']:
        return "YES"
    elif x in ["NO","N",'FALSE','PHYSICAL FORCE','ELECTRICAL WEAPON','OC SPRAY','IMPACT WEAPON', 'NO FORCE'] or \
        any([y in x for y in ['CANINE','BLANKET','DISPLAYED','CUTTING']]):
        return "NO"
    elif no_id=='test':
        raise ValueError(f"Unknown value in injury column: {orig}")
    elif no_id=='error':
        raise ValueError(f"Unknown value in injury column: {orig}")
    else:
        return orig if no_id=="keep" else ""
    

def std_dict(col, std_map, converter, *args):
    # Standardize column containing dictionaries containing the races of multiple people
    new_vals = []
    for j in col.index:
        p_dict = col[j].copy()
        for k, v in p_dict.items():
            if v not in std_map:
                std_map[v] = converter(v, *args)
            p_dict[k] = std_map[v]
        new_vals.append(p_dict)

    # If all values are of length
    if all([len(x)==1 for x in new_vals]):
        # No need for dictionary
        new_vals = [x[0] for x in new_vals]

    return new_vals


def std_demo_col(col, vals, map_dict, item_num, converter, *args):
    # Standardize demographics column
    map = {}
    delims = ["(", ","]
    multi_found = False
    for x in vals:
        if isinstance(x,str):
            items = [x]
            for d in delims:
                if d in x:
                    items = x.split(d)
                    break
            
            new_val = {}
            if len(items[0])==0:
                istart = 1
            else:
                istart = 0
            for k, i in enumerate(items[istart:]):
                if "," in i:
                    i = i.split(",")[item_num].strip()
                elif "/" in i:
                    i = i.split("/")[item_num].strip()
                elif i != "Unk":
                    raise NotImplementedError()

                if i not in map_dict:
                    map_dict[i] = converter(i, *args)

                if len(items[istart:])==1:
                    new_val = map_dict[i]
                else:
                    new_val[k] = map_dict[i]
                    multi_found = True

            map[x] = new_val
        else:
            if x not in map_dict:
                map_dict[x] = converter(x, *args)
                map[x] = map_dict[x]

    if multi_found:
        # Convert non-dicts to dict
        map = {k:(v if (isinstance(v,dict) or pd.isnull(v)) else {0:v}) for k,v in map.items()}

    return col.map(map)


def std_counts(col, vals, map_dict, delim, converter, *args, **kwargs):
    map = {}
    race_count_re = re.compile(r"(\d+)\s?-\s?([A-Za-z]+\.?\s?[A-Za-z]*\.?)")
    for x in vals:
        matches = race_count_re.findall(x) if isinstance(x,str) else False
        if matches:
            total = sum([int(m[0]) for m in matches])
            k = 0
            new_val = {}
            for m in matches:
                if m[1] not in map_dict:
                    map_dict[m[1]] = converter(m[1], *args, **kwargs)

                if total==1:
                    new_val = map_dict[m[1]]
                else:
                    for _ in range(int(m[0])):
                        new_val[k] = map_dict[m[1]]
                        k+=1
                    multi_found = True

            map[x] = new_val
        else:
            if x not in map_dict:
                map_dict[x] = converter(x, *args, **kwargs)
                map[x] = map_dict[x]

    if multi_found:
        # Convert non-dicts to dict
        map = {k:(v if (isinstance(v,dict) or pd.isnull(v)) else {0:v}) for k,v in map.items()}

    return col.map(map)


def std_with_names(col, vals, map_dict, item_num, converter, *args, **kwargs):
    map = {}
    multi_found = False
    p = re.compile(r"[\sÊ](\w{1,2}/\w)")
    for x in vals:
        if isinstance(x,str):
            items = p.findall(x)
            
            new_val = {}
            for k, i in enumerate(items):
                i = i.split("/")
                i = i[item_num]

                if i not in map_dict:
                    map_dict[i] = converter(i, *args)

                if len(items)==1:
                    new_val = map_dict[i]
                else:
                    new_val[k] = map_dict[i]
                    multi_found = True

            map[x] = new_val
        else:
            if x not in map_dict:
                map_dict[x] = converter(x, *args)
                map[x] = map_dict[x]

    if multi_found:
        # Convert non-dicts to dict
        map = {k:(v if (isinstance(v,dict) or pd.isnull(v)) else {0:v}) for k,v in map.items()}

    return col.map(map)


def std_list(col, vals, map_dict, delim, converter, *args, **kwargs):
    map = {}
    multi_found = False
    # Look for multiplication pattern such as F x 2 for 2 females
    re_mult = re.compile(r"([A-Za-z])\s?[Xx]\s?(\d+)")
    re_mult_reverse = re.compile(r"(\d+)\s?[Xx]\s?([A-Za-z])")
    for x in vals:
        if isinstance(x,str):
            xorig = x
            for m in re_mult.finditer(x):
                multi_found = True
                new_str = delim.join([m.group(1) for _ in range(int(m.group(2)))])
                x = x.replace(m.group(0), new_str)
            for m in re_mult_reverse.finditer(x):
                multi_found = True
                new_str = delim.join([m.group(2) for _ in range(int(m.group(1)))])
                x = x.replace(m.group(0), new_str)

            items = x.split(delim)
            new_val = {}
            for k, i in enumerate(items):
                if i == "ISL":  # LA County code for AAPI is ASIAN-PACIFIC,ISL
                    continue
                i = i.strip()
                if i not in map_dict:
                    map_dict[i] = converter(i, *args, **kwargs)
                if len(items)==1:
                    new_val = map_dict[i]
                else:
                    new_val[k] = map_dict[i]
                    multi_found = True

            map[xorig] = new_val
        else:
            if x not in map_dict:
                map_dict[x] = converter(x, *args, **kwargs)
                map[x] = map_dict[x]

    if multi_found:
        # Convert non-dicts to dict
        map = {k:(v if (isinstance(v,dict) or pd.isnull(v)) else {0:v}) for k,v in map.items()}

    return col.map(map)


def std_with_counts(col, vals, map_dict, delim, converter, no_id, *args, **kwargs):
    map = {}
    multi_found = False
    no_id_error = 'error'
    for x in vals:
        if isinstance(x,str):
            x = x.strip()
            try:
                map_dict[x] = converter(x, no_id_error, *args, **kwargs)
                map[x] = map_dict[x]
            except:
                # This needs parsed. Strings can be injuries to multiple delimited by spaces (i.e. 'Deceased Injured')
                # and/or counts of an injury (i.e. '2 Injured')
                items = x.split(delim)
                subject_count = 0
                count = 1
                cur_item = ''
                new_val = {}
                for i in items:
                    if i.isdigit():
                        count = int(i)
                    else:
                        cur_item = cur_item + " " + i.strip() if len(cur_item)>0 else i.strip()
                        try:
                            if cur_item not in map_dict:
                                map_dict[cur_item] = converter(cur_item, no_id_error, *args, **kwargs)
                        except:
                            continue
                        
                        for j in range(0, count):
                            new_val[subject_count+j] = map_dict[cur_item]
                        subject_count+=count
                        count = 1
                        cur_item = ''
                        multi_found = True

                if len(cur_item)>0:
                    if no_id in ['error','test']:
                        raise ValueError("Unable to parse subject injury")
                    elif no_id=='null':
                        new_val = ""
                    else:
                        new_val = x
                map[x] = new_val
        else:
            if x not in map_dict:
                map_dict[x] = converter(x, *args, **kwargs)
                map[x] = map_dict[x]

    if multi_found:
        # Convert non-dicts to dict
        map = {k:(v if (isinstance(v,dict) or pd.isnull(v)) else {0:v}) for k,v in map.items()}

    return col.map(map)
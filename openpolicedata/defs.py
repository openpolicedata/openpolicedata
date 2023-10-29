# Definition of constants
from __future__ import annotations
from copy import deepcopy
from enum import Enum
import inspect
import pandas as pd
import re
from sys import version_info
import warnings

# These are the types of data currently available in opd.
# They all have corresponding data loaders in data_loaders.py
# When new data loaders are added, this list should be updated.
class DataType(str, Enum):
    EXCEL = "Excel"
    CSV = "CSV"
    ArcGIS = "ArcGIS"
    SOCRATA = "Socrata"
    CARTO = "Carto"

# These are the types of tables currently available in opd.
# Add to this list when datasets do not correspond to the below data types
class TableType(str, Enum):
    # Adding a description property in addition to value
    # https://rednafi.github.io/reflections/add-additional-attributes-to-enum-members-in-python.html
    description = None

    def __new__(
        cls, value: str, description: str = ""
    ) -> TableType:

        obj = str.__new__(cls, value)
        obj._value_ = value
        obj.description = description
        return obj
    
    def __str__(self):
        return self.value
    
    
    @classmethod
    def _missing_(cls, value):
        # https://stackoverflow.com/questions/37444002/overriding-enum-call-method
        # Handles deprecation of CIVILIANS usage to SUBJECTS
        if "- CIVILIANS" in value:
            new_value = value.replace("- CIVILIANS", "- SUBJECTS")
            if new_value in TableType._value2member_map_:
                warnings.warn(
                    f"TableType {value} is deprecated. CIVILIAN has been replaced with SUBJECT in TableType names. Requested TableType has been automatically updated to {new_value}.",
                    DeprecationWarning,
                )
                return TableType(new_value)
        
        return super()._missing_(value)

    # Below tuples are (value, description)
    ARRESTS = ("ARRESTS", "Seizures or forcible restraints by police")
    CALLS_FOR_SERVICE = ("CALLS FOR SERVICE", "Includes dispatched calls (911 or non-emergency #) and officer-initiated calls")
    CITATIONS = ("CITATIONS","Commonly referred to as tickets, citations indicate a violation of the law and may be given for "+
        "violations such as traffic and civil violations")
    COMPLAINTS = ("COMPLAINTS","Complaints of police misconduct made internally or by the community")
    COMPLAINTS_ALLEGATIONS = ("COMPLAINTS - ALLEGATIONS",
        "Complaint data may be split into several tables. This table contains specific data on the allegations.")
    COMPLAINTS_BACKGROUND = ("COMPLAINTS - BACKGROUND",
        "Complaint data may be split into several tables. This table contains data on the general background of the complaints.")
    COMPLAINTS_SUBJECTS = ("COMPLAINTS - SUBJECTS",
        "Complaint data may be split into several tables. This table contains specific data on the involved subjects.")
    COMPLAINTS_OFFICERS = ("COMPLAINTS - OFFICERS",
        "Complaint data may be split into several tables. This table contains specific data on the involved officers.")
    COMPLAINTS_PENALTIES = ("COMPLAINTS - PENALTIES",
        "Complaint data may be split into several tables. This table contains specific data on any resulting penalties.")
    CRASHES = ("CRASHES", "Traffic crashes")
    CRASHES_SUBJECTS = ("CRASHES - SUBJECTS",
        "Crash data may be split into several tables due to the possibility that multiple "+
        "subjects (drivers and/or passengers) and vehicles may be involved in an incident. This table contains data on subjects.")
    CRASHES_INCIDENTS = ("CRASHES - INCIDENTS",
        "Crash data may be split into several tables due to the possibility that multiple "+
        "subjects and vehicles may be involved in an incident. This table contains data on the incident.")
    CRASH_NONMOTORIST = ("CRASHES - NONMOTORIST",
        "Crash data may be split into several tables due to the possibility that multiple "+
        "subjects and vehicles may be involved in an incident. This table contains information on non-motorists involved in a crash.")
    CRASHES_VEHICLES = ("CRASHES - VEHICLES",
        "Crash data may be split into several tables due to the possibility that multiple "+
        "subjects and vehicles may be involved in an incident. This table contains data on vehicles.")
    DISCIPLINARY_RECORDS = ("DISCIPLINARY RECORDS",
        "Disciplinary records of officers")
    EMPLOYEE = ("EMPLOYEE","Demographic data of the police workforce")
    DEATHS_IN_CUSTODY = ("DEATHS IN CUSTODY", "Deaths that occur in custody or during the process of arrest")
    FIELD_CONTACTS = ("FIELD CONTACTS", "Consensual contacts between officers and the community.")
    INCIDENTS = ("INCIDENTS", "Crime incident reports")
    LAWSUITS = ("LAWSUITS", "Lawsuits against a police department")
    PEDESTRIAN = ("PEDESTRIAN STOPS","Stops of pedestrians based on 'reasonable suspicion'. May lead to a frisk.")
    PEDESTRIAN_ARRESTS = ("PEDESTRIAN ARRESTS","Pedestrian stops leading to an arrest")
    PEDESTRIAN_CITATIONS = ("PEDESTRIAN CITATIONS","Pedestrian stops leading to a citation")
    PEDESTRIAN_WARNINGS = ("PEDESTRIAN WARNINGS","Pedestrian stops leading to a warning")
    SHOOTINGS = ("OFFICER-INVOLVED SHOOTINGS","Shootings by officers")
    SHOOTINGS_SUBJECTS = ("OFFICER-INVOLVED SHOOTINGS - SUBJECTS",
        "Officer-involved shootings data may be split into several tables due to the possibility that multiple "+
        "subjects and officers may be involved in an incident. This table contains data on subjects.")
    SHOOTINGS_OFFICERS = ("OFFICER-INVOLVED SHOOTINGS - OFFICERS",
        "Officer-involved shootings data may be split into several tables due to the possibility that multiple "+
        "subjects and officers may be involved in an incident. This table contains data on officer.")
    SHOOTINGS_INCIDENTS = ("OFFICER-INVOLVED SHOOTINGS - INCIDENTS",
        "Officer-involved shootings data may be split into several tables due to the possibility that multiple "+
        "subjects and officers may be involved in an incident. This table contains data on the incident.")
    STOPS = ("STOPS","Contains data on both pedestrian and traffic stops.")
    TRAFFIC = ("TRAFFIC STOPS","Traffic stops are stops by police of motor vehicles due to reasonable suspicion " + 
        " or traffic violations.")
    TRAFFIC_ARRESTS = ("TRAFFIC ARRESTS","Traffic stops leading to an arrest.")
    TRAFFIC_CITATIONS = ("TRAFFIC CITATIONS","Traffic stops leading to a citation.")
    TRAFFIC_WARNINGS = ("TRAFFIC WARNINGS","Traffic stops leading to a warning.")
    USE_OF_FORCE = ("USE OF FORCE","Documentation of physical force used against subjects.")
    USE_OF_FORCE_SUBJECTS = ("USE OF FORCE - SUBJECTS",
        "Use of force data may be split into several tables due to the possibility that multiple "+
        "subjects and officers may be involved in an incident. This table contains data on subjects.")
    USE_OF_FORCE_OFFICERS = ("USE OF FORCE - OFFICERS",
        "Use of force data may be split into several tables due to the possibility that multiple "+
        "subjects and officers may be involved in an incident. This table contains data on officer.")
    USE_OF_FORCE_INCIDENTS = ("USE OF FORCE - INCIDENTS",
        "Use of force data may be split into several tables due to the possibility that multiple "+
        "subjects and officers may be involved in an incident. This table contains data on the incident.")
    USE_OF_FORCE_SUBJECTS_OFFICERS = ("USE OF FORCE - SUBJECTS/OFFICERS",
        "Use of force data may be split into several tables due to the possibility that multiple "+
        "subjects and officers may be involved in an incident. This table contains data on subjects and officers.")
    VEHICLE_PURSUITS = ("VEHICLE PURSUITS","Attempts by officers in vehicles to pursue vehicles where the operator " + 
        "is believed to be aware that they are being signaled to stop but who is fleeing or ignoring the officer's attempt "+
        "to stop them.")

# Constants used in dataset parameters
MULTI = "MULTIPLE"    # For data sets that put multiple years or agencies in 1 dataset
NA = "NONE"         # None = not applicable (pandas converts "N/A" to NaN)

states = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "District of Columbia": "DC",
    "American Samoa": "AS",
    "Guam": "GU",
    "Northern Mariana Islands": "MP",
    "Puerto Rico": "PR",
    "United States Minor Outlying Islands": "UM",
    "U.S. Virgin Islands": "VI",
}

class _ToDict_Mixin:
    def to_dict(self):
        attributes = inspect.getmembers(self, lambda a:not(inspect.isroutine(a)))
        return {a:v for a,v in attributes if not(a.startswith('__'))}

# Standard column names
class _Columns(_ToDict_Mixin):
    DATE = "DATE"
    TIME = "TIME"
    DATETIME = "DATETIME"
    RACE_SUBJECT = "SUBJECT_RACE"
    RACE_ONLY_SUBJECT = "SUBJECT_RACE_ONLY"
    ETHNICITY_SUBJECT = "SUBJECT_ETHNICITY"
    RACE_OFFICER = "OFFICER_RACE"
    RACE_ONLY_OFFICER = "OFFICER_RACE_ONLY"
    ETHNICITY_OFFICER = "OFFICER_ETHNICITY"
    ETHNICITY_OFFICER_SUBJECT = "OFFICER/SUBJECT_ETHNICITY"
    RACE_OFFICER_SUBJECT = "OFFICER/SUBJECT_RACE"
    RACE_ONLY_OFFICER_SUBJECT = "OFFICER/SUBJECT_RACE_ONLY"
    AGENCY = "AGENCY"
    AGE_SUBJECT = "SUBJECT_AGE"
    AGE_OFFICER = "OFFICER_AGE"
    AGE_OFFICER_SUBJECT = "OFFICER/SUBJECT_AGE"
    AGE_RANGE_SUBJECT = "SUBJECT_AGE_RANGE"
    AGE_RANGE_OFFICER = "OFFICER_AGE_RANGE"
    AGE_RANGE_OFFICER_SUBJECT = "OFFICER/SUBJECT_AGE_RANGE"
    GENDER_SUBJECT = "SUBJECT_GENDER"
    GENDER_OFFICER = "OFFICER_GENDER"
    GENDER_OFFICER_SUBJECT = "OFFICER/SUBJECT_GENDER"
    SUBJECT_OR_OFFICER = "SUBJECT_OR_OFFICER"

    def _get_columns_as_df(self):
        attributes = self.to_dict()
        props = []
        columns = []
        defs = []
        sort_by = []
        for a, v in attributes.items():
            props.append(a)
            columns.append(v)

            match = re.match(r"^(SUBJECT|OFFICER|OFFICER/SUBJECT)\s(.+)$", v.replace("_"," "))
            if v=='SUBJECT_OR_OFFICER':
                sort_by.append(v)
                defs.append("Whether row describes an officer or an subject/civilian")
            elif match:
                if match.group(2)=="RACE ONLY":
                    addon = f". This is the standardized race column when both race and ethnicity are detected. "+\
                            f"If present, {v.replace('_ONLY','')} is a combination of race and ethnicity."
                else:
                    addon = ''
                if match.group(1)=="OFFICER/SUBJECT":
                    defs.append(f'{match.group(2).title()} of either an officer or subject (depending on column "{self.SUBJECT_OR_OFFICER}")'+addon)
                else:
                    defs.append(f"{match.group(2).title()} of {match.group(1).lower()}"+addon)
                sort_by.append(match.group(2).title())
            else:
                if a=="DATETIME":
                    defs.append("Combination of date and time when both columns are found (not generated when detected date column contains datetime values)")
                elif a=="DATE":
                    defs.append("Date. Some agencies only provide the period. In these cases, the date will be the 1st date of the period (i.e. Jan. 1 for years and the 1st of the month for months).")
                else:
                    defs.append(v.title())
                sort_by.append(v.title())

        df = pd.DataFrame({"Attribute":props, "Column Name":columns, "Definition":defs, 'sort_by':sort_by})
        return df.sort_values(by='sort_by').drop(columns='sort_by').reset_index(drop=True)

    def __repr__(self, ) -> str:
        df = self._get_columns_as_df()
        repr_params = pd.io.formats.format.get_dataframe_repr_params()
        return df.to_string(**repr_params)
    
    def _repr_html_(self):
        df = self._get_columns_as_df()
        return df.to_html()
    

columns = _Columns()

class _Races(_ToDict_Mixin):
    AAPI = "AAPI"
    ASIAN = "ASIAN"
    BLACK = "BLACK"
    INDIGENOUS = "INDIGENOUS"
    LATINO = "LATINO"
    MIDDLE_EASTERN = "MIDDLE_EASTERN"
    MIDDLE_EASTERN_SOUTH_ASIAN = "MIDDLE_EASTERN_SOUTH_ASIAN"
    MULTIPLE = "MULTIPLE"
    OTHER = "OTHER"
    OTHER_UNKNOWN = "OTHER / UNKNOWN"
    PACIFIC_ISLANDER = "PACIFIC_ISLANDER"
    SOUTH_ASIAN = "SOUTH_ASIAN"
    UNKNOWN = "UNKNOWN"
    UNSPECIFIED = "UNSPECIFIED"
    WHITE = "WHITE"

_race_keys = _Races()

class _Ethnicities(_ToDict_Mixin):
    LATINO = _race_keys.LATINO
    MIDDLE_EASTERN = _race_keys.MIDDLE_EASTERN
    NONLATINO = "NON-LATINO"
    UNKNOWN = _race_keys.UNKNOWN
    UNSPECIFIED = _race_keys.UNSPECIFIED

_eth_keys = _Ethnicities()


_eth_cats_basic = {
    _eth_keys.LATINO:"HISPANIC/LATINO",
    _eth_keys.MIDDLE_EASTERN:"MIDDLE EASTERN",
    _eth_keys.NONLATINO:"NON-HISPANIC/NON-LATINO",
    _eth_keys.UNKNOWN:"UNKNOWN",
    _eth_keys.UNSPECIFIED:"UNSPECIFIED",
}

_race_cats_basic = {
    _race_keys.AAPI:"ASIAN / PACIFIC ISLANDER",
    _race_keys.ASIAN:"ASIAN",
    _race_keys.BLACK:"BLACK",
    _race_keys.LATINO:"HISPANIC / LATINO",
    _race_keys.MULTIPLE:"MULTIPLE",
    _race_keys.INDIGENOUS:"INDIGENOUS",
    _race_keys.OTHER:"OTHER",
    _race_keys.OTHER_UNKNOWN:"OTHER OR UNKNOWN",
    _race_keys.UNKNOWN:"UNKNOWN",
    _race_keys.UNSPECIFIED:"UNSPECIFIED",
    _race_keys.WHITE:"WHITE"
}

_more_race_cats = {
    _race_keys.PACIFIC_ISLANDER : "HAWAIIAN / PACIFIC ISLANDER",
    _race_keys.MIDDLE_EASTERN:"MIDDLE EASTERN",
    _race_keys.MIDDLE_EASTERN_SOUTH_ASIAN:"MIDDLE EASTERN / SOUTH ASIAN",
    _race_keys.SOUTH_ASIAN:"SOUTH ASIAN",
}

# Combine to form _agg_race_cats
if version_info.minor >= 9:
    _race_cats_expanded = _race_cats_basic | _more_race_cats
else:
    _race_cats_expanded = {**_race_cats_basic, **_more_race_cats}

class _Genders(_ToDict_Mixin):
    MALE = "MALE"
    FEMALE = "FEMALE"
    TRANSGENDER_MALE = "TRANSGENDER_MALE"
    TRANSGENDER_FEMALE = "TRANSGENDER_FEMALE"
    TRANSGENDER = "TRANSGENDER"
    GENDER_NONCONFORMING = "GENDER_NONCONFORMING"
    TRANSGENDER_OR_GENDER_NONCONFORMING = "TRANSGENDER_OR_GENDER_NONCONFORMING"
    GENDER_NONBINARY = "GENDER_NONBINARY"
    OTHER = "OTHER"
    UNKNOWN = "UNKNOWN"
    UNSPECIFIED = "UNSPECIFIED"

_gender_keys = _Genders()

_genders = {
    _gender_keys.MALE:"MALE",
    _gender_keys.FEMALE:"FEMALE",
    _gender_keys.TRANSGENDER_MALE:"TRANSGENDER MALE",
    _gender_keys.TRANSGENDER_FEMALE:"TRANSGENDER FEMALE",
    _gender_keys.TRANSGENDER:"TRANSGENDER",
    _gender_keys.GENDER_NONCONFORMING:"GENDER NON-CONFORMING",
    _gender_keys.TRANSGENDER_OR_GENDER_NONCONFORMING:"TRANSGENDER OR GENDER NON-CONFORMING",
    _gender_keys.GENDER_NONBINARY:"GENDER NON-BINARY",
    _gender_keys.OTHER:"OTHER",
    _gender_keys.UNKNOWN:"UNKNOWN",
    _gender_keys.UNSPECIFIED:"UNSPECIFIED"
}

class _PersonTypes(_ToDict_Mixin):
    OFFICER = "OFFICER"
    SUBJECT = "SUBJECT"
    UNSPECIFIED = "UNSPECIFIED"

_roles = _PersonTypes()

def get_roles():
    return deepcopy(_roles)

def get_race_keys():
    return deepcopy(_race_keys)

def get_eth_keys():
    return deepcopy(_eth_keys)

def get_race_cats(expand=False):
    return _race_cats_expanded.copy() if expand else _race_cats_basic.copy()

def get_eth_cats():
    return _eth_cats_basic.copy()

def get_gender_keys():
    return deepcopy(_gender_keys)

def get_gender_cats():
    return _genders.copy()

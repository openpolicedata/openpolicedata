# Definition of constants
from __future__ import annotations
from enum import Enum
from sys import version_info

# These are the types of data currently available in opd.
# They all have corresponding data loaders in data_loaders.py
# When new data loaders are added, this list should be updated.
class DataType(Enum):
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
    COMPLAINTS_CIVILIANS = ("COMPLAINTS - CIVILIANS",
        "Complaint data may be split into several tables. This table contains specific data on the involved civilians.")
    COMPLAINTS_OFFICERS = ("COMPLAINTS - OFFICERS",
        "Complaint data may be split into several tables. This table contains specific data on the involved officers.")
    COMPLAINTS_PENALTIES = ("COMPLAINTS - PENALTIES",
        "Complaint data may be split into several tables. This table contains specific data on any resulting penalties.")
    CRASHES = ("CRASHES", "Traffic crashes")
    CRASHES_CIVILIANS = ("CRASHES - CIVILIANS",
        "Crash data may be split into several tables due to the possibility that multiple "+
        "civilians and vehicles may be involved in an incident. This table contains data on civilians.")
    CRASHES_INCIDENTS = ("CRASHES - INCIDENTS",
        "Crash data may be split into several tables due to the possibility that multiple "+
        "civilians and vehicles may be involved in an incident. This table contains data on the incident.")
    CRASHES_VEHICLES = ("CRASHES - VEHICLES",
        "Crash data may be split into several tables due to the possibility that multiple "+
        "civilians and vehicles may be involved in an incident. This table contains data on vehicles    .")
    EMPLOYEE = ("EMPLOYEE","Demographic data of the police workforce")
    FIELD_CONTACTS = ("FIELD CONTACTS", "Consensual contacts between officers and the community.")
    INCIDENTS = ("INCIDENTS", "Crime incident reports")
    PEDESTRIAN = ("PEDESTRIAN STOPS","Stops of pedestrians based on 'reasonable suspicion'. May lead to a frisk.")
    PEDESTRIAN_ARRESTS = ("PEDESTRIAN ARRESTS","Pedestrian stops leading to an arrest")
    PEDESTRIAN_CITATIONS = ("PEDESTRIAN CITATIONS","Pedestrian stops leading to a citation")
    PEDESTRIAN_WARNINGS = ("PEDESTRIAN WARNINGS","Pedestrian stops leading to a warning")
    SHOOTINGS = ("OFFICER-INVOLVED SHOOTINGS","Shootings by officers")
    SHOOTINGS_CIVILIANS = ("OFFICER-INVOLVED SHOOTINGS - CIVILIANS",
        "Officer-involved shootings data may be split into several tables due to the possibility that multiple "+
        "civilians and officers may be involved in an incident. This table contains data on civilians.")
    SHOOTINGS_OFFICERS = ("OFFICER-INVOLVED SHOOTINGS - OFFICERS",
        "Officer-involved shootings data may be split into several tables due to the possibility that multiple "+
        "civilians and officers may be involved in an incident. This table contains data on officer.")
    SHOOTINGS_INCIDENTS = ("OFFICER-INVOLVED SHOOTINGS - INCIDENTS",
        "Officer-involved shootings data may be split into several tables due to the possibility that multiple "+
        "civilians and officers may be involved in an incident. This table contains data on the incident.")
    STOPS = ("STOPS","Contains data on both pedestrian and traffic stops.")
    TRAFFIC = ("TRAFFIC STOPS","Traffic stops are stops by police of motor vehicles due to reasonable suspicion " + 
        " or traffic violations.")
    TRAFFIC_ARRESTS = ("TRAFFIC ARRESTS","Traffic stops leading to an arrest.")
    TRAFFIC_CITATIONS = ("TRAFFIC CITATIONS","Traffic stops leading to a citation.")
    TRAFFIC_WARNINGS = ("TRAFFIC WARNINGS","Traffic stops leading to a warning.")
    USE_OF_FORCE = ("USE OF FORCE","Documentation of physical force used against civilians.")
    USE_OF_FORCE_CIVILIANS = ("USE OF FORCE - CIVILIANS",
        "Use of force data may be split into several tables due to the possibility that multiple "+
        "civilians and officers may be involved in an incident. This table contains data on civilians.")
    USE_OF_FORCE_OFFICERS = ("USE OF FORCE - OFFICERS",
        "Use of force data may be split into several tables due to the possibility that multiple "+
        "civilians and officers may be involved in an incident. This table contains data on officer.")
    USE_OF_FORCE_INCIDENTS = ("USE OF FORCE - INCIDENTS",
        "Use of force data may be split into several tables due to the possibility that multiple "+
        "civilians and officers may be involved in an incident. This table contains data on the incident.")
    USE_OF_FORCE_CIVILIANS_OFFICERS = ("USE OF FORCE - CIVILIANS/OFFICERS",
        "Use of force data may be split into several tables due to the possibility that multiple "+
        "civilians and officers may be involved in an incident. This table contains data on civilians and officers.")
    VEHICLE_PURSUITS = ("VEHICLE PURSUITS","Attempts by officers in vehicles to pursue vehicles where the operator " + 
        "is believed to be aware that they are being signaled to stop but who is fleeing or ignoring the officer's attempt "+
        "to stop them.")

# Constants used in dataset parameters
MULTI = "MULTI"    # For data sets that put multiple years or agencies in 1 dataset
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

# Standard column names
class _Columns:
    DATE = "DATE"
    TIME = "TIME"
    DATETIME = "DATETIME"
    RACE_CIVILIAN = "RACE_CIVILIAN"
    RACE_ONLY_CIVILIAN = "RACE_ONLY_CIVILIAN"
    ETHNICITY_CIVILIAN = "ETHNICITY_CIVILIAN"
    RACE_OFFICER = "RACE_OFFICER"
    RACE_ONLY_OFFICER = "RACE_ONLY_OFFICER"
    ETHNICITY_OFFICER = "ETHNICITY_OFFICER"
    ETHNICITY_OFF_AND_CIV = "ETHNICITY_OFF_AND_CIV"
    RACE_OFF_AND_CIV = "RACE_OFF_AND_CIV"
    RACE_ONLY_OFF_AND_CIV = "RACE_ONLY_OFF_AND_CIV"
    AGENCY = "AGENCY"
    AGE_CIVILIAN = "AGE_CIVILIAN"
    AGE_OFFICER = "AGE_OFFICER"
    AGE_OFF_AND_CIV = "AGE_OFF_AND_CIV"
    AGE_RANGE_CIVILIAN = "AGE_RANGE_CIVILIAN"
    AGE_RANGE_OFFICER = "AGE_RANGE_OFFICER"
    AGE_RANGE_OFF_AND_CIV = "AGE_RANGE_OFF_AND_CIV"
    GENDER_CIVILIAN = "GENDER_CIVILIAN"
    GENDER_OFFICER = "GENDER_OFFICER"
    GENDER_OFF_AND_CIV = "GENDER_OFF_AND_CIV"
    CIVILIAN_OR_OFFICER = "CIVILIAN_OR_OFFICER"
    
columns = _Columns()

class _Races:
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

class _Ethnicities:
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

class _Genders:
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

class _PersonTypes:
    OFFICER = "OFFICER"
    CIVILIAN = "CIVILIAN"
    UNSPECIFIED = "UNSPECIFIED"

_roles = _PersonTypes()

def get_roles():
    return _roles

def get_race_keys():
    return _race_keys

def get_eth_keys():
    return _eth_keys

def get_race_cats(expand=False):
    return _race_cats_expanded if expand else _race_cats_basic

def get_eth_cats():
    return _eth_cats_basic

def get_gender_keys():
    return _gender_keys

def get_gender_cats():
    return _genders
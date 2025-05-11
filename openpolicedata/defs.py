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
# They all have corresponding data loaders.
# When new data loaders are added, this list should be updated.
class DataType(str, Enum):
    ArcGIS = "ArcGIS"
    CARTO = "Carto"
    CKAN = 'CKAN'
    CSV = "CSV"
    EXCEL = "Excel"
    HTML = 'HTML'
    SOCRATA = "Socrata"
    

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
    COMPLAINTS_BODY_WORN_CAMERA = ("COMPLAINTS - BODY WORN CAMERA",
        "Complaint data may be split into several tables. This table contains data on body worn camera details.")
    COMPLAINTS_SUBJECTS = ("COMPLAINTS - SUBJECTS",
        "Complaint data may be split into several tables. This table contains specific data on the involved subjects.")
    COMPLAINTS_SUBJECTS_OFFICERS = ("COMPLAINTS - SUBJECTS/OFFICERS",
        "Complaint data may be split into several tables. This table contains data on the involved subjects and officers. "+
        "A row in the data will indicate whether that row corresponds to an officer or subject.")
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
    CRASHES_NONMOTORIST = ("CRASHES - NONMOTORIST",
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
    INCIDENTS_INCIDENTS = ("INCIDENTS - INCIDENTS", 
                          "Incidents data may be split into several tables due to the possibility that multiple "+
                        "subjects may be involved in an incident. This table contains data on the details of the incident.")
    INCIDENTS_SUBJECTS = ("INCIDENTS - SUBJECTS", 
                          "Incidents data may be split into several tables due to the possibility that multiple "+
                        "subjects may be involved in an incident. This table contains data on subjects.")
    LAWSUITS = ("LAWSUITS", "Lawsuits against a police department")
    PEDESTRIAN = ("PEDESTRIAN STOPS","Stops of pedestrians based on 'reasonable suspicion'. May lead to a frisk.")
    PEDESTRIAN_ARRESTS = ("PEDESTRIAN ARRESTS","Pedestrian stops leading to an arrest")
    PEDESTRIAN_CITATIONS = ("PEDESTRIAN CITATIONS","Pedestrian stops leading to a citation")
    PEDESTRIAN_WARNINGS = ("PEDESTRIAN WARNINGS","Pedestrian stops leading to a warning")
    POINTING_WEAPON = ('POINTING WEAPON', "Instances of officers pointing a weapon (firearm, Taser, etc.) at individuals.")
    SEARCHES = ("SEARCHES", "Details of searches")
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
    STOPS_INCIDENTS = ("STOPS - INCIDENTS",
        "Stops data may be split into several tables due to the possibility that multiple "+
        "subjects and officers may be involved in an incident. This table contains data on the incident.")
    STOPS_SUBJECTS = ("STOPS - SUBJECTS",
        "Stops data may be split into several tables due to the possibility that multiple "+
        "subjects and officers may be involved in an incident. This table contains data on the subjects.")
    TRAFFIC = ("TRAFFIC STOPS","Traffic stops are stops by police of motor vehicles due to reasonable suspicion " + 
        " or traffic violations.")
    TRAFFIC_INCIDENTS = ("TRAFFIC STOPS - INCIDENTS",
        "Traffic stops data may be split into several tables due to the possibility that multiple "+
        "subjects and officers may be involved in an incident. This table contains data on the incident.")
    TRAFFIC_SUBJECTS = ("TRAFFIC STOPS - SUBJECTS",
        "Traffic stops data may be split into several tables due to the possibility that multiple "+
        "subjects and officers may be involved in an incident. This table contains data on the subjects.")
    TRAFFIC_ARRESTS = ("TRAFFIC ARRESTS","Traffic stops leading to an arrest.")
    TRAFFIC_CITATIONS = ("TRAFFIC CITATIONS","Traffic stops leading to a citation.")
    TRAFFIC_WARNINGS = ("TRAFFIC WARNINGS","Traffic stops leading to a warning.")
    USE_OF_FORCE = ("USE OF FORCE","Documentation of physical force used against subjects.")
    USE_OF_FORCE_ADDITIONAL = ("USE OF FORCE - ADDITIONAL",
        "Table with additional use of force information")
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
        "subjects and officers may be involved in an incident. This table contains data on subjects and officers."+
        "A row in the data will indicate whether that row corresponds to an officer or subject.")
    VEHICLE_PURSUITS = ("VEHICLE PURSUITS","Attempts by officers in vehicles to pursue vehicles where the operator " + 
        "is believed to be aware that they are being signaled to stop but who is fleeing or ignoring the officer's attempt "+
        "to stop them.")
    WARNINGS = ("WARNINGS","Warnings for traffic and civil violations")

# Constants used in dataset parameters
MULTI = "MULTIPLE"    # For data sets that put multiple years or agencies in 1 dataset
NA = "NONE"         # None = not applicable (pandas converts "N/A" to NaN)
UNSPECIFIED = "UNSPECIFIED"

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
    RACE_ETHNICITY_SUBJECT = "SUBJECT_RACE/ETHNICITY"
    ETHNICITY_SUBJECT = "SUBJECT_ETHNICITY"
    RACE_OFFICER = "OFFICER_RACE"
    RACE_ETHNICITY_OFFICER = "OFFICER_RACE/ETHNICITY"
    ETHNICITY_OFFICER = "OFFICER_ETHNICITY"
    ETHNICITY_OFFICER_SUBJECT = "OFFICER/SUBJECT_ETHNICITY"
    RACE_OFFICER_SUBJECT = "OFFICER/SUBJECT_RACE"
    RACE_ETHNICITY_OFFICER_SUBJECT = "OFFICER/SUBJECT_RACE/ETHNICITY"
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
    RE_GROUP_OFFICER = "OFFICER_RE_GROUP"
    RE_GROUP_SUBJECT = "SUBJECT_RE_GROUP"
    RE_GROUP_OFFICER_SUBJECT = "OFFICER/SUBJECT_RE_GROUP"
    SUBJECT_OR_OFFICER = "SUBJECT_OR_OFFICER"
    FATAL_SUBJECT = "SUBJECT_FATAL"
    FATAL_OFFICER = "OFFICER_FATAL"
    FATAL_OFFICER_SUBJECT = "OFFICER/SUBJECT_FATAL"
    INCIDENT_ID = "INCIDENT_ID"
    INJURY_SUBJECT = "SUBJECT_INJURY"
    INJURY_OFFICER = "OFFICER_INJURY"
    INJURY_OFFICER_SUBJECT = "OFFICER/SUBJECT_INJURY"
    NAME_SUBJECT = "SUBJECT_NAME"
    NAME_OFFICER = "OFFICER_NAME"
    NAME_OFFICER_SUBJECT = "OFFICER/SUBJECT_NAME"
    ZIP_CODE = 'ZIP_CODE'

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
            elif match and match.group(2)=="FATAL":
                defs.append(f"Whether {match.group(1).lower()} was fatally injured in an officer-involved shooting or use of force")
                sort_by.append(match.group(2).title())
            elif match and match.group(2)=="INJURY":
                defs.append(f"Whether {match.group(1).lower()} was injured in an officer-involved shooting or use of force")
                sort_by.append(match.group(2).title())
            elif match and match.group(2)=="NAME":
                defs.append(f"Name of {match.group(1).lower()} in an officer-involved shooting")
                sort_by.append(match.group(2).title())
            elif match:
                addon = ''
                if match.group(2)=="RE GROUP":
                    desc = "Convenience column identical to Race/Ethnicity (if there is a merged Race/Ethncity column) or Race (otherwise)"
                else:
                    desc = match.group(2).title()
                if match.group(1)=="OFFICER/SUBJECT":
                    defs.append(f'{desc} of either an officer or subject (depending on column "{self.SUBJECT_OR_OFFICER}")'+addon)
                else:
                    defs.append(f"{desc} of {match.group(1).lower()}"+addon)
                sort_by.append(match.group(2).title())
            else:
                if a=="DATETIME":
                    defs.append("Combination of date and time when both columns are found (not generated when detected date column contains datetime values)")
                elif a=='INCIDENT_ID':
                    defs.append("A unique incident ID given to an incident (arrest, use of force, etc.). "+
                                "It can be used to relate information across tables. Only standardized by Table.merge function.")
                else:
                    defs.append(v.replace("_"," ").title())
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

class _Label:
    def __init__(self, full, compact=None) -> None:
        self.full = full
        self.compact = compact if compact else full[0]

_eth_cats_basic = {
    _eth_keys.LATINO:_Label("HISPANIC/LATINO"),
    _eth_keys.MIDDLE_EASTERN:_Label("MIDDLE EASTERN",'ME'),
    _eth_keys.NONLATINO:_Label("NON-HISPANIC/NON-LATINO",'NH'),
    _eth_keys.UNKNOWN:_Label("UNKNOWN","UNKNOWN"),
    _eth_keys.UNSPECIFIED:_Label(UNSPECIFIED,UNSPECIFIED),
}

_race_cats_basic = {
    _race_keys.AAPI:_Label("ASIAN/PACIFIC ISLANDER",'AAPI'),
    _race_keys.ASIAN:_Label("ASIAN"),
    _race_keys.BLACK:_Label("BLACK"),
    _race_keys.LATINO:_Label("HISPANIC/LATINO"),
    _race_keys.MULTIPLE:_Label("MULTIPLE","MULTIPLE"),
    _race_keys.INDIGENOUS:_Label("INDIGENOUS"),
    _race_keys.OTHER:_Label("OTHER"),
    _race_keys.OTHER_UNKNOWN:_Label("OTHER OR UNKNOWN",'OTHER OR UNKNOWN'),
    _race_keys.UNKNOWN:_Label("UNKNOWN","UNKNOWN"),
    _race_keys.UNSPECIFIED:_Label(UNSPECIFIED, UNSPECIFIED),
    _race_keys.WHITE:_Label("WHITE","W")
}

_more_race_cats = {
    _race_keys.PACIFIC_ISLANDER : _Label("HAWAIIAN/PACIFIC ISLANDER","H/PI"),
    _race_keys.MIDDLE_EASTERN:_Label("MIDDLE EASTERN","ME"),
    # Use in CA stops data: https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2023-01/RIPA%20Dataset%20Read%20Me%202021%20Final%20rev%20011223.pdf
    _race_keys.MIDDLE_EASTERN_SOUTH_ASIAN:_Label("MIDDLE EASTERN/SOUTH ASIAN","ME/SA"),
    _race_keys.SOUTH_ASIAN:_Label("SOUTH ASIAN","SA"),
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
    UNSPECIFIED_OR_ANOTHER = "UNSPECIFIED_OR_ANOTHER_GENDER_IDENTITY" # https://www.state.gov/x-gender-marker-available-on-u-s-passports-starting-april-11/

_gender_keys = _Genders()

_genders = {
    _gender_keys.MALE:_Label("MALE"),
    _gender_keys.FEMALE:_Label("FEMALE"),
    _gender_keys.TRANSGENDER_MALE:_Label("TRANSGENDER MALE","TM"),
    _gender_keys.TRANSGENDER_FEMALE:_Label("TRANSGENDER FEMALE","TF"),
    _gender_keys.TRANSGENDER:_Label("TRANSGENDER","T"),
    _gender_keys.GENDER_NONCONFORMING:_Label("GENDER NON-CONFORMING","GNC"),
    _gender_keys.TRANSGENDER_OR_GENDER_NONCONFORMING:_Label("TRANSGENDER OR GENDER NON-CONFORMING","T/GNC"),
    _gender_keys.GENDER_NONBINARY:_Label("GENDER NON-BINARY","GNB"),
    _gender_keys.OTHER:_Label("OTHER"),
    _gender_keys.UNKNOWN:_Label("UNKNOWN","UNKNOWN"),
    _gender_keys.UNSPECIFIED:_Label(UNSPECIFIED,UNSPECIFIED),
    _gender_keys.UNSPECIFIED_OR_ANOTHER:_Label('UNSPECIFIED_OR_ANOTHER_GENDER_IDENTITY','X'),
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

def get_race_cats(expand=False, compact=False):
    cats = _race_cats_expanded if expand else _race_cats_basic
    cats = {k: (v.compact if compact else v.full) for k,v in cats.items()}
    assert len(cats) == len(set(cats.values()))
    return cats

def get_eth_cats(compact=False):
    cats = {k: (v.compact if compact else v.full) for k,v in _eth_cats_basic.items()}
    assert len(cats) == len(set(cats.values()))
    return cats

def get_gender_keys():
    return deepcopy(_gender_keys)

def get_gender_cats(compact=False):
    cats = {k: (v.compact if compact else v.full) for k,v in _genders.items()}
    assert len(cats) == len(set(cats.values()))
    return cats
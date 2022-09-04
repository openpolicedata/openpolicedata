# Definition of constants
from __future__ import annotations
from enum import Enum
from collections import namedtuple

# These are the types of data currently available in opd.
# They all have corresponding data loaders in data_loaders.py
# When new data loaders are added, this list should be updated.
class DataType(Enum):
    CSV = "CSV"
    ArcGIS = "ArcGIS"
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

    # Below tuples are (value, description)
    ARRESTS = ("ARRESTS", "Seizures or forcible restraints by police")
    CALLS_FOR_SERVICE = ("CALLS FOR SERVICE", "Includes dispatched calls (911 or non-emergency #) and officer-initiated calls")
    CITATIONS = ("CITATIONS","Commonly referred to as tickets, citations indicate a violation of the law and may be given for "+
        "violations such as traffic and civil violations")
    COMPLAINTS = ("COMPLAINTS","Complaints of police misconduct made internally or by the community")
    EMPLOYEE = ("EMPLOYEE","Demographic data of the police workforce")
    FIELD_CONTACTS = ("FIELD CONTACTS", "Consensual contacts between officers and the community.")
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

# Standard column names
class _Columns:
    DATE = "DATE"
    TIME = "TIME"
    DATETIME = "DATETIME"
    RACE_CIVILIAN = "RACE_CIVILIAN"
    ETHNICITY_CIVILIAN = "ETHNICITY_CIVILIAN"
    RACE_OFFICER = "RACE_OFFICER"
    ETHNICITY_OFFICER = "ETHNICITY_OFFICER"
    ETHNICITY_OFF_AND_CIV = "ETHNICITY_OFF_AND_CIV"
    RACE_OFF_AND_CIV = "RACE_OFF_AND_CIV"
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
    AAPI = "ASIAN / PACIFIC ISLANDER"
    ASIAN = "ASIAN"
    ASIAN_INDIAN = "ASIAN INDIAN"
    BAD_DATA = "BAD DATA?"
    BLACK = "BLACK"
    EXEMPT = "EXEMPT"
    HAWAIIAN = "HAWAIIAN / PACIFIC ISLANDER"
    LATINO = "HISPANIC / LATINO"
    MIDDLE_EASTERN = "MIDDLE EASTERN"
    MIDDLE_EASTERN_SOUTH_ASIAN = "MIDDLE EASTERN / SOUTH ASIAN"
    MULTIPLE = "MULTIPLE"
    NATIVE_AMERICAN = "NATIVE_AMERICAN"
    OTHER = "OTHER"
    OTHER_UNKNOWN = "OTHER / UNKNOWN"
    SOUTH_ASIAN = "SOUTH ASIAN"
    UNKNOWN = "UNKNOWN"
    UNSPECIFIED = "UNSPECIFIED"
    WHITE = "WHITE"

races = _Races()

class _Genders:
    MALE = "MALE"
    FEMALE = "FEMALE"
    TRANSGENDER_MAN = "TRANSGENDER_MAN"
    TRANSGENDER_WOMAN = "TRANSGENDER_WOMAN"
    TRANSGENDER = "TRANSGENDER"
    GENDER_NONCONFORMING = "GENDER_NONCONFORMING"
    TRANSGENDER_OR_GENDER_NONCONFORMING = "TRANSGENDER_OR_GENDER_NONCONFORMING"
    GENDER_NONBINARY = "GENDER_NON-BINARY"
    EXEMPT = "EXEMPT"
    OTHER = "OTHER"
    UNKNOWN = "UNKNOWN"
    BAD_DATA = "BAD DATA?"
    UNSPECIFIED = "UNSPECIFIED"

genders = _Genders()

class _PersonTypes:
    OFFICER = "OFFICER"
    CIVILIAN = "CIVILIAN"

person_types = _PersonTypes()
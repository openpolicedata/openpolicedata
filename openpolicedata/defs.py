# Definition of constants
from __future__ import annotations
from enum import Enum

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
    CRASH_NONMOTORIST = ("CRASHES - NONMOTORIST",
        "Crash data may be split into several tables due to the possibility that multiple "+
        "civilians and vehicles may be involved in an incident. This table contains information on non-motorists involved in a crash.")
    CRASHES_VEHICLES = ("CRASHES - VEHICLES",
        "Crash data may be split into several tables due to the possibility that multiple "+
        "civilians and vehicles may be involved in an incident. This table contains data on vehicles    .")
    EMPLOYEE = ("EMPLOYEE","Demographic data of the police workforce")
    FIELD_CONTACTS = ("FIELD CONTACTS", "Consensual contacts between officers and the community.")
    INCIDENTS = ("INCIDENTS", "Crime incident reports")
    LAWSUITS = ("LAWSUITS", "Lawsuits against a police department")
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
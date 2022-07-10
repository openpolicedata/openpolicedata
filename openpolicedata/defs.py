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

    # Definition from https://www.policedatainitiative.org/datasets/calls-for-service/
    CALLS_FOR_SERVICE = ("CALLS FOR SERVICE", "Calls for service to law enforcement agencies generally include calls " +
        "to “911” for emergency assistance and may also include calls to non-emergency numbers. Generally, “311” data " +
        "is not included in calls for service data.")
    # Definition from https://police.laws.com/police/police-citation
    CITATIONS = ("CITATIONS","A police citation, which is commonly called a 'ticket', is official documentation stating " +
        "that an individual has been made aware of a violation by an officer of the law. Police citations can be issued in " +
        "a variety of spectrums, ranging from vehicular violations to civil violations. This table is for citations resulting " +
        "from both traffic and pedestrian stops.")
    # Defintion from https://www.policedatainitiative.org/datasets/complaints/
    COMPLAINTS = ("COMPLAINTS","An important aspect of accountable public service is citizen recourse and the ability " +
        "for the community to share information regarding any potential mistreatment, including situations that may violate " +
        "agency policies and procedures and/or local ordinances or law. National law enforcement accreditation standards call " +
        "for agencies to have a process for collecting complaint information and have a process for making data about the " +
        "complaints available to the community.")
    # Definition from https://www.policedatainitiative.org/datasets/agency-workforce-demographics/
    EMPLOYEE = ("EMPLOYEE","Demographic data regarding the police workforce and in some cases, applicants")
    FIELD_CONTACTS = ("FIELD CONTACTS", "")
    # Defintion from https://www.urban.org/sites/default/files/publication/25781/412647-Key-Issues-in-the-Police-Use-of-Pedestrian-Stops-and-Searches.PDF
    PEDESTRIAN = ("PEDESTRIAN STOPS","A street stop by an officer whereby an officer stops and questions a " +
        "pedestrian based on reasonable suspicion that the pedestrian is or was recently engaged in unlawful " + 
        "activity. Street stops may lead to a limited search, called a “pat down” or frisk. If the officer " +
        "obtains probable cause that the pedestrian is or was involved in a crime, the stop could lead to a " +
        "full body search.")
    PEDESTRIAN_ARRESTS = ("PEDESTRIAN ARRESTS","Pedestrian stops table only containing data for incidents ending in arrests. " +
        "See TableType.PEDESTRIAN.description for the definitinon of a pedestrian stop.")
    PEDESTRIAN_CITATIONS = ("PEDESTRIAN CITATIONS","Pedestrian stops table only containing data for incidents ending in citations. " +
        "See TableType.CITATIONS.description for a general definition of a citation. See TableType.PEDESTRIAN.description " + 
        "for the definitinon of a pedestrian stop.")
    PEDESTRIAN_WARNINGS = ("PEDESTRIAN WARNINGS","Pedestrian stops table only containing data for incidents ending in warnings. " + 
        "A pedestrian warning is a verbal or written warning issued by a police " + 
        "officer durian a pedestrian stop. See TableType.PEDESTRIAN.description " + 
        "for the definitinon of a pedestrian stop.")
    # Definition from https://www.policedatainitiative.org/datasets/officer-involved-shootings/
    SHOOTINGS = ("OFFICER-INVOLVED SHOOTINGS","Although no national or standard definition exists, an officer " + 
        "involved shooting (OIS) may be defined as the discharge of a firearm, which may include accidental and " +
        "intentional discharges, by a police officer, whether on or off duty. In some cases OIS datasets only " +
        "include instances in which an officer discharged a firearm at a person and may not include discharges " +
        "directed into or at a vehicle, animal, etc.")
    SHOOTINGS_CIVILIANS = ("OFFICER-INVOLVED SHOOTINGS - CIVILIANS",
        "Since multiple civilians and officers can be involved in a use of force incident, some departments " +
        "have separate tables for the incident, the civilians involved in the shooting, and the officers " + 
        "involved in the shooting. They are typically linked by a unique identifier. This table is for the " + 
        "civilians. See TableType.SHOOTINGS.description for definition of an officer-involved shooting.")
    SHOOTINGS_OFFICERS = ("OFFICER-INVOLVED SHOOTINGS - OFFICERS",
        "Since multiple civilians and officers can be involved in a use of force incident, some departments " +
        "have separate tables for the incident, the civilians involved in the shooting, and the officers " + 
        "involved in the shooting. They are typically linked by a unique identifier. This table is for the " + 
        "officers. See TableType.SHOOTINGS.description for definition of an officer-involved shooting.")
    SHOOTINGS_INCIDENTS = ("OFFICER-INVOLVED SHOOTINGS - INCIDENTS",
        "Since multiple civilians and officers can be involved in an officer-involved shooting, some departments " +
        "have separate tables for the incident, the civilians involved in the shooting, and the officers " + 
        "involved in the shooting. They are typically linked by a unique identifier. This table is for the " + 
        "incident. See TableType.SHOOTINGS.description for definition of an officer-involved shooting.")
    STOPS = ("STOPS","Table containing both pedestrian and traffic stops by officers. See TableType.TRAFFIC.description " +
        " and TableType.PEDESTRIAN.description for defintions of traffic and pedestrian stops, respectively.")
    TRAFFIC = ("TRAFFIC STOPS","Traffic stops are stops by police of motor vehicles due to reasonable suspicion " + 
        " or traffic violations.")
    TRAFFIC_ARRESTS = ("TRAFFIC ARRESTS","Traffic stops table for traffic stops ending in arrests. " +
        "See TableType.TRAFFIC.description for the definitinon of a traffic stop.")
    TRAFFIC_CITATIONS = ("TRAFFIC CITATIONS","Traffic stops table for traffic stops ending in citations. " +
        "See TableType.CITATIONS.description for a general definition of a citation and " +
        "TableType.TRAFFIC.description for the definitinon of a traffic stop.")
    # Defintion from https://legalbeagle.com/7700165-traffic-citation-warning.html
    TRAFFIC_WARNINGS = ("TRAFFIC WARNINGS","Traffic stops table for traffic stops ending in citations. " +
        "A traffic citation warning is a verbal or written warning issued by a police " + 
        "officer in the event of a traffic violation" + 
        "See TableType.TRAFFIC.description for the definitinon of a traffic stop.")
    # Definition from https://www.policedatainitiative.org/use-of-force/
    USE_OF_FORCE = ("USE OF FORCE","The use of force can generally be defined as the means of compelling compliance or " + 
        "overcoming resistance to an officer’s command(s) in order to protect life or property or to take a person into custody. " + 
        "For this reason, some agencies refer to the use of force as “response to resistance.” Types of force can include verbal, " + 
        "physical, chemical, impact, electronic, and firearm. Other definitions of the use of force in law enforcement may differ.")
    USE_OF_FORCE_CIVILIANS = ("USE OF FORCE - CIVILIANS",
        "Since multiple civilians and officers can be involved in a use of force incident, some departments " +
        "have separate tables for the use of force incident, the civilians involved in the incident, and the officers " + 
        "involved in the incident. They are typically linked by a unique identifier. This table is for the " + 
        "civilians. See TableType.USE_OF_FORCE.description for definition of use of force.")
    USE_OF_FORCE_OFFICERS = ("USE OF FORCE - OFFICERS",
        "Since multiple civilians and officers can be involved in a use of force incident, some departments " +
        "have separate tables for the use of force incident, the civilians involved in the incident, and the officers " + 
        "involved in the incident. They are typically linked by a unique identifier. This table is for the " + 
        "officers. See TableType.USE_OF_FORCE.description for definition of use of force.")
    USE_OF_FORCE_INCIDENTS = ("USE OF FORCE - INCIDENTS",
        "Since multiple civilians and officers can be involved in a use of force incident, some departments " +
        "have separate tables for the use of force incident, the civilians involved in the incident, and the officers " + 
        "involved in the incident. They are typically linked by a unique identifier. This table is for the " + 
        "incident. See TableType.USE_OF_FORCE.description for definition of use of force.")
    USE_OF_FORCE_CIVILIANS_OFFICERS = ("USE OF FORCE - CIVILIANS/OFFICERS",
        "Since multiple civilians and officers can be involved in a use of force incident, some departments " +
        "have separate tables for the use of force incident, the civilians involved in the incident, and the officers " + 
        "involved in the incident. They are typically linked by a unique identifier. This table contains data for both the " + 
        "civilians and the officers. See TableType.USE_OF_FORCE.description for definition of use of force.")
    # Definitino from https://www.police.ucla.edu/other/vehicle-pursuits
    VEHICLE_PURSUITS = ("VEHICLE PURSUITS","A vehicle pursuit is an event involving one or more law enforcement officers " +
        "attempting to apprehend a suspect who is attempting to avoid arrest while operating a motor vehicle by using high " +
        "speed driving or other evasive tactics such as driving off a highway, turning suddenly or driving in a legal manner " +
        "but willfully failing to yield to an officer’s signal to stop")

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
    UNSPECIFIED = "UNSPECIFIED"

genders = _Genders()

class _PersonTypes:
    OFFICER = "OFFICER"
    CIVILIAN = "CIVILIAN"

person_types = _PersonTypes()
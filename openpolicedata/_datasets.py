from enum import Enum
import pandas as pd
import numpy as np
import requests

# SOCRATA data requires a dataset ID
# Example: For https://data.virginia.gov/resource/segb-5y2c.json, data set ID is segb-5y2c. Include key id in the lutDict with this value
class DataTypes(Enum):
    CSV = "CSV"
    # EXCEL = "Excel"
    GeoJSON = "GeoJSON"
    # ArcGIS URL should end with {Feature|Map}Server/{#} with # indicating the table/layer number
    ArcGIS = "ArcGIS"
    SOCRATA = "Socrata"

class TableTypes(Enum):
    ARRESTS = "ARRESTS"
    TRAFFIC = "TRAFFIC STOPS"
    STOPS = "STOPS"
    TRAFFIC_WARNINGS = "TRAFFIC WARNINGS"
    TRAFFIC_CITATIONS = "TRAFFIC CITATIONS"
    COMPLAINTS = "COMPLAINTS"
    EMPLOYEE = "EMPLOYEE"
    USE_OF_FORCE = "USE OF FORCE"
    ARRAIGNMENT = "ARRAIGNMENT"
    DEATHES_IN_CUSTODY = "DEATHES IN CUSTODY"
    CALLS_FOR_SERVICE = "CALLS FOR SERVICE"
    SHOOTINGS = "OFFICER-INVOLVED SHOOTINGS"

# import datasets
# df = datasets.get()  # Return a dataframe containing all datasets
# df = datasets.get(state="Virginia")  # Return a dataframe containing only datasets for Virginia

_all_states = [
    'Alabama', 'Alaska', 'American Samoa', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District Of Columbia',
    'Florida', 'Georgia', 'Guam', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine',
    'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire',
    'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Northern Mariana Islands', 'Ohio', 'Oklahoma', 'Oregon',
    'Pennsylvania', 'Puerto Rico', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virgin Islands',
    'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming'
]

_us_state_abbrev = {
    'AL' : 'Alabama', 
    'AK' : 'Alaska',
    'AS' : 'American Samoa',
    'AZ' : 'Arizona',
    'AR' : 'Arkansas',
    'CA' : 'California',
    'CO' : 'Colorado',
    'CT' : 'Connecticut',
    'DE' : 'Delaware',
    'DC' : 'District of Columbia',
    'FL' : 'Florida',
    'GA' : 'Georgia',
    'GU' : 'Guam',
    'HI' : 'Hawaii',
    'ID' : 'Idaho',
    'IL' : 'Illinois',
    'IN' : 'Indiana',
    'IA' : 'Iowa',
    'KS' : 'Kansas',
    'KY' : 'Kentucky',
    'LA' : 'Louisiana',
    'ME' : 'Maine',
    'MD' : 'Maryland',
    'MA' : 'Massachusetts',
    'MI' : 'Michigan',
    'MN' : 'Minnesota',
    'MS' : 'Mississippi',
    'MO' : 'Missouri',
    'MT' : 'Montana',
    'NE' : 'Nebraska',
    'NV' : 'Nevada',
    'NH' : 'New Hampshire',
    'NJ' : 'New Jersey',
    'NM' : 'New Mexico',
    'NY' : 'New York',
    'NC' : 'North Carolina',
    'ND' : 'North Dakota',
    'MP' : 'Northern Mariana Islands',
    'OH' : 'Ohio',
    'OK' : 'Oklahoma',
    'OR' : 'Oregon',
    'PA' : 'Pennsylvania',
    'PR' : 'Puerto Rico',
    'RI' : 'Rhode Island',
    'SC' : 'South Carolina',
    'SD' : 'South Dakota',
    'TN' : 'Tennessee',
    'TX' : 'Texas',
    'UT' : 'Utah',
    'VT' : 'Vermont',
    'VI' : 'Virgin Islands',
    'VA' : 'Virginia',
    'WA' : 'Washington',
    'WV' : 'West Virginia',
    'WI' : 'Wisconsin',
    'WY' : 'Wyoming'
}

# For data sets that put multiple years or jurisdictions in 1 dataset
MULTI = "MULTI"

def _clean_source_name(name):
    str_rem = "Police Department"
    if len(name) >= len(str_rem):
        name_end = name[-len(str_rem):]
        name = name[:-len(str_rem)]
        name_end = name_end.replace(str_rem, "")
        name += name_end

    name = name.strip()

    return name

def _clean_jurisdiction_name(name):
    return _clean_source_name(name)

class _DatasetBuilder:
    columns = {
        'ID' : pd.StringDtype(),
        'State' : pd.StringDtype(),
        'SourceName' : pd.StringDtype(),
        'Jurisdiction': pd.StringDtype(),
        'TableType': pd.StringDtype(),
        'Year': np.dtype("O"),
        'Description': pd.StringDtype(),
        'DataType': pd.StringDtype(),
        'URL': pd.StringDtype(),
        'LUT': np.dtype("O"),
    }

    row_data = []


    def add_data(self, state, jurisdiction, table_type, url, data_type, source_name=None, years=MULTI, description="", lut_dict={}):
        if state not in _all_states:
            raise ValueError(f"Unknown state: {state}")
        
        if not isinstance(years, list):
            years = [years]

        if not isinstance(url, list):
            url = [url]

        if source_name==None:
            source_name = jurisdiction

        source_name = _clean_source_name(source_name)
        jurisdiction = _clean_jurisdiction_name(jurisdiction)

        for k, year in enumerate(years):
            # TODO: Make values in lut_dict into columns of data frame
            # Socrata data must have an ID
            # If 
            if jurisdiction == MULTI and "jurisdiction_field" not in lut_dict:
                raise ValueError("Multi-jurisidiction data must have a jurisdiction field")
            if year == MULTI and "date_field" not in lut_dict and table_type != TableTypes.EMPLOYEE:
                raise ValueError("Multi-year data must have a date field")
            if data_type == DataTypes.SOCRATA and "id" not in lut_dict:
                raise ValueError("Socrata data must have an ID field")
            self.row_data.append([0, state, source_name, jurisdiction, table_type.value, year, description, data_type.value, url[k], lut_dict])


    def add_stanford_data(self):
        url = "https://openpolicing.stanford.edu/data/"

        r = requests.get(url)

        def find_next(r, string, last_loc):
            new_loc = r.text[last_loc+1:].find(string)
            if new_loc >= 0:
                new_loc += last_loc+1
            return new_loc

        def find_next_state(r, last_loc):
            new_loc = find_next(r, '<tr class="state-title">', last_loc)
            if new_loc < 0:
                return new_loc, None
            td_loc = find_next(r, '<td', new_loc)
            start = find_next(r, '>', td_loc)+1
            end = find_next(r, '<', start)
            name = r.text[start:end].strip()
            name = _us_state_abbrev[name]
            return new_loc, name

        def find_next_pd(r, last_loc):
            new_loc = find_next(r, '<td class="state text-left" data-title="State">', last_loc)
            if new_loc < 0:
                return new_loc, None, None
            span_loc = find_next(r, '<span', new_loc)
            start = find_next(r, '>', span_loc)+1
            end = find_next(r, '<', start)
            name = r.text[start:end].strip()
            local_str = "<sup>1</sup>"
            is_multi = r.text[end:end+len(local_str)] == local_str
            return new_loc, name, is_multi

        def find_next_csv(r, start, end):
            open_loc = start
            while open_loc < end:
                open_loc = find_next(r, '<a href', open_loc+1)
                if open_loc >= end:
                    raise ValueError("unable to find CSV")
                close_loc = find_next(r, '</a>', open_loc)
                if close_loc >= end:
                    raise ValueError("unable to find CSV")

                if "Download data as CSV" in r.text[open_loc:close_loc]:
                    first_quote = find_next(r, '"', open_loc)
                    last_quote = find_next(r, '"', first_quote+1)
                    return r.text[first_quote+1:last_quote]

            raise ValueError("unable to find CSV")

        def includes_pedestrian_stops(r, start, end):
            open_loc = find_next(r, '<td class="text-right" data-title="Stops">', start)
            if open_loc >= end:
                raise ValueError("Unable to find # of stops")

            close_loc = find_next(r, '</td>', open_loc)
            if close_loc >= end:
                raise ValueError("Unable to find # of stops")

            return '<sup>2</sup>' in r.text[open_loc:close_loc]
            
            
        row_states = [x[1] for x in self.row_data]
        row_pds = ["Charlotte" if x[3] == "Charlotte-Mecklenburg" else x[3] for x in self.row_data]
        row_types = [x[4] for x in self.row_data]

        st_loc, state = find_next_state(r, -1)
        next_st_loc, next_state = find_next_state(r, st_loc)
        pd_loc, pd_name, is_multi = find_next_pd(r, -1)
        while pd_loc >= 0 and pd_loc != len(r.text):
            next_pd_loc, next_pd_name, next_is_multi = find_next_pd(r, pd_loc+1)
            if next_pd_loc < 0:
                next_pd_loc = len(r.text)
            csv_file = find_next_csv(r, pd_loc, next_pd_loc)

            if includes_pedestrian_stops(r, pd_loc, next_pd_loc):
                table_type = TableTypes.STOPS
            else:
                table_type = TableTypes.TRAFFIC

            already_added = False
            for k in range(len(row_states)):
                if pd_name == row_pds[k] and state == row_states[k] and table_type.value == row_types[k]:
                    already_added = True
                    break

            if not already_added:
                if is_multi:
                    jurisdiction = MULTI
                    lut_dict={"date_field" : "date", "jurisdiction_field" : "department_name"}
                else:
                    jurisdiction = pd_name
                    lut_dict={"date_field" : "date"}
                self.add_data(state, jurisdiction=jurisdiction, table_type=table_type,
                            url=csv_file, data_type=DataTypes.CSV, source_name=pd_name, 
                            description="Standardized stop data from the Stanford Open Policing Project",
                            lut_dict=lut_dict)

            pd_loc = next_pd_loc
            pd_name = next_pd_name
            is_multi = next_is_multi

            if pd_loc > next_st_loc:
                st_loc = next_st_loc
                state = next_state
                next_st_loc, next_state = find_next_state(r, st_loc)


    def build_data_frame(self):
        df = pd.DataFrame(self.row_data, columns=self.columns.keys())
        keyVals = ['State', 'SourceName', 'Jurisdiction', 'TableType','Year']
        df.drop_duplicates(subset=keyVals, inplace=True)
        df = df.astype(self.columns)
        df.sort_values(by=keyVals, inplace=True, ignore_index=True)

        df["ID"] = pd.util.hash_pandas_object(df[keyVals], index=False)

        return df
    

_builder = _DatasetBuilder()

###################### Add datasets here #########################
_builder.add_data(state="Virginia", source_name="Virginia Community Policing Act", jurisdiction=MULTI, table_type=TableTypes.STOPS, url="data.virginia.gov", data_type=DataTypes.SOCRATA, 
    description="A data collection consisting of all traffic and investigatory stops made in Virginia as aggregated by Virginia Department of State Police",
    lut_dict={"id" :"2c96-texw","date_field" : "incident_date", "jurisdiction_field" : "agency_name"})
_builder.add_data(state="Virginia", jurisdiction="Fairfax County",
    table_type=TableTypes.TRAFFIC_WARNINGS, 
    url=["https://opendata.arcgis.com/datasets/f9c4429fb0dc440ba97a0616c99c9493_0.geojson",
        "https://opendata.arcgis.com/datasets/19307e74fb5948c1a9d0270b44ebb638_0.geojson"], 
    data_type=DataTypes.GeoJSON,
    years=[2019,2020],
    description="Traffic Warnings issued by Fairfax County Police")
_builder.add_data(state="Virginia", jurisdiction="Fairfax County",
    table_type=TableTypes.TRAFFIC_CITATIONS, 
    url=["https://opendata.arcgis.com/datasets/67a02d6ebbdf41f089b9afda47292697_0.geojson",
        "https://opendata.arcgis.com/datasets/1a262db8328e42d79feac20ec8424b38_0.geojson"], 
    data_type=DataTypes.GeoJSON,
    years=[2019,2020],
    description="Traffic Citations issued by Fairfax County Police")
_builder.add_data(state="Virginia", jurisdiction="Fairfax County",
    table_type=TableTypes.ARRESTS, 
    url=["https://opendata.arcgis.com/datasets/946c2420324f4151b3526698d14021cd_0.geojson",
        "https://opendata.arcgis.com/datasets/0245b41f40a9439e928f17366dfa0b62_0.geojson",
        "https://opendata.arcgis.com/datasets/26ecb857abeb45bdbb89658b1d2b6eb1_0.geojson",
        "https://opendata.arcgis.com/datasets/71900edc43ed4be79270cdf505b06209_0.geojson",
        "https://opendata.arcgis.com/datasets/c722428522bb4666a609dd282463e11e_0.geojson"], 
    data_type=DataTypes.GeoJSON,
    years=[2016,2017,2018,2019,2020],
    description="Traffic Citations issued by Fairfax County Police")
_builder.add_data("Maryland", jurisdiction="Montgomery County", 
    table_type=TableTypes.TRAFFIC, url="data.montgomerycountymd.gov", data_type=DataTypes.SOCRATA,
    description="This dataset contains traffic violation information from all electronic traffic violations issued in Montgomery County",
    lut_dict={"id" :"4mse-ku6q","date_field" : "date_of_stop"})
_builder.add_data("Maryland", jurisdiction="Montgomery County", 
    table_type=TableTypes.COMPLAINTS, url="data.montgomerycountymd.gov", data_type=DataTypes.SOCRATA,
    description="This dataset contains allegations brought to the attention of the Internal Affairs Division either through external complaints or internal complaint or recognition",
    lut_dict={"id" :"usip-62e2","date_field" : "created_dt"})
_builder.add_data(state="Colorado", jurisdiction="Denver",
    table_type=TableTypes.STOPS, 
    url=["https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer/32/"], 
    data_type=DataTypes.ArcGIS,
    description="Police Pedestrian Stops and Vehicle Stops",
    lut_dict={"date_field" : "TIME_PHONEPICKUP"})
_builder.add_data(state="North Carolina", jurisdiction="Charlotte-Mecklenburg",
    table_type=TableTypes.TRAFFIC, 
    url=["https://gis.charlottenc.gov/arcgis/rest/services/CMPD/CMPD/MapServer/14/"], 
    data_type=DataTypes.ArcGIS,
    description="Traffic Stops",
    lut_dict={"date_field" : "Month_of_Stop"})
_builder.add_data(state="North Carolina", jurisdiction="Charlotte-Mecklenburg",
    table_type=TableTypes.EMPLOYEE, 
    url=["https://gis.charlottenc.gov/arcgis/rest/services/CMPD/CMPD/MapServer/16/"], 
    data_type=DataTypes.ArcGIS,
    description="CMPD Employee Demographics")
# TODO: Combine officer-involved shootings data for Charlotte: https://data.charlottenc.gov/search?groupIds=82e7ad57f9bd4443af251fe88442dd17
# TODO: Create data loader for Burlington
# _builder.add_data(state="Vermont", jurisdiction="Burlington",
#     tableType=TableTypes.USE_OF_FORCE, 
#     url=["https://data.burlingtonvt.gov/explore/dataset/bpd-use-of-force/"], 
#     data_type=DataTypes.UNKNOWN,
#     description="Use-of-Force incidents",
#     lut_dict={"date_field" : "call_time"})
# _builder.add_data(state="Vermont", jurisdiction="Burlington",
#     tableType=TableTypes.TRAFFIC, 
#     url=["https://data.burlingtonvt.gov/explore/dataset/bpd-traffic-stops/"], 
#     data_type=DataTypes.UNKNOWN,
#     description="Traffic Stops",
#     lut_dict={"date_field" : "call_time"})
# _builder.add_data(state="Vermont", jurisdiction="Burlington",
#     tableType=TableTypes.ARRESTS, 
#     url=["https://data.burlingtonvt.gov/explore/dataset/arrests/"], 
#     data_type=DataTypes.UNKNOWN,
#     description="Arrests",
#     lut_dict={"date_field" : "arrest_date"})
# _builder.add_data(state="Vermont", jurisdiction="Burlington",
#     tableType=TableTypes.ARRAIGNMENT, 
#     url=["https://data.burlingtonvt.gov/explore/dataset/arraignment-and-bail-data/"], 
#     data_type=DataTypes.UNKNOWN,
#     description="Case level data set on arraignment and bail",
#     lut_dict={"date_field" : "arraignment_date"})
# TODO: Add in police incidents data using function as url: https://data.burlingtonvt.gov/explore/?refine.theme=Public+Safety&disjunctive.theme&disjunctive.publisher&disjunctive.keyword&sort=modified
_builder.add_data(state="California", source_name="California Department of Justice", jurisdiction=MULTI,
    table_type=TableTypes.STOPS, 
    url=["https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2020-01/RIPA%20Stop%20Data%202018.csv", 
        "https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2021-01/RIPA%20Stop%20Data%202019.csv",
        "https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2021-12/RIPA%20Stop%20Data%202020.csv"], 
    data_type=DataTypes.CSV,
    years=[2018,2019,2020],
    description="RIPA Stop Data: Assembly Bill 953 (AB 953) requires each state and local agency in California that employs peace officers to annually report to the Attorney General data on all stops, as defined in Government Code section 12525.5(g)(2), conducted by the agency's peace officers.",
    lut_dict={"date_field" : "DATE_OF_STOP", "jurisdiction_field" : "AGENCY_NAME"})
# TODO: Add in link for description of data fields. CA data has a file online containing this info
# TODO: Add data loader for Excel
# _builder.add_data(state="California", source_name="California Department of Justice", jurisdiction=MULTI,
#     tableType=TableTypes.DEATHES_IN_CUSTODY, 
#     url=["https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2021-07/DeathInCustody_2005-2020_20210603.xlsx"], 
#     data_type=DataTypes.EXCEL,
#     escription="State and local law enforcement agencies and correctional facilities report information on deaths that occur in custody or during the process of arrest in compliance with Section 12525 of the California Government Code",
#     lut_dict={"date_field" : "date_of_death_yyyy"})
# TODO: Add CA UoF: https://openjustice.doj.ca.gov/data
_builder.add_data(state="Maryland", jurisdiction="Baltimore",
    table_type=TableTypes.ARRESTS, 
    url=["https://egis.baltimorecity.gov/egis/rest/services/GeoSpatialized_Tables/Arrest/FeatureServer/0"], 
    data_type=DataTypes.ArcGIS,
    description="Arrest in the City of Baltimore",
    lut_dict={"date_field" : "ArrestDateTime"})
# TODO: Functionalize URLs that have a pattern?
_builder.add_data(state="Maryland", jurisdiction="Baltimore",
    table_type=TableTypes.CALLS_FOR_SERVICE, 
    url=["https://opendata.baltimorecity.gov/egis/rest/services/Hosted/911_Calls_For_Service_2017_csv/FeatureServer/0",
         "https://opendata.baltimorecity.gov/egis/rest/services/Hosted/911_Calls_For_Service_2018_csv/FeatureServer/0",
         "https://opendata.baltimorecity.gov/egis/rest/services/Hosted/911_Calls_For_Service_2019_csv/FeatureServer/0",
         "https://opendata.baltimorecity.gov/egis/rest/services/Hosted/911_Calls_For_Service_2020_csv/FeatureServer/0"], 
    data_type=DataTypes.ArcGIS,
    description=" Police Emergency and Non-Emergency calls to 911",
    years=[2017,2018,2019,2020],
    lut_dict={"date_field" : "calldatetime"})
_builder.add_data(state="Maryland", jurisdiction="Baltimore",
    table_type=TableTypes.CALLS_FOR_SERVICE, 
    url="https://services1.arcgis.com/UWYHeuuJISiGmgXx/arcgis/rest/services/911_Calls_for_Service_2021_Historic/FeatureServer/0", 
    data_type=DataTypes.ArcGIS,
    description=" Police Emergency and Non-Emergency calls to 911",
    years=2021,
    lut_dict={"date_field" : "callDateTime"})
include_comport = False  # These dataset links are unreliable 2/10/2022
if include_comport:
    _builder.add_data(state="Maryland", jurisdiction="Baltimore",
        table_type=TableTypes.COMPLAINTS, 
        url=["https://www.projectcomport.org/department/4/complaints.csv"], 
        data_type=DataTypes.CSV,
        description="The Baltimore Police Department’s Office of Professional Responsibility tracks and investigates both internal complaints and citizen complaints regarding officer interactions in order to better serve the people of Baltimore.",
        lut_dict={"date_field" : "occurredDate"})
    _builder.add_data(state="Maryland", jurisdiction="Baltimore",
        table_type=TableTypes.USE_OF_FORCE, 
        url=["https://www.projectcomport.org/department/4/uof.csv"], 
        data_type=DataTypes.CSV,
        description="Officers must immediately report any use of force incident, and all incidents undergo a thorough review process to ensure that the force used was reasonable, necessary, and proportional.",
        lut_dict={"date_field" : "occurredDate"})
    _builder.add_data(state="Maryland", jurisdiction="Baltimore",
        table_type=TableTypes.SHOOTINGS, 
        url=["https://www.projectcomport.org/department/4/ois.csv"], 
        data_type=DataTypes.CSV,
        description="",
        lut_dict={"date_field" : "occurredDate"})
    _builder.add_data(state="Indiana", jurisdiction="Indianapolis",
        table_type=TableTypes.COMPLAINTS, 
        url=["https://www.projectcomport.org/department/1/complaints.csv"], 
        data_type=DataTypes.CSV,
        description="The Citizens' Police Complaint Office (CPCO) gathers this data as part of accepting and investigating resident complaints about interactions with IMPD officers. More information is available in the CPCO FAQ (http://www.indy.gov/eGov/City/DPS/CPCO/Pages/faq.aspx).",
        lut_dict={"date_field" : "occurredDate"})
    _builder.add_data(state="Indiana", jurisdiction="Indianapolis",
        table_type=TableTypes.USE_OF_FORCE, 
        url=["https://www.projectcomport.org/department/1/uof.csv"], 
        data_type=DataTypes.CSV,
        description="The Indianapolis Metropolitan Police Department (IMPD) gathers this data as part of its professional standards practices.",
        lut_dict={"date_field" : "occurredDate"})
    _builder.add_data(state="Indiana", jurisdiction="Indianapolis",
        table_type=TableTypes.SHOOTINGS, 
        url=["https://www.projectcomport.org/department/1/ois.csv"], 
        data_type=DataTypes.CSV,
        description="The Indianapolis Metropolitan Police Department (IMPD) gathers this data as part of its professional standards practices.",
        lut_dict={"date_field" : "occurredDate"})
    _builder.add_data(state="Kansas", jurisdiction="Wichita",
        table_type=TableTypes.COMPLAINTS, 
        url=["https://www.projectcomport.org/department/7/complaints.csv"], 
        data_type=DataTypes.CSV,
        description="",
        lut_dict={"date_field" : "receivedDate"})
    _builder.add_data(state="Kansas", jurisdiction="Wichita",
        table_type=TableTypes.USE_OF_FORCE, 
        url=["https://www.projectcomport.org/department/7/uof.csv"], 
        data_type=DataTypes.CSV,
        description="The Wichita Police Department tracks all incidents of force used in a situation during the line of duty as part of its office of Professional Standards. ",
        lut_dict={"date_field" : "occurredDate"})

_builder.add_stanford_data()
        
datasets = _builder.build_data_frame()


def get(source_name=None, state=None, jurisdiction=None, table_type=None):
    query = ""
    if state != None:
        query += "State == '" + state + "' and "

    if source_name != None:
        query += "SourceName == '" + source_name + "' and "

    if jurisdiction != None:
        query += "Jurisdiction == '" + jurisdiction + "' and " 

    if table_type != None:
        if isinstance(table_type, TableTypes):
            table_type = table_type.value
        query += "TableType == '" + table_type + "' and "

    if len(query) == 0:
        return datasets
    else:
        return datasets.query(query[0:-5]) 

# TODO: Add function for converting "all" datasets to CSV with some filtering options

if __name__=="__main__":
    df = get()
    df = get("Virginia")

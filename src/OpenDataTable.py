from enum import Enum

# SOCRATA data requires a dataset ID
# Example: For https://data.virginia.gov/resource/segb-5y2c.json, data set ID is segb-5y2c. Include key id in the lutDict with this value
class DataTypes(Enum):
    CSV = "CSV"
    GeoJSON = "GeoJSON"
    REQUESTS = "Requests"
    SOCRATA = "Socrata"

class TableTypes(Enum):
    ARRESTS = "ARRESTS"
    TRAFFIC = "TRAFFIC STOPS"
    STOPS = "STOPS"
    TRAFFIC_WARNINGS = "TRAFFIC WARNINGS"
    TRAFFIC_CITATIONS = "TRAFFIC CITATIONS"

class OpenDataTable:
    year = None
    tableName = None
    table = None
    url = None
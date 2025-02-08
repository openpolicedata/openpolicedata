from packaging import version
import pandas as pd
import warnings

from ..exceptions import CompatSourceTableLoadError
from .. import __version__

github_compat_versions_file='https://github.com/openpolicedata/opd-data/raw/main/compatibility/compat_versions.csv'

def check_compat_source_table(column_types=None, df_compat=None, cur_ver=__version__,
                              compat_versions_file=github_compat_versions_file):
    
    df = None
    success = False
    loaded_file = None
    try:
        cur_ver = version.parse(cur_ver)
        if df_compat is None:
            df_compat = pd.read_csv(compat_versions_file, dtype=str)
        df_compat['version'] = df_compat['version'].apply(version.parse)
        df_compat = df_compat.sort_values(by='version')
        df_compat['required'] = df_compat['required'].apply(lambda x: x=='True')
        avail_ver = df_compat['version'].apply(lambda x: cur_ver<=x)
    except:
        return success, df, loaded_file
        
    if avail_ver.any():
        idx = compat_versions_file.rfind('/')
        idx = compat_versions_file.rfind('\\') if idx<0 else idx
        for k in avail_ver[avail_ver].index:
            try:
                file = compat_versions_file[:idx+1]+df_compat['csv_name'].loc[k]
                df = pd.read_csv(file, dtype=column_types)
                success = True
                loaded_file = file
                warnings.warn("This version of OpenPoliceData requires usage of the deprecated source table located at "+
                        f"{file}. OpenPoliceData will still operate with this source table. "+
                        "However, the latest datasets added to OpenPoliceData will not be available. "
                        "Updating OpenPoliceData is recommended: python -m pip install openpolicedata --upgrade.",
                        DeprecationWarning)
                break
            except:
                if df_compat['required'].loc[k]:
                    raise CompatSourceTableLoadError("This version of OpenPoliceData requires usage of the deprecated source table located at "+
                                                    f"{file}. This file cannot be loaded. This may be due to a poor internet connection. "+
                                                    "If not, updating OpenPoliceData is recommended: python -m pip install openpolicedata --upgrade.")
                
    return success, df, loaded_file
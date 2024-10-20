import glob
import os
from packaging import version
import re
import requests
import subprocess

class color:
   PURPLE = '\033[95m'
   CYAN = '\033[96m'
   DARKCYAN = '\033[36m'
   BLUE = '\033[94m'
   GREEN = '\033[92m'
   YELLOW = '\033[93m'
   RED = '\033[91m'
   BOLD = '\033[1m'
   UNDERLINE = '\033[4m'
   END = '\033[0m'

r = requests.get('https://pypi.org/pypi/openpolicedata/json')
opd_data = r.json()

file = glob.glob('**/opd_version_test.sh',recursive=True)[0]
if len(os.path.dirname(file))>0:
    os.chdir(os.path.dirname(file))

cur_dir = os.getcwd()
while os.path.basename(cur_dir)!='openpolicedata':
    cur_dir = os.path.dirname(cur_dir)

dist_dir = os.path.join(cur_dir, 'dist')
assert os.path.exists(dist_dir)

whls = glob.glob(os.path.join(dist_dir, '*.whl'))
assert len(whls)

pd_future_warnings = ["FutureWarning: errors='ignore' is deprecated", 
                      'FutureWarning: A value is trying to be set on a copy of a DataFrame']

for w in whls:
    ver = re.search(r'openpolicedata-(?P<ver>\d+\.?\d*\.?\d*)-py3-none-any.whl', w).group('ver')
    out = subprocess.run(['./opd_version_test.sh '+ver+' '+w], capture_output=True, text=True, shell=True)

    if version.parse(ver) == version.parse('0.4'):
        print(f'{color.YELLOW}Arcgis dependency was removed in version 0.4{color.END}')
    elif version.parse(ver) == version.parse('0.5.4'):
        print(f'{color.YELLOW}packaging library was not in dependencies prior to version 0.5.4. Library manually installed for versions <0.5.4{color.END}')

    if "ModuleNotFoundError: No module named 'openpolicedata'" in out.stderr:
        print(f'{color.RED}{ver}: FAILURE{color.END} to install version')
    else:
        packaging_installed = 'Installing packaging' in out.stdout.split('\n')
        std_err = [x for x in out.stderr.split('\n') if len(x)>0]
        if packaging_installed:
            pkg_added = std_err.index("ModuleNotFoundError: No module named \'packaging\'")
            std_err = std_err[pkg_added+1:]

            assert version.parse(ver) < version.parse('0.5.4')
            
        future_warning = len(std_err)>0 and any(x in std_err[0] for x in pd_future_warnings)
        
        if future_warning:
            print(f'{color.GREEN}{ver}: SUCCESS{color.END} with pandas FutureWarning')
        elif len(std_err)==0:
            # Success!
            print(f'{color.GREEN}{ver}: SUCCESS{color.END}')
        else:
            raise NotImplementedError()
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
    elif version.parse(ver) == version.parse('0.6'):
        print(f'{color.YELLOW}Compatibility tables were not introduced prior to version 0.6.{color.END}')

    if "ModuleNotFoundError: No module named 'openpolicedata'" in out.stderr:
        print(f'{color.RED}{ver}: FAILURE{color.END} to install version')
    else:
        packaging_installed = 'Installing packaging' in out.stdout.split('\n')
        std_err = [x for x in out.stderr.split('\n') if len(x)>0]
        if packaging_installed:
            pkg_added = std_err.index("ModuleNotFoundError: No module named \'packaging\'")
            std_err = std_err[pkg_added+1:]

            assert version.parse(ver) < version.parse('0.5.4')

        # Parse std_err
        warnings = set()
        errors = []
        idx = 0
        std_err_copy = std_err.copy()
        while idx<len(std_err_copy):
            s = std_err_copy[idx]
            if re.search(r'^(?:/[\w\.\-]+)+\.py',s):
                m = re.search(r'^(?:/[\w\.\-]+)+\.py\:\d+\:\s+(\w+Warning)\:',s)
                warnings.add(m.group(1))

                # Remove message
                std_err_copy.pop(idx)
                while idx<len(std_err_copy) and \
                    not (re.search(r'^(?:/[\w\.\-]+)+\.py',std_err_copy[idx]) or std_err_copy[idx].startswith('Traceback ')):
                    std_err_copy.pop(idx)
            elif s.startswith('Traceback '):
                std_err_copy.pop(idx)
                last_msg = None
                while idx<len(std_err_copy) and \
                    not (re.search(r'^(?:/[\w\.\-]+)+\.py',std_err_copy[idx]) or std_err_copy[idx].startswith('Traceback ')):
                    last_msg = std_err_copy[idx]
                    std_err_copy.pop(idx)

                if not re.search(r'^[A-Z]+Error\:', last_msg, flags=re.IGNORECASE):
                    raise ValueError(f"Unable to parse std_err: {std_err}")
                errors.append(last_msg)
            else:
                raise ValueError(f"Unexpected message: {s}")
        
        if len(errors)>0:
            print(f'{color.RED}{ver}: FAILURE{color.END} to import OPD: {", ".join(errors)}')
        elif len(warnings)>0:
            print(f'{color.GREEN}{ver}: SUCCESS{color.END} with warnings: {", ".join(warnings)}')
        elif len(std_err)==0:
            # Success!
            print(f'{color.GREEN}{ver}: SUCCESS{color.END}')
        else:
            raise NotImplementedError()
from io import BytesIO
import pytest
import re
import requests
import sys
from zipfile import ZipFile

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders
import pandas as pd

import warnings

def test_load_multi_excel_file_zip():
    url = 'https://cdn.muckrock.com/foia_files/2024/05/29/evidence.com_case_P008746_package_1_of_1_created_2024-05-28T12_58_42Z.zip'
    file = 'data1.xlsx'
    loader = data_loaders.Excel(url, data_set=file)
    df = loader.load(pbar=False)

    r = requests.get(url, stream=True)
    r.raise_for_status()
    b = BytesIO()
    for data in r.iter_content(100000):
        b.write(data)

    b.seek(0)
    z = ZipFile(b, 'r')
    df_true = pd.read_excel(BytesIO(z.read(file)))
    df_true = df_true.convert_dtypes()

    assert df.equals(df_true)


def test_excel():
    url = "https://www.norristown.org/DocumentCenter/View/1789/2017-2018-Use-of-Force"
    date_field = "Date"
    loader = data_loaders.Excel(url, date_field=date_field, data_set='2017-2018')
    assert loader.isfile()
    df = loader.load(pbar=False)

    offset = 1
    nrows = len(df)-offset-1
    df_offset = loader.load(offset=offset,nrows=nrows, pbar=False)
    assert df_offset.equals(df.iloc[offset:nrows+offset].reset_index(drop=True))

    df_offset = loader.load(offset=offset, pbar=False)
    assert df_offset.equals(df.iloc[offset:].reset_index(drop=True))

    df_comp = pd.read_excel(url)
    df_comp = df_comp.convert_dtypes()
    df_comp.columns = [x.strip() if isinstance(x, str) else x for x in df_comp.columns]

    with pytest.raises(ValueError):
        count = loader.get_count()
    count = loader.get_count(force=True)
    assert len(df_comp) == count

    # Testing 2nd call which should used cached value
    assert count == loader.get_count(force=True)

    assert df_comp.equals(df)

    with pytest.raises(ValueError):
        loader.get_years()

    years = loader.get_years(force=True)

    df = df.astype({date_field: 'datetime64[ns]'})
    assert list(df[date_field].dt.year.sort_values(ascending=True).dropna().unique()) == years

    nrows = 7
    df = loader.load(nrows=nrows, pbar=False)        
    df_comp = pd.read_excel(url, nrows=nrows)
    df_comp = df_comp.convert_dtypes()
    df_comp.columns = [x.strip() if isinstance(x, str) else x for x in df_comp.columns]
    assert df_comp.equals(df)


# Add Norwich OIS and UoF to Unnamed column test
# Unnamed column: https://northamptonpd.com/images/ODP%20Spreadsheets/2021/Use%20of%20Force.xlsx
@pytest.mark.parametrize('src, url, multitable', [
    ("Norwich", "https://www.norwichct.org/ArchiveCenter/ViewFile/Item/922", True), # Multiple separated tables in same sheet for different years
    ("Norwich", "https://www.norwichct.org/ArchiveCenter/ViewFile/Item/771", False), # 1st row is just the year and data type
    ("Norwich", "https://www.norwichct.org/ArchiveCenter/ViewFile/Item/882", False), # 1st row is just the year and data type
    ("Norwich", "https://www.norwichct.org/ArchiveCenter/ViewFile/Item/923", False), # 1st row is just the year and data type
    ("Northampton", "https://northamptonpd.com/images/ODP%20Spreadsheets/2021/Use%20of%20Force.xlsx", False), # 1st row is just the year and data type
    ('Omaha', 'https://cdn.muckrock.com/outbound_request_attachments/OmahaPoliceDepartment/87672/OIS202010-2019202.xlsx', True)
])
def test_1st_row_not_headers(skip, src, url, multitable):
    if src in skip:
        return

    loader = data_loaders.Excel(url)
    df = loader.load(pbar=False)

    def clean_df(df):
        df.columns= [x for x in df.iloc[0]]
        df = df.drop(index=df.index[0])
        if multitable:
            keep = df.apply(lambda x: not all([y==df.columns[k] for k,y in enumerate(x)]), axis=1)
            keep = keep & df.apply(lambda x: not x.iloc[2:].isnull().all(), axis=1)
            df = df[keep]

        df = df.reset_index(drop=True)
        df = df.convert_dtypes()
        df.columns = [x.strip() if isinstance(x, str) else x for x in df.columns]

        df = df.dropna(axis=0, thresh=2).reset_index(drop=True)

        return df
    
    headers = {'User-agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A'}
    r = requests.get(url, stream=True, headers=headers)
    df_comp = pd.read_excel(BytesIO(r.content))
    if src=='Omaha':
        reps = [7, 2, 1]
        first_col_row = list(df_comp.loc[5])
        ct = 0
        rep_idx = 0
        for k, x in enumerate(first_col_row):
            if pd.notnull(x):
                ct = 1
            elif ct:
                ct+=1
                first_col_row[k] = first_col_row[k-1]
                if ct==reps[rep_idx]:
                    ct = 0
                    rep_idx+=1
    
        df_comp.columns = [x+" "+y if pd.notnull(x) else y for x,y in zip(first_col_row, list(df_comp.loc[6]))]
        df_comp = df_comp.loc[7:]
        df_comp = df_comp[df_comp['VICTIM/SUSPECT GANG AFFIL'].notnull() | df_comp['OFFICER NAME'].notnull() | df_comp['OFFICER SERGEANT'].notnull()]
        df_comp = df_comp[~df_comp['OFFICER SERGEANT'].isin(['OFFICER', 'SERGEANT'])]
        df_comp = df_comp.reset_index(drop=True).convert_dtypes()
    else:
        df_comp = clean_df(df_comp)

    pd.testing.assert_frame_equal(df, df_comp)


@pytest.mark.parametrize('src, url, date_field, yrs', [
    ("Northampton", "https://northamptonpd.com/images/ODP%20Spreadsheets/2014-2020_MV_Pursuits_incident_level_data.xlsx", "Date", range(2014,2021)), # This dataset has a typo in 1 of the year sheet names
    ("Northampton", "https://northamptonpd.com/images/ODP%20Spreadsheets/NPD_Use_of_Force_2014-2020_incident_level_data.xlsx", "Year", range(2014,2021)), # This dataset has a typo in the column names of some sheets
    ('Louisville', 'https://www.arcgis.com/sharing/rest/content/items/73672aa470da4095a88fcac074ee00e6/data', 'Year', range(2011, 2022))
]
)
def test_excel_year_sheets(skip, src, url, date_field, yrs):
    if src in skip:
        return
    
    warnings.filterwarnings('ignore', message='Identified difference in column names', category=UserWarning)
    warnings.filterwarnings('ignore', message=r"Column '.+' in current DataFrame does not match '.+' in new DataFrame. When they are concatenated, both columns will be included.", category=UserWarning)

    loader = data_loaders.Excel(url, date_field=date_field)

    years = loader.get_years()
    yrs = [x for x in yrs]
    assert years == yrs

    def clean_df(df, yr):
        if all(['Unnamed' in x for x in df.columns[2:]]):
            df.columns= [x for x in df.iloc[0]]
            df = df.drop(index=df.index[0])
        elif any('Unnamed' in x for x in df.columns):
            new_cols = []
            addon = ''
            for c in df.columns:
                if pd.isnull(df.loc[0,c]):
                    addon = ''
                    new_cols.append(c)
                elif c.lower().endswith('info'):
                    addon = re.sub(r'[Ii]nfo', '', c).strip() + ' '
                    new_cols.append(addon + df.loc[0,c])
                else:
                    new_cols.append(addon + df.loc[0,c])

            df = df.copy() # Avoids any warnings from pandas
            df.columns = new_cols
            df = df.iloc[1:]

        df = df.reset_index(drop=True)
        df['Year'] = yr
        if 'Month' in df:
            cols = []
            for c in df.columns:
                if c=='Month':
                    cols.append('Year')
                    cols.append(c)
                elif c!='Year':
                    cols.append(c)
            df = df[cols]
        df = df.convert_dtypes()
        df.columns = [x.strip() if isinstance(x, str) else x for x in df.columns]
        if pd.isnull(df.columns[0]):
            df = df.iloc[:, 1:]
        return df
    
    df_comp = pd.read_excel(url, sheet_name=str(yrs[0]))
    df_comp = clean_df(df_comp, yrs[0])

    # Load all years
    df_loaded1 = loader.load(year=yrs[0], pbar=False)

    assert df_comp.equals(df_loaded1)

    df_comp = pd.read_excel(url, sheet_name=str(yrs[1]))
    df_comp = clean_df(df_comp, yrs[1])

    # Load all years
    df_loaded2 = loader.load(year=yrs[1], pbar=False)

    assert df_comp.equals(df_loaded2)

    df_multi = loader.load(year=[yrs[0]-1,yrs[1]], pbar=False)

    df_loaded2.columns = df_loaded1.columns  # This takes care of case where columns had typos which is handled by data loader
    assert df_multi.equals(pd.concat([df_loaded1, df_loaded2], ignore_index=True))

    df = loader.load(pbar=False)

    df2 = df[df_multi.columns].head(len(df_multi)).convert_dtypes()
    pd.testing.assert_frame_equal(df2, df_multi, check_dtype=False)

    df_last = loader.load(year=years[-1], pbar=False)
    if 'Incident/Type of Charges' in df_last:
        df_last = df_last.rename(columns={'Incident/Type of Charges':'Incident Type/Charges',
                                          'Event':'Event #',
                                          'Alcohol/Drugs':'Alcohol Drugs',
                                          'Arrest or ProtectiveCustody':'Arrest or Protective Custody'})
    elif 'Lethal Y/N' in df_last:
        df_last = df_last.rename(columns={'Lethal Y/N':'Lethal Y/YS/N'})
    df2 = df[df_last.columns].tail(len(df_last)).reset_index(drop=True).convert_dtypes()
    pd.testing.assert_frame_equal(df2, df_last, check_dtype=False)


def test_excel_header():
    url = "https://www.cityofsparks.us/2000-2024-SPD-OIS-Incidents%20(3).xlsx"

    loader = data_loaders.Excel(url)
    df = loader.load(pbar=False)

    df_comp = pd.read_excel(url)
    df_comp.columns= [x for x in df_comp.iloc[3]]
    df_comp.drop(index=df_comp.index[0:4], inplace=True)
    df_comp.reset_index(drop=True, inplace=True)
    df_comp = df_comp.convert_dtypes()
    df_comp.columns = [x.strip() if isinstance(x, str) else x for x in df_comp.columns]
    df_comp = df_comp.dropna(thresh=10)

    assert(df_comp.equals(df))


def test_excel_xls():
    url = r"http://gouda.beloitwi.gov/WebLink/0/edoc/66423/3Use%20of%20Force%202017%20-%20last%20updated%201-12-18.xls"

    try:
        df_comp = pd.read_excel(url)
    except Exception as e:
        if len(e.args) and e.args[0]=='Excel file format cannot be determined, you must specify an engine manually.':
            r = requests.get(url)
            r.raise_for_status()
            text = r.content
            file_like = BytesIO(text)
            df_comp = pd.read_excel(file_like)
        else:
            raise e
    df_comp = df_comp.convert_dtypes()
    df_comp.columns = [x.strip() if isinstance(x, str) else x for x in df_comp.columns]
    df = data_loaders.Excel(url).load()

    assert df_comp.equals(df)


def test_excel_xls_protected():
    url = "http://www.rutlandcitypolice.com/app/download/5136813/ResponseToResistance+2015-2017.xls"

    r = requests.get(url)
    r.raise_for_status()

    import os
    import msoffcrypto
    import tempfile
    # Create a file path by joining the directory name with the desired file name
    output_directory = tempfile.gettempdir()
    file_path = os.path.join(output_directory, 'temp1.xls')

    # Write the file
    with open(file_path, 'wb') as output:
        output.write(r.content)

    file_path_decrypted = os.path.join(output_directory, 'temp2.xls')
    # Try and unencrypt workbook with magic password
    fp = open(file_path, 'rb')
    wb_msoffcrypto_file = msoffcrypto.OfficeFile(fp)

    # https://stackoverflow.com/questions/22789951/xlrd-error-workbook-is-encrypted-python-3-2-3
    # https://nakedsecurity.sophos.com/2013/04/11/password-excel-velvet-sweatshop/
    wb_msoffcrypto_file.load_key(password='VelvetSweatshop')
    with open(file_path_decrypted, 'wb') as output:
        wb_msoffcrypto_file.decrypt(output)

    fp.close()

    with open(file_path_decrypted, 'rb') as f:
        df_comp = pd.read_excel(f)

    os.remove(file_path)
    os.remove(file_path_decrypted)

    loader = data_loaders.Excel(url)
    df = loader.load(pbar=False)

    df_comp = df_comp.convert_dtypes()
    df_comp.columns = [x.strip() if isinstance(x, str) else x for x in df_comp.columns]
    df_comp = df_comp[[x for x in df_comp.columns if 'Unnamed' not in x]]
    assert df_comp.equals(df)

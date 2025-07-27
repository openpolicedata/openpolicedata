import openpolicedata as opd

src = opd.Source('Colorado Springs')
try:
    t = src.load(table_type='OFFICER-INVOLVED SHOOTINGS', date=2023)
except:
    try:
        t = src.load(table_type='OFFICER-INVOLVED SHOOTINGS', year=2023)
    except:
        t = src.load_from_url(year='MULTIPLE', table_type='OFFICER-INVOLVED SHOOTINGS')
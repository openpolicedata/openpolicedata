import numpy as np
import pandas as pd
import datetime as dt

def parse_date_to_datetime(date_col):
    if len(date_col.shape)==2:
        if date_col.shape[1] > 1:
            dts = date_col["year"][pd.notnull(date_col["year"])]
            if hasattr(dts.iloc[0], "year"):
                un_vals = pd.to_datetime(dts.unique())
                if (un_vals.month != 1).any() or (un_vals.day != 1).any() or (un_vals.hour != 0).any() or \
                    (un_vals.minute != 0).any() or (un_vals.second != 0).any():
                    raise ValueError("Expected year data to not contain any month, day, or time info")

                # Making a copy to avoid warning
                d = date_col.copy()
                d["year"] = date_col["year"].dt.year
            return pd.to_datetime(d)
        else:
            date_col = date_col.iloc[:,0]

    dts = date_col[date_col.notnull()]

    if len(dts) > 0:
        one_date = dts.iloc[0] 
        if not hasattr(one_date, "year"):
            if date_col.dtype == np.int64:
                # Date as number like MMDDYYYY. Need to determine order
                # Assuming year is either first or last
                if (dts < 0).any():
                    raise ValueError("Date values cannot be negative")

                year_last = dts % 10000
                year_first = np.floor(dts / 10000)
                this_year = dt.datetime.now().year

                is_valid_last = (year_last <= this_year).all() and (year_last > 1300).all()
                is_valid_first = (year_first <= this_year).all() and (year_first > 1300).all()

                if is_valid_first and is_valid_last:
                    raise ValueError("Error parsing date")
                elif is_valid_first:
                    year = date_col.apply(lambda x : np.floor(x / 10000) if not pd.isnull(x) else x)
                    month_day = date_col.apply(lambda x : x % 10000 if not pd.isnull(x) else x)
                else:
                    year = date_col.apply(lambda x : x % 10000 if not pd.isnull(x) else x)
                    month_day = date_col.apply(lambda x : np.floor(x / 10000) if not pd.isnull(x) else x)

                # Determine if month is first or last in month_day
                first_val = np.floor(month_day / 100)
                last_val = month_day % 100

                is_valid_month_first = first_val.max() < 13 and last_val.max() < 32
                is_valid_month_last = last_val.max() < 13 and first_val.max() < 32
                if is_valid_month_first and is_valid_month_last:
                    raise ValueError("Error parsing month and day")
                elif is_valid_month_first:
                    month = first_val
                    day = last_val
                else:
                    month = last_val
                    day = first_val

                return pd.to_datetime({"year" : year, "month" : month, "day" : day})
            elif date_col.dtype == "O":
                new_col = date_col.convert_dtypes()
                if new_col.dtype == "string":
                    try:
                        return pd.to_datetime(new_col)
                    except:
                        raise NotImplementedError()
                else:
                    raise NotImplementedError()
            else:
                raise NotImplementedError()

    return date_col

def combine_date_and_time(date_col, time_col):
    times_sec = parse_time_to_sec(time_col)

    dt_sec = (date_col - dt.datetime(1970,1,1)).dt.total_seconds() + times_sec

    # If date has a time, can it be assumed to be an offset from the local time zone to 
    # UTC since a time is also provided?

    return pd.to_datetime(dt_sec, unit='s')

def parse_time_to_sec(time_col):
    # Returns time in seconds since 00:00
    if time_col.dtype == np.int64 or time_col.dtype == np.float64:
        # Expected to be time as integer in 24-hr HHMM format
        # Check that this is true
        hour = np.floor(time_col/100)
        min = time_col - np.floor(time_col/100)*100
        if not (12 < hour.max() < 24) or hour.min() < 0 or min.max() > 59 or (min != round(min)).any():
            raise NotImplementedError

        return hour*3600 + min*60
    elif time_col.dtype == 'O':
        new_col = time_col.convert_dtypes()
        if new_col.dtype == "string":
            # #NAME? results from Excel errors
            num_colons = new_col.apply(lambda x: x.count(":") if pd.notnull(x) and x!="#NAME?" else pd.NA)          
            num_colons = {x for x in num_colons if pd.notnull(x)}
            if len(num_colons.difference({1,2})) > 0:
                raise ValueError("Unknown time format")

            def convert_timestr_to_sec(x):
                if pd.isnull(x):
                    return x

                time_list = x.split(":")
                if "T" in time_list[0]:
                    t_loc = [k for k,x in enumerate(time_list[0]) if x=="T"]
                    time_list[0] = time_list[0][t_loc[0]+1:]

                sec_add = 0
                if "AM" in time_list[-1]:
                    time_list[-1] = time_list[-1].replace("AM", "")
                elif "PM" in time_list[-1]:
                    sec_add = 12*3600
                    time_list[-1] = time_list[-1].replace("PM", "")

                try:
                    t = float(time_list[0])*3600 + float(time_list[1])*60 + sec_add
                except:
                    return pd.NaT

                if len(time_list) > 2:
                    try:
                        t += float(time_list[2])
                    except:
                        # One case found where seconds are XX
                        pass

                return t

            return new_col.apply(convert_timestr_to_sec)
        else:
            raise NotImplementedError()
    else:
        raise NotImplementedError()
import numpy as np
import pandas as pd
import datetime as dt
import re

def parse_date_to_datetime(date_col):
    if len(date_col.shape)==2:
        if date_col.shape[1] > 1:
            dts = date_col.iloc[:,0][date_col.iloc[:,0].notnull()]
            if hasattr(dts.iloc[0], "year"):
                un_vals = pd.to_datetime(dts.unique())
                if (un_vals.month != 1).any() or (un_vals.day != 1).any() or (un_vals.hour != 0).any() or \
                    (un_vals.minute != 0).any() or (un_vals.second != 0).any():
                    raise ValueError("Expected year data to not contain any month, day, or time info")

                # Making a copy to avoid warning
                d = date_col.copy()
                d.iloc[:,0] = date_col.iloc[:,0].dt.year

                def month_name_to_num(x):
                    month_list = ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"]
                    if type(x) == str:
                        month_num = [k+1 for k,y in enumerate(month_list) if x.lower().startswith(y)]
                        return month_num[0]
                    else:
                        return x

                d.iloc[:,1] = date_col.iloc[:,1].apply(month_name_to_num)

                return pd.to_datetime(d)
        else:
            date_col = date_col.iloc[:,0]

    dts = date_col[date_col.notnull()]

    if len(dts) > 0:
        one_date = dts.iloc[0] 
        if not hasattr(one_date, "year"):
            is_num = date_col.dtype == np.int64
            if not is_num:
                # Try to convert to all numbers
                new_col = date_col.convert_dtypes()
                if new_col.dtype == "string" and \
                    new_col.apply(lambda x: pd.isnull(x) or x.isdigit() or x.strip()=="").all():
                    date_col = new_col.apply(lambda x: int(x) if (pd.notnull(x) and x.isdigit()) else np.nan)
                    dts = date_col[date_col.notnull()]
                    is_num = True

            if is_num:
                # Date as number like MMDDYYYY. Need to determine order
                # Assuming year is either first or last
                if (dts < 0).any():
                    raise ValueError("Date values cannot be negative")

                year_last = dts % 10000
                year_first = np.floor(dts / 10000)
                this_year = dt.datetime.now().year

                is_valid_last = (year_last <= this_year).all() and (year_last > 1300).all()
                is_valid_first = (year_first <= this_year).all() and (year_first > 1300).all()

                any_valid = True
                if is_valid_first and is_valid_last:
                    raise ValueError("Error parsing date")
                elif is_valid_first:
                    year = date_col.apply(lambda x : np.floor(x / 10000) if not pd.isnull(x) else x)
                    month_day = date_col.apply(lambda x : x % 10000 if not pd.isnull(x) else x)
                elif is_valid_last:
                    year = date_col.apply(lambda x : x % 10000 if not pd.isnull(x) else x)
                    month_day = date_col.apply(lambda x : np.floor(x / 10000) if not pd.isnull(x) else x)
                else:
                    any_valid = False

                if any_valid:
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
                    elif is_valid_month_last:
                        month = last_val
                        day = first_val
                    else:
                        any_valid = False

                    if any_valid:
                        return pd.to_datetime({"year" : year, "month" : month, "day" : day})

                if not any_valid:
                    # This may be Epoch time
                    try:
                        new_date_col = pd.to_datetime(dts, unit='s')
                    except pd._libs.tslibs.np_datetime.OutOfBoundsDatetime:
                        new_date_col = pd.to_datetime(dts, unit='ms')
                    except:
                        raise

                    if (new_date_col.dt.year > this_year).any() or (new_date_col.dt.year < 1950).any():
                        raise ValueError("Date is outside acceptable range (1950 to this year)")

                    return new_date_col
                    
            elif date_col.dtype == "O":
                new_col = date_col.convert_dtypes()
                if new_col.dtype == "string":
                    p = re.compile(r"\d{1,2}[/-]\d{1,2}[/-]\d{4}")
                    p2 = re.compile(r"\d{4}[/-]\d{1,2}[/-]\d{1,2}")
                    p3 = re.compile(r"\d{2}-[A-Z][a-z][a-z]-\d{2}")
                    p_not_match = re.compile(r"\d{1,2}[:\.]?\d\d[:\.]?\d?\d?")
                    num_match = 0
                    num_not_match = 0
                    k = 0
                    num_check = 5
                    for m in range(len(new_col)):
                        if pd.notnull(new_col[m]) and len(new_col[m].strip())!=0:
                            if p.search(new_col[m])!=None or p2.search(new_col[m])!=None or p3.search(new_col[m])!=None:
                                num_match+=1
                            elif p_not_match.match(new_col[m])==None:
                                a = 1
                            else:
                                num_not_match+=1
                            k+=1

                        if k==num_check:
                            break

                    if num_match<num_check-1:
                        raise ValueError("Column is not a date column")
                    try:
                        return pd.to_datetime(new_col, errors="coerce")
                    except:
                        def to_dt(x):
                            try:
                                return pd.to_datetime(x)
                            except:
                                return pd.NaT
                        new_col = new_col.apply(to_dt)
                        if new_col.isnull().sum()/len(new_col) > 0.5:
                            raise NotImplementedError()
                        else:
                            return new_col
                else:
                    raise NotImplementedError()
            else:
                raise NotImplementedError()

    return date_col


def merge_date_and_time(date_col, time_col):
    # If date even has a time, this ignores it.
    # We assume that the time in time column is more likely to be local time
    # Often returned date is in UTC but local time is preferred for those who want to do day vs. night analysis

    # We could use:
    # return pd.to_datetime(date_col.dt.date.astype(str) + " " + time_col.astype(str), errors="coerce")
    # but not for now to catch unexpected values
    return pd.Series([d.replace(hour=t.hour, minute=t.minute, second=t.second) if (pd.notnull(d) and pd.notnull(t)) else pd.NaT for d,t in zip(date_col, time_col)])

def validate_date(date_col):
    # Fails if date column is not valid. Otherwise, returns a 
    # numerical value that is higher for more complete datetimes (i.e. date-only lower than date and time)
    date_col = parse_date_to_datetime(date_col)

    dts = date_col[date_col.notnull()]

    if len(dts) > 0:
        one_date = dts.iloc[0]
        max_val = 6
        same_sec = (dts.dt.second == one_date.second).all()
        if not same_sec: 
            return max_val
        same_min = (dts.dt.minute == one_date.minute).all()
        if not same_min: 
            return max_val-1
        same_hour = (dts.dt.hour == one_date.hour).all()
        if not same_hour: 
            return max_val-2
        same_day = (dts.dt.day == one_date.day).all()
        if not same_day: 
            return max_val-3
        same_month = (dts.dt.month == one_date.month).all()
        if not same_month: 
            return max_val-4
        else:
            return max_val-5

    return None

def validate_time(time_col, date_col=None):
    not_a_time_col_msg = "This column contains date information, and therefore, is not a time column"
    date_has_time_msg = "The date column has a time in it. There is no need for a time column."
    try:
        # Try to convert to datetime. If successful, see if there is a date value that varies.
        # If so, this is not a time column
        test_date_col = parse_date_to_datetime(time_col)
        num_unique = len(test_date_col.dt.date.unique())
        # If there are more than 2 dates or 2 dates are only 1 day apart (could be due to time zone change),
        # then the supposed time contains date information
        if num_unique > 2 or (num_unique==2 and  abs(test_date_col[0]-test_date_col[1]) > pd.Timedelta(days=1)):
            raise ValueError(not_a_time_col_msg)
    except ValueError as e:
        if len(e.args)>0 and e.args[0] in [not_a_time_col_msg, date_has_time_msg]:
            raise e
    except Exception:
        pass

    try: 
        if date_col is not None:
            new_date_col = date_col[date_col.notnull()]
            if not hasattr(new_date_col.iloc[0],'year'):
                new_date_col = parse_date_to_datetime(new_date_col)

            unique_times = pd.Series(new_date_col.apply(lambda x: x.replace(year=1970,month=1,day=1)).unique())
            # If there is more than 2 times or the times cannot be explained by a time zone differene,
            # then there is time information in the date
            if len(unique_times) > 2 or (len(unique_times)==2 and \
                unique_times.notnull().all() and abs(unique_times[1]-unique_times[0]) > pd.Timedelta(hours=1)):
                if len(unique_times)==2 and len(new_date_col)>100:
                    # There are enough dates that they shouldn't all mostly be the same
                    # See if one could be a UTC time and one could be no time
                    diffs = unique_times - unique_times.apply(lambda x: x.replace(hour=0,minute=0,second=0))
                    if not (diffs==pd.Timedelta(0)).any():
                        raise ValueError(date_has_time_msg)
                else:
                    raise ValueError(date_has_time_msg)
    except ValueError as e:
        if len(e.args)>0 and e.args[0] in [not_a_time_col_msg, date_has_time_msg]:
            raise e
    except Exception:
        pass

    col = parse_time(time_col)
    col = col[col.notnull()]
    
    if len(col) > 0:
        hours = pd.Series([t.hour if pd.notnull(t) else np.nan for t in col])
        mins = pd.Series([t.minute if pd.notnull(t) else np.nan for t in col])
        # secs = time_sec - hours*3600 - mins*60
        max_val = 3
        # same_sec = (secs == secs.iloc[0]).all()
        # if not same_sec: 
        #     return max_val
        same_min = (mins == mins.iloc[0]).all()
        if not same_min: 
            return max_val-1
        same_hour = (hours == hours.iloc[0]).all()
        if not same_hour: 
            return max_val-2
        else:
            return max_val-3

    return None


def parse_time(time_col):
    # Returns time in seconds since 00:00
    if time_col.dtype == np.int64 or time_col.dtype == np.float64:
        # Expected to be time as integer in 24-hr HHMM format
        # Check that this is true
        hour = np.floor(time_col/100)
        min = time_col - np.floor(time_col/100)*100
        if hour.max() >= 24:
            invalid = hour>=24
            if invalid.mean() < 0.01:
                # These are likely recording errors. Replace with NaN
                hour.loc[invalid] = np.nan
            else:
                raise NotImplementedError()
        if min.max() > 59:
            invalid = min>=60
            if invalid.mean() < 0.01:
                # These are likely recording errors. Replace with NaN
                min.loc[invalid] = np.nan
            else:
                raise NotImplementedError()

        return pd.Series([dt.time(hour=int(x),minute=int(y)) if (pd.notnull(x) and pd.notnull(y)) else pd.NaT for x,y in zip(hour,min)])
    elif time_col.dtype == 'O':
        new_col = time_col.convert_dtypes()
        if new_col.dtype == "string":
            def convert_timestr_to_sec(x):
                if pd.isnull(x):
                    return x

                time_list = x.split(":")
                if len(time_list)==1 and len(x.split("."))>1:
                    time_list = x.split(".")
                    if len(time_list)!=3:
                        raise NotImplementedError()

                if len(time_list)==1:
                    if len(x) == 0 or len(x) > 4:
                        if x in ["#NAME?",'#VALUE!']:
                            return pd.NaT
                        else:
                            raise ValueError("Expected HHMM format")
                    elif x.strip()=="":
                        return pd.NaT
                    min = float(x[-2:])
                    if len(x) > 2:
                        hour = float(x[:-2])
                    else:
                        hour = 0

                    if min > 59:
                        return pd.NaT

                    return dt.time(hour=int(hour),minute=int(min))

                if "T" in time_list[0]:
                    t_loc = [k for k,x in enumerate(time_list[0]) if x=="T"]
                    time_list[0] = time_list[0][t_loc[0]+1:]

                hours_add = 0
                if "AM" in time_list[-1]:
                    time_list[-1] = time_list[-1].replace("AM", "")
                    if time_list[0].strip() == "12":
                        time_list[0] = "0"
                elif "PM" in time_list[-1]:
                    hours_add = 12
                    time_list[-1] = time_list[-1].replace("PM", "")
                    if time_list[0].strip() == "12":
                        time_list[0] = "0"

                try:
                    t = dt.time(hour=int(time_list[0])+hours_add,minute=int(time_list[1]))
                except:
                    return pd.NaT

                if len(time_list) > 2:
                    try:
                        t = t.replace(second=int(time_list[2]))
                    except:
                        # One case found where seconds are XX
                        pass

                return t

            return new_col.apply(convert_timestr_to_sec)
        else:
            raise NotImplementedError()
    else:
        raise NotImplementedError()
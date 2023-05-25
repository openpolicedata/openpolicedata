import numpy as np
import pandas as pd
import datetime as dt
import re
import warnings

def parse_date_to_datetime(date_col):
    if len(date_col.shape)==2:
        if date_col.shape[1] > 1:
            dts = date_col.iloc[:,0][date_col.iloc[:,0].notnull()]
            if hasattr(dts.iloc[0], "year"):
                un_vals = to_datetime(dts.unique())
                if (un_vals.month != 1).any() or (un_vals.day != 1).any() or (un_vals.hour != 0).any() or \
                    (un_vals.minute != 0).any() or (un_vals.second != 0).any():
                    raise ValueError("Expected year data to not contain any month, day, or time info")

                # Making a copy to avoid warning
                d = date_col.copy()
                d.iloc[:,0] = date_col.iloc[:,0].dt.year

                def month_name_to_num(x):
                    month_list = ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"]
                    if isinstance(x,str) and not x.isdigit():
                        month_num = [k+1 for k,y in enumerate(month_list) if x.lower().startswith(y)]
                        return month_num[0]
                    else:
                        return int(x) if pd.notnull(x) else x

                d.iloc[:,1] = date_col.iloc[:,1].apply(month_name_to_num)

                return to_datetime(d)
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
                if new_col.dtype in ["object", "string"] and \
                    new_col.apply(lambda x: pd.isnull(x) or isinstance(x,int) or x.isdigit() or x.strip()=="").all():
                    date_col = new_col.apply(lambda x: int(x) if (pd.notnull(x) and (isinstance(x,int) or x.isdigit())) else np.nan)
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
                        return to_datetime({"year" : year, "month" : month, "day" : day})

                if not any_valid:
                    # This may be Epoch time
                    try:
                        new_date_col = to_datetime(dts, unit='s')
                    except pd._libs.tslibs.np_datetime.OutOfBoundsDatetime:
                        new_date_col = to_datetime(dts, unit='ms')
                    except:
                        raise

                    if (new_date_col.dt.year > this_year).any() or (new_date_col.dt.year < 1971).any():
                        raise ValueError("Date is outside acceptable range (1971 to this year)")
                    
                    if new_date_col.dt.year.max() < 1980:
                        raise ValueError("All dates are before 1980. This is unlikely to be a date column.")

                    return new_date_col
                    
            elif date_col.dtype == "O":
                new_col = date_col.convert_dtypes()
                if new_col.dtype == "string":
                    p = re.compile(r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}")
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
                                pass
                            else:
                                num_not_match+=1
                            k+=1

                        if k==num_check:
                            break

                    if num_match<num_check-1:
                        raise ValueError("Column is not a date column")
                    try:
                        return to_datetime(new_col, errors="coerce")
                    except:
                        def to_dt(x):
                            try:
                                return to_datetime(x)
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

def validate_date(df, match_cols_test):
    score = None
    match_cols = []
    for col_name in match_cols_test:
        try:
            # Fails if date column is not valid. Otherwise, returns a 
            # numerical value that is higher for more complete datetimes (i.e. date-only lower than date and time)
            date_col = parse_date_to_datetime(df[col_name])

            dts = date_col[date_col.notnull()]

            if len(dts) == 0:
                continue

            one_date = dts.iloc[0]
            max_val = 6
            same_sec = (dts.dt.second == one_date.second).all()
            new_score = None
            if not same_sec: 
                new_score = max_val
            same_min = (dts.dt.minute == one_date.minute).all()
            if new_score is None and not same_min: 
                new_score = max_val-1
            same_hour = (dts.dt.hour == one_date.hour).all()
            if new_score is None and not same_hour: 
                new_score = max_val-2
            same_day = (dts.dt.day == one_date.day).all()
            if new_score is None and not same_day: 
                new_score = max_val-3
            same_month = (dts.dt.month == one_date.month).all()
            if new_score is None:
                if not same_month: 
                    new_score = max_val-4
                else:
                    new_score = max_val-5

            if score == new_score:
                match_cols.append(col_name)
            elif new_score != None and (score == None or new_score > score):
                # Higher scoring item found. This now takes priority
                score = new_score
                match_cols = [col_name]
        except Exception as e:
            pass

    return match_cols

    
def validate_time(df, match_cols_test, date_col=None):
    score = None
    match_cols = []
    for col_name in match_cols_test:
        try:
            time_col = df[col_name]
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
                    continue
            except Exception:
                pass

            new_time_col = parse_time(time_col)

            try: 
                if date_col is not None:
                    date_times = date_col.dt.time
                    # Find times from the date column that differ from the time column
                    date_times = date_times[
                        date_col.dt.time.apply(lambda x: x.replace(second=0) if pd.notnull(x) else pd.NaT) != 
                        new_time_col.apply(lambda x: x.replace(second=0) if pd.notnull(x) else pd.NaT)]
                    counts = date_times.value_counts()
                    if len(counts)==0:
                        # All times are the same between date and time columns
                        raise ValueError(date_has_time_msg)
                    most_common_time = date_times.mode()

                    # If most common time is all zeros, assume no time in date
                    if most_common_time[0]!=dt.time(hour=0, minute=0, second=0):
                        # If the date has no time, it will have zeros, standard time offsets, or DST offsets
                        if len(counts)>3 or any([x.minute!=0 or x.second!=0 for x in counts.index]):
                            # Check the percentage of times in the date column whose minute is 0. It should be relatively rare
                            if sum([counts[x] for x in counts.index if x.minute!=0]) / counts.sum() > 0.4:
                                raise ValueError(date_has_time_msg)
                        else:
                            # 00:00 must be one of the numbers if 3 unique values
                            if len(counts)==3 and all([x!=dt.time(hour=0, minute=0, second=0) for x in counts.index]):
                                raise ValueError(date_has_time_msg)
                            
                            non_zero = [x for x in counts.index if x.hour!=0]
                            if len(non_zero)>1:
                                # Values should be off by one hour
                                d1 = dt.timedelta(hours=non_zero[0].hour)
                                d2 = dt.timedelta(hours=non_zero[1].hour)
                                if d2-d1 != dt.timedelta(hours=1) and d1-d2 != dt.timedelta(hours=1):
                                    raise ValueError(date_has_time_msg)
            except ValueError as e:
                if len(e.args)>0 and e.args[0] in [not_a_time_col_msg, date_has_time_msg]:
                    continue
            except Exception:
                pass

            new_time_col = new_time_col[new_time_col.notnull()]
            
            new_score = None
            if len(new_time_col) == 0:
                continue

            hours = pd.Series([t.hour if pd.notnull(t) else np.nan for t in new_time_col])
            mins = pd.Series([t.minute if pd.notnull(t) else np.nan for t in new_time_col])
            # secs = time_sec - hours*3600 - mins*60
            max_val = 3
            # same_sec = (secs == secs.iloc[0]).all()
            # if not same_sec: 
            #     return max_val
            same_min = (mins == mins.iloc[0]).all()
            if not same_min: 
                new_score = max_val-1
            same_hour = (hours == hours.iloc[0]).all()
            if new_score is None:
                if not same_hour: 
                    new_score = max_val-2
                else:
                    new_score = max_val-3
                
            if score == new_score:
                match_cols.append(col_name)
            elif new_score != None and (score == None or new_score > score):
                # Higher scoring item found. This now takes priority
                score = new_score
                match_cols = [col_name]

        except Exception as e:
            pass

    return match_cols


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
        if new_col.dtype == "string" or time_col.apply(lambda x: isinstance(x,str) or isinstance(x,int)).all():
            try:
                new_col = to_datetime(new_col)
                return new_col.dt.time
            except:
                pass

            p_date = re.compile(r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}")
            def convert_timestr_to_sec(x):
                if pd.isnull(x):
                    return x
                
                if isinstance(x,int):
                    hour = np.floor(x/100)
                    min = x - np.floor(x/100)*100
                    if hour >= 24:
                        raise NotImplementedError()
                    if min > 59:
                        raise NotImplementedError()
                    return dt.time(hour=int(hour),minute=int(min))

                x = x.replace(" ","")
                time_list = x.split(":")
                if len(time_list)==1 and len(x.split("."))>1:
                    time_list = x.split(".")
                    if len(time_list)!=3:
                        raise NotImplementedError()

                if len(time_list)==1:
                    if x.strip() in ["","-"]:
                        return pd.NaT
                    elif len(x) == 0 or len(x) > 4 or not x.isdigit():
                        if x in ["#NAME?",'#VALUE!', 'TIME'] or x.startswith('C2') or \
                            p_date.search(x):  # Date accidently entered in time column
                            # C2 values were observed in 1 dataset
                            return pd.NaT
                        else:
                            raise ValueError("Expected HHMM format")

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
                if "AM" in time_list[-1].upper():
                    time_list[-1] = time_list[-1].upper().replace("AM", "")
                    if time_list[0].strip() == "12":
                        time_list[0] = "0"
                elif "PM" in time_list[-1].upper():
                    hours_add = 12
                    time_list[-1] = time_list[-1].upper().replace("PM", "")
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
    

def to_datetime(col, *args, **kwargs):
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, message="Could not infer format")
        return pd.to_datetime(col, *args, **kwargs)
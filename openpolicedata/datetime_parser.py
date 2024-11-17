import datetime
import dateutil
import math
import numpy as np
import pandas as pd
from pandas .api.types import is_numeric_dtype
import datetime as dt
import re
import warnings
import numbers

from .utils import is_str_number

def parse_date_to_datetime(date_col):
    if len(date_col.shape)==2:
        if date_col.shape[1] > 1:
            dts = date_col.iloc[:,0][date_col.iloc[:,0].notnull()]
            if hasattr(dts.iloc[0], "year"):
                if isinstance(dts.dtype, pd.api.types.PeriodDtype):
                    un_vals = to_datetime([x.to_timestamp() for x in dts.unique()])
                else:
                    un_vals = to_datetime(dts.unique())
                if (un_vals.month != 1).any() or (un_vals.day != 1).any() or (un_vals.hour != 0).any() or \
                    (un_vals.minute != 0).any() or (un_vals.second != 0).any():
                    raise ValueError("Expected year data to not contain any month, day, or time info")

                # Making a copy to avoid warning
                d = date_col.copy()
                d[d.columns[0]] = date_col.iloc[:,0].dt.year

                def month_name_to_num(x):
                    month_list = ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"]
                    if isinstance(x,str) and not x.isdigit():
                        month_num = [k+1 for k,y in enumerate(month_list) if x.lower().startswith(y)]
                        return month_num[0]
                    else:
                        return int(x) if pd.notnull(x) else x

                d[d.columns[1]] = date_col.iloc[:,1].apply(month_name_to_num)

                if d.shape[1]==2:
                    return d.apply(lambda x: pd.Period(to_datetime(f"{int(x.iloc[1])}/1/{int(x.iloc[0])}"), 'M') 
                                   if pd.notnull(x.iloc[1])
                                   else pd.Period(to_datetime(f"{int(x.iloc[0])}"), 'Y'), axis=1)
                else:
                    return to_datetime(d)
        else:
            date_col = date_col.iloc[:,0]

    ind_is_num = date_col.notnull()
    if ind_is_num.any():
        if not hasattr(date_col[ind_is_num].iloc[0], "year") or (date_col[ind_is_num].apply(type)==str).any():
            is_num = is_numeric_dtype(date_col)
            if not is_num:
                # Try to convert to all numbers
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=RuntimeWarning, message="invalid value encountered in cast")
                    new_col = date_col.convert_dtypes()
                if new_col.dtype in ["object", "string", "string[python]"] and \
                    new_col.apply(lambda x: pd.notnull(x) and (isinstance(x, numbers.Number) or is_str_number(x))).sum() > 0 and \
                    new_col.apply(lambda x: pd.isnull(x) or isinstance(x,(pd.Timestamp,int, dt.datetime)) or \
                                  isinstance(x, numbers.Number) or is_str_number(x) or \
                                  (isinstance(x,str) and x.strip()=="")).sum()>=len(new_col)-1:
                    # Almost all numeric, null, or timestamp values and at least one numeric value
                    def clean_dates(x):
                        if pd.notnull(x) and (isinstance(x, numbers.Number) or is_str_number(x)):
                            return int(float(x))
                        elif isinstance(x,str):
                            return np.nan
                        else:
                            return x
                        
                    date_col = new_col.apply(clean_dates)

                    ind_is_num = date_col.notnull() & date_col.apply(lambda x: not isinstance(x, (pd.Timestamp, pd.Period, dt.datetime)))
                    is_num = True

            if is_num:
                # Date as number like MMDDYYYY. Need to determine order
                # Assuming year is either first or last
                dts = date_col[ind_is_num]
                if (dts < 0).any():
                    raise ValueError("Date values cannot be negative")

                year_last = dts % 10000
                year_first = np.floor(dts / 10000)
                year_last_2digit = dts % 100
                this_year = dt.datetime.now().year

                is_valid_last = (year_last <= this_year).all() and (year_last > 1300).all()
                is_valid_first = (year_first <= this_year).all() and (year_first > 1300).all()
                is_valid_last_2digit = (year_last_2digit <= this_year-2000).all() and (year_last_2digit >= 0).all()

                any_valid = True
                if is_valid_first:
                    year = dts.apply(lambda x : np.floor(x / 10000) if not pd.isnull(x) else x)
                    month_day = dts.apply(lambda x : x % 10000 if not pd.isnull(x) else x)
                elif is_valid_last:
                    year = dts.apply(lambda x : x % 10000 if not pd.isnull(x) else x)
                    month_day = dts.apply(lambda x : np.floor(x / 10000) if not pd.isnull(x) else x)
                elif is_valid_last_2digit:
                    year = 2000+dts.apply(lambda x : x % 100 if not pd.isnull(x) else x)
                    month_day = dts.apply(lambda x : np.floor(x / 100) if not pd.isnull(x) else x)
                else:
                    any_valid = False

                if any_valid:
                    # Determine if month is first or last in month_day
                    first_val = month_day.apply(lambda x: np.floor(x/100) if pd.notnull(x) and isinstance(x,numbers.Number) else x)
                    last_val = month_day % 100

                    is_valid_month_first = first_val.max() < 13 and last_val.max() < 32
                    is_valid_month_last = last_val.max() < 13 and first_val.max() < 32
                    if is_valid_month_first:
                        # If also is_valid_month_last, assuming month is first because this is more common in the U.S. 
                        month = first_val
                        day = last_val
                    elif is_valid_month_last:
                        month = last_val
                        day = first_val
                    else:
                        any_valid = False

                    if any_valid:
                        # Convert data type to object to avoid changing dtype warning and then convert to TimeStamp
                        date_col = date_col.astype('O')
                        date_col[ind_is_num] = to_datetime({"year" : year, "month" : month, "day" : day})
                        date_col[date_col.isnull()] = pd.NaT
                        return date_col.convert_dtypes()

                if not any_valid:
                    # Convert data type to object to avoid changing dtype warning and then convert to TimeStamp
                    date_col = date_col.astype('O')
                    # This may be Epoch time
                    try:
                        date_col[ind_is_num] = to_datetime(dts, unit='s')
                    except pd._libs.tslibs.np_datetime.OutOfBoundsDatetime:
                        date_col[ind_is_num] = to_datetime(dts, unit='ms')
                    except:
                        raise

                    date_col = date_col.convert_dtypes()

                    if (date_col.dt.year > this_year).any() or (date_col.dt.year < 1971).any():
                        if not (date_col.dt.year > this_year).any() and (m:=(date_col.dt.year < 1971)).mean() < 0.01:
                            date_col[m] = pd.NaT
                        else:
                            raise ValueError("Date is outside acceptable range (1971 to this year)")
                    
                    if date_col.dt.year.max() < 1980:
                        raise ValueError("All dates are before 1980. This is unlikely to be a date column.")

                    return date_col
                    
            elif date_col.dtype in ["O", "object", "string", "string[python]"]:
                new_col = date_col.convert_dtypes()
                if new_col.dtype in ["string", "string[python]"] or \
                    str in new_col.apply(type).unique():
                    p = re.compile(r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}")
                    p2 = re.compile(r"\d{4}[/-]\d{1,2}[/-]\d{1,2}")
                    p3 = re.compile(r"\d{2}-[A-Z][a-z][a-z]-\d{2,4}", re.IGNORECASE)
                    p_not_match = re.compile(r"\d{1,2}[:\.]?\d\d[:\.]?\d?\d?")
                    num_match = 0
                    num_not_match = 0
                    k = 0
                    num_check = min(5, len(new_col))
                    for m in range(len(new_col)):
                        if isinstance(new_col[m], (pd.Timestamp, dt.datetime)):
                            num_match+=1
                            k+=1
                        elif pd.notnull(new_col[m]) and len(new_col[m].strip())!=0:
                            if p.search(new_col[m]) or p2.search(new_col[m]) or p3.search(new_col[m]):
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
                        return to_datetime(new_col)
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


def merge_date_and_time(date_col, time_col, empty_time):
    # If date even has a time, this ignores it.
    # We assume that the time in time column is more likely to be local time
    # Often returned date is in UTC but local time is preferred for those who want to do day vs. night analysis

    # We could use:
    # return pd.to_datetime(date_col.dt.date.astype(str) + " " + time_col.astype(str), errors="coerce")
    # but not for now to catch unexpected values
    empty_time = empty_time.lower()
    def combine(d, t):
        if pd.isnull(d):
            return pd.NaT
        elif pd.isnull(t):
            if empty_time=="nat":
                return pd.NaT
            elif empty_time == "ignore":
                return d
            else:
                raise ValueError("empty_time must be 'NaT' or 'ignore'")
        else:
            return d.replace(hour=t.hour, minute=t.minute, second=t.second)

    return pd.Series([combine(d, t) for d,t in zip(date_col, time_col)])

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
            new_score = None
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
    max_len = 100000
    if len(df) > max_len:
        df = df.head(max_len)  # No need to validate everything
        date_col = date_col.head(max_len) if date_col is not None else date_col
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
    time_col = time_col.copy()
    if pd.api.types.is_numeric_dtype(time_col):
        # Expected to be time as integer in 24-hr HHMM format
        # Check that this is true
        time_col[(time_col==9999) | (time_col==999)] = pd.NA  # 9999 is used as error code
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
    elif time_col.dtype in ['O','string[python]']:
        new_col = time_col.convert_dtypes()
        if new_col.dtype in ["string",'string[python]'] or \
            time_col.apply(lambda x: isinstance(x,str) or isinstance(x,int) or pd.isnull(x) or isinstance(x,dt.time)).all():
            # Cleanup split AM or PM, which causes a warning from pandas
            new_col = new_col.apply(lambda x: x.replace("P M","PM").replace("A M","AM") if isinstance(x,str) else x)
            new_col = new_col.apply(lambda x: x.strftime('%H:%M') if isinstance(x,dt.time) else x)
            try:
                new_col = to_datetime(new_col)
                return new_col.dt.time
            except:
                pass

            p_date = [re.compile(r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}"),
                      re.compile(r"^\d{1,2}-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)$"),
                      re.compile(r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{1,2}$")]
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

                if (x.upper().endswith('AM') or x.upper().endswith('PM')) and \
                    x[:-2].isdigit():
                    # Time does not have colon. Try to insert one
                    x = x[:-4] + ":" + x[-4:]
                    
                time_list = x.split(":")
                if len(time_list)==1 and len(x.split("."))>1:
                    time_list = x.split(".")
                    if len(time_list)!=3:
                        raise NotImplementedError()

                if len(time_list)==1 and len(x)==5 and x[2]==';':
                    # Accidentally used ; instead of :
                    time_list = x.split(";")
                if len(time_list)==1:
                    if x.strip() in ["","-"]:
                        return pd.NaT
                    elif len(x) == 0 or len(x) > 4 or not x.isdigit():
                        if x in ["#NAME?",'#VALUE!', 'TIME','NULL'] or re.search(r'^C\d+',x) or \
                            any([y.search(x) for y in p_date]):  # Date accidently entered in time column
                            # C2 values were observed in 1 dataset
                            return pd.NaT
                        else:
                            raise ValueError("Expected HHMM format")

                    min = float(x[-2:])
                    if min > 59:
                        return pd.NaT
                    
                    if len(x) > 2:
                        hour = float(x[:-2])
                    else:
                        hour = 0

                    if hour > 23:
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
                    elif int(time_list[0].strip()) > 12:
                        # Typo where 24 hour time was provided but also PM indicated
                        hours_add = 0

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
    

def to_datetime(dates, ignore_errors=False, *args, **kwargs):
    coerce = 'errors' in kwargs and kwargs['errors']=='coerce'
    def to_datetime_local(x, *args, **kwargs):
        try:
            return pd.to_datetime(x, *args, **kwargs)
        except:
            return pd.NaT if coerce else x  # Coerce input to pd.to_datetime should return NaT if cannot convert to datetime

    if isinstance(dates, pd.DataFrame) and \
        (dates.isnull().any().any() or dates.apply(lambda x: isinstance(x,str)).any()):
        # This should have columns year, month, and day
        # Special code to handle nans in month or day which will be set to Periods instead of datetimes
        # or to handle cases where the day is an ordinal
        # This is year/month/date with NaNs in some of the values
        year_col = [x for x in dates.columns if x.lower()=='year'][0]
        month_col = [x for x in dates.columns if x.lower()=='month'][0]
        day_col = [x for x in dates.columns if x.lower()=='day'][0]
        def to_datetime_local(x):
            if pd.isnull(x[day_col]):
                if pd.isnull(x[month_col]):
                    if pd.isnull(x[year_col]):
                        return pd.NaT
                    else:
                        return pd.Period(freq='Y', year=x[year_col])
                else:
                    return pd.Period(freq='M', year=x[year_col], month=x[month_col])
            elif pd.isnull(x[month_col]):
                return ValueError(f"Month is null but day is not. Unable to parse {x}")
            elif pd.isnull(x[year_col]):
                return ValueError(f"Year is null but month and day are not. Unable to parse {x}")
            
            return pd.to_datetime(x.to_frame().T, *args, **kwargs).iloc[0]
        
        # Replace any ordinal days
        try:
            dates[day_col] = dates[day_col].apply(lambda x: x.strip()[:-2] if  isinstance(x,str) and re.search(r'\d+(th|st|nd|rd)',x) else x)
            return dates.apply(to_datetime_local, axis=1)
        except:
            # Adding this to put breakpoint here to catch errors. This try/except should be removed in the future
            raise

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, message="Could not infer format")
        try:
            return pd.to_datetime(dates, *args, **kwargs)
        except UnicodeEncodeError as e:
            if (ignore_errors or 'errors' in kwargs and kwargs['errors']=='coerce') and isinstance(dates, pd.Series):
                return dates.apply(to_datetime_local)
            else:
                return dates
        except (pd._libs.tslibs.parsing.DateParseError, dateutil.parser._parser.ParserError) as e:
            if 'out of range' in str(e) and \
                ((m1:=re.search(r'year ((19|20)\d{6}) is out of range: \1\.0.* position 0', str(e))) or \
                 re.search(r'year (\d{3,4}(19|20)\d{2}) is out of range: 0?\1\.0.* position 0', str(e))):
                if isinstance(dates, pd.Series):
                    return dates.apply(to_datetime)
                elif m1:
                    return pd.to_datetime(dates[:-2])
                else:
                    dates = dates[:-2]
                    year = dates[-4:]
                    mmdd = dates[:-4] if len(dates)==8 else '0'+dates[:-4]
                    return pd.to_datetime(year+mmdd)
            elif re.search(r'\d{2}/\d{2}/\d{4} \d{4} hours', str(e)) and isinstance(dates, pd.Series):
                return pd.to_datetime(dates, format='%m/%d/%Y %H%M hours')
            elif ignore_errors and 'out of range' in str(e) and not isinstance(dates, pd.Series):
                return dates
            elif ignore_errors and isinstance(dates,pd.Series) and \
                  (re.search(r'(Unknown string format|unable to parse): \d{4}\-__', str(e)) or \
                   re.search(r'(Unknown string format|unable to parse): \d{4}\-\d{2}\-__', str(e))):
                return dates.apply(lambda x: to_datetime(x, ignore_errors, *args, **kwargs))
            elif ignore_errors and isinstance(dates,str) and \
                ((m:=re.search(r'^(?P<year>\d{4})\-(?P<month>[\d\_]{2})\-\_\_', dates)) or \
                 (m:=re.search(r'^(?P<year>\d{4})\-(?P<month>\_\_)\-\d{2}', dates))):
                if m.groupdict()['month'].isdigit():
                    return pd.Period(freq='M', year=int(m.groupdict()['year']), month=int(m.groupdict()['month']))
                else:
                    return pd.Period(freq='Y', year=int(m.groupdict()['year']))
            elif ignore_errors and isinstance(dates, str) and re.search(r'Unknown(\sdatetime)? string format.+: \d+[-/]\d+[-/]\d+,?\s?\d+[-/]\d+[-/]\d+', str(e)):
                # Comma separated list of dates. Just return value
                return dates
            elif re.search(r'year \d\d20\d\d is out of range: \d{1,2}/\d\d20\d\d', str(e)) and isinstance(dates, pd.Series):
                dates = dates.apply(lambda x: re.sub(r'(\d\d)20(\d\d)', r'\1/20\2', x) if isinstance(x,str) else x)
                return to_datetime(dates, ignore_errors=ignore_errors, *args, **kwargs)
            else:
                raise
        except ValueError as e:
            if ignore_errors and "Given date string" in str(e) and "not likely a datetime" in str(e) and \
                (len(args)>0 or 'errors' not in kwargs or kwargs['errors']!='coerce'):
                new_kwargs = kwargs.copy()
                new_kwargs['errors'] = 'coerce'
                dts = pd.to_datetime(dates, *args, **new_kwargs)
                raise NotImplementedError()
            elif ignore_errors and isinstance(dates,pd.Series) and \
                    (re.search(r'time data \"\d{4}\-(\d\d|\_\_)\-\_\_\"',  str(e)) or \
                     re.search(r'time data \"\d{4}\-\_\_\-\d\d\"',  str(e))):
                def to_datetime_local(x):
                    if isinstance(x, str) and ((m:=re.search(r'^(?P<year>\d{4})\-(?P<month>[\d\_]{2})\-\_\_', x)) or \
                        (m:=re.search(r'^(?P<year>\d{4})\-(?P<month>[\_]{2})\-\d{2}', x))):
                        if m.groupdict()['month'].isdigit():
                            return pd.Period(freq='M', year=int(m.groupdict()['year']), month=int(m.groupdict()['month']))
                        else:
                            return pd.Period(freq='Y', year=int(m.groupdict()['year']))
                    else:
                        return pd.to_datetime(x, *args, **kwargs)
                    
                return dates.apply(to_datetime_local)
            elif 'doesn\'t match format "%Y-%m-%dT%H:%M:%S.%f%z"' in str(e):
                return pd.to_datetime(dates.apply(lambda x: x[:-1] if isinstance(x,str) and x[-1]=='Z' else x), format='mixed')
            elif ignore_errors and 'is out of range' in str(e) and is_str_number(dates):
                if (dec:=dates.find('.'))>0 and int(dates[dec+1:])==0:
                    dates = int(dates[:dec])
                    year_first = math.floor(dates/10000)
                    year_last = dates % 10000

                    is_year_first = 1980 <= year_first <= datetime.datetime.now().year
                    is_year_last =  1980 <= year_last <= datetime.datetime.now().year
                    if is_year_first + is_year_last != 1:
                        raise e
                    elif is_year_first:
                        year = year_first
                        month_day = dates - year*10000
                        month = math.floor(month_day/100)
                        day = month_day - month*100
                    else:
                        year = year_last
                        month_day = year_first
                        month = math.floor(month_day/100)
                        day = month_day - month*100
                    if month>12 or day>31:
                        raise e
                    return pd.to_datetime(datetime.datetime(year=year, month=month, day=day))
                else:
                    raise e
            elif ignore_errors and re.search(r'Unknown string format: \d+[-/]\d+[-/]\d+,?\s?\d+[-/]\d+[-/]\d+', str(e)):
                # Comma separated list of dates. Just return value
                return dates
            elif ignore_errors and re.search(r'time\s+data\s+\"\d{1,2}/\d\d20\d\d\"', str(e)) and isinstance(dates, pd.Series):
                dates = dates.apply(lambda x: re.sub(r'(\d\d)20(\d\d)', r'\1/20\2', x) if isinstance(x,str) else x)
                return to_datetime(dates, ignore_errors=ignore_errors, *args, **kwargs)
            elif isinstance(dates, pd.DataFrame) and dates.shape[1]==3 and (str(e)=='cannot convert NA to integer' or \
                    (re.search(r'Unable to parse string \"\d+(th|st|nd|rd)\"', str(e)) and \
                    dates.iloc[:,2][dates.iloc[:,2].notnull()].apply(lambda x: isinstance(x, numbers.Number) or is_str_number(x) or \
                    re.search(r'\d+(th|st|nd|rd)',x) is not None).all())):
                raise NotImplementedError("This should no longer be reachable. Remove in a future release")
            else:
                return pd.to_datetime(dates, *args, format='mixed', **kwargs)
        except Exception as e:
            raise
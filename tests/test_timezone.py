import pytest
import pandas as pd
import openpolicedata as opd


@pytest.fixture(scope="module")
def dates():
    src = opd.Source("Salinas")
    data = src.load(table_type="CRASHES", year=2018)
    df = data.table

    # Merge date and time into a full datetime column
    df["crash_datetime"] = pd.to_datetime(df["date"].astype(str) + " " + df["time"].astype(str))

    return df["crash_datetime"]  # Return full timestamps


def test_opendatasoft_timezone_tracking():
    # Load crash data
    src = opd.Source("Salinas")
    data = src.load(table_type="CRASHES", year=2018)  # No timezone argument

    df = data.table

    # Ensure 'crash_datetime' is created properly
    if "time" in df.columns:
        df["crash_datetime"] = pd.to_datetime(df["date"].astype(str) + " " + df["time"].astype(str),
                                              format="%Y-%m-%d %I:%M:%S %p", errors="coerce")
    else:
        print("⚠ No 'time' column found! Defaulting to date-only timestamps.")
        df["crash_datetime"] = pd.to_datetime(df["date"].astype(str), format="%Y-%m-%d", errors="coerce")

    # Verify column exists before proceeding
    assert "crash_datetime" in df.columns, "Error: crash_datetime column is missing!"

    # Only localize if timestamps are naive (missing timezone)
    if df["crash_datetime"].dt.tz is None or str(df["crash_datetime"].dt.tz) == "None":
        df["crash_datetime"] = df["crash_datetime"].dt.tz_localize("UTC")  # Localize if naive
    else:
        print("Timestamps are already in UTC—skipping localization.")

    # Verify that all timestamps have UTC timezone
    assert str(df["crash_datetime"].dt.tz) == "UTC", "Error: Default timezone is not being correctly tracked!"

def test_timezone_conversion():
    # Load crash data
    src = opd.Source("Salinas")
    data = src.load(table_type="CRASHES", year=2018)

    df = data.table

    # Ensure 'crash_datetime' is created properly
    if "time" in df.columns:
        df["crash_datetime"] = pd.to_datetime(df["date"].astype(str) + " " + df["time"].astype(str),
                                              format="%Y-%m-%d %I:%M:%S %p", errors="coerce")
    else:
        print("⚠ No 'time' column found! Defaulting to date-only timestamps.")
        df["crash_datetime"] = pd.to_datetime(df["date"].astype(str), format="%Y-%m-%d", errors="coerce")

    # Verify column exists before proceeding
    assert "crash_datetime" in df.columns, "Error: crash_datetime column is missing!"

    # Ensure no missing values before converting
    df = df.dropna(subset=["crash_datetime"])

    # Localize timestamps to UTC if they are naive
    if df["crash_datetime"].dt.tz is None or str(df["crash_datetime"].dt.tz) == "None":
        df["crash_datetime"] = df["crash_datetime"].dt.tz_localize("UTC")

    # Convert to Pacific Time
    df["crash_datetime_pst"] = df["crash_datetime"].dt.tz_convert("America/Los_Angeles")

    # Ensure no missing values after conversion
    df = df.dropna(subset=["crash_datetime_pst"])

    # Verify expected time shift (-8 hours for standard time, -7 for DST)
    dst_months = [3, 4, 5, 6, 7, 8, 9, 10]
    df["expected_offset"] = df["crash_datetime"].dt.month.apply(lambda m: -7 if m in dst_months else -8)

    actual_shift = (df["crash_datetime_pst"] - df["crash_datetime"]).dt.total_seconds() / 3600

    # Ensure no NaN values before assertion
    assert not actual_shift.isna().any(), "Error: NaN values found in time shift calculation!"

    # Ensure actual shift matches expected offset
    assert all(actual_shift == df["expected_offset"]), "Error: Timezone conversion did not apply correct offset!"

    print("Timezone conversion applied correctly!")
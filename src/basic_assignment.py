import gspread

import pandas as pd
import numpy as np
import yaml

from pathlib import Path


def load_yaml_config():
    """
    Loads the config yaml file
    """

    file_name = Path(__file__).parent / "../config/parameters.yml"
    with file_name.open() as params_file:
        params = yaml.full_load(params_file)

    return params


def add_availability_columns(observers_df):
    """
    Adds availability columns to observers dataframe
    """

    observers_df["inside_all_day"] = observers_df["election_day"].str.contains(
        "ALL DAY - INSIDE"
    )
    observers_df["outside_AM"] = observers_df["election_day"].str.contains("OUTSIDE AM")
    observers_df["outside_PM"] = observers_df["election_day"].str.contains("OUTSIDE PM")
    observers_df["outside_allday"] = (
        observers_df["outside_AM"] & observers_df["outside_PM"]
    )

    return observers_df


def clean_observer_df(observers_df):
    """
    Cleans and formats observers dataframe
    """

    valid_post_codes = load_yaml_config()["valid_post_codes"]

    # clean phone number
    observers_df["phone_number"] = (
        observers_df["phone_number"].str.replace("-", "").replace(" ", "")
    )

    # drop totally missing rows
    observers_df = observers_df.loc[
        ~observers_df[["date_entered", "name", "phone_number"]].isna().all(axis=1)
    ]

    # clean post-codes - keep to first 5 and cast as int
    observers_df["post_code"] = (
        observers_df["post_code"].astype(str).apply(lambda x: int(x.split("-")[0]))
    )
    observers_df = observers_df.loc[observers_df.post_code.isin(valid_post_codes)]

    # drop duplicates
    observers_df = observers_df.sort_values("date_entered")
    observers_df = observers_df.drop_duplicates(["name", "phone_number"], keep="last")

    # map legal background as boolean
    observers_df["legal_background"] = observers_df["legal_background"] == "Yes"

    return observers_df


def get_observer_dataset():
    """
    Loads the google sheets observer forms and returns a dataframe with
    important columns. Adds additional columns and cleans data
    """

    gc = gspread.oauth()
    config = load_yaml_config()
    params = config["columns_map"]

    sh = gc.open(config["observer_google_sheet"])

    required_length = sh.sheet1.row_count

    all_columns = {
        "assigned_am": np.nan,
        "assigned_pm": np.nan,
    }

    for column_name, column_params in params.items():
        column_data = sh.sheet1.col_values(column_params["col_num"])[1:]
        column_data += [column_params["fill_missing"]] * (
            required_length - len(column_data)
        )

        all_columns[column_name] = column_data

    observer_df = pd.DataFrame(all_columns)
    observer_df = add_availability_columns(observer_df)
    observer_df = clean_observer_df(observer_df)
    observer_df = observer_df.sort_values("outside_allday", ascending=False)

    return observer_df


def get_precinct_dataset():
    """
    Load precinct excel sheet. Note that it much define "Priority" column
    """

    precinct = pd.read_excel(
        Path(__file__).parent / "../data/00_raw/PollingPlaceDetails.xls"
    )
    precinct = precinct.sort_values("Priority")
    return precinct


def get_available_observers(observers_df, n_required, location, need_legal_background):
    """
    Get available observers that can be assigned to precincts

    Parameters
    ----------
    observer_df: pd.DataFrame
        The observers dataframe
    n_required: int
        The number of observers required. This is the maximum that will be returned. If
        there are few that these available, it will be padded with np.nan
    location: string
        Must be one of "inside_all_day", "outside_AM", "outside_PM"
    need_legal_background: bool
        If observer must have legal expertise

    Returns
    -------
    available_names: np.array
        An array of available observer names to assign to precincts

    Note
    ----
    SIDE EFFECT ALERT: Once observers are returned, they are also marked in the
    observers_df as no longer being free.

    #TODO: Fix this side effect
    """

    if "AM" in location:
        assignment_cols = ["assigned_am"]
    elif "PM" in location:
        assignment_cols = ["assigned_pm"]
    else:
        assignment_cols = ["assigned_pm", "assigned_am"]

    assigned = observers_df[assignment_cols].isna().all(axis=1)

    available_mask = (
        (observers_df[location])
        & (observers_df["legal_background"] == need_legal_background)
        & assigned
    )

    available_names = observers_df[available_mask]["name"].values
    observers_df.loc[available_mask, assignment_cols] = True

    if len(available_names) < n_required:
        available_names = np.pad(
            available_names,
            (0, n_required - len(available_names)),
            constant_values=np.nan,
        )
    elif len(available_names) > n_required:
        available_names = available_names[:n_required]

    return available_names


def assign_observers(precinct, observers, location, is_attorney, params=None):
    """
    Assign free observers to precincts.

    Parameters
    ----------
    precinct: pd.DataFrame
        The precinct DataFrame
    observers: pd.DataFrame
        The observets DataFrame
    location: string
        one of "inside", "outside_am", "outside_pm"
    is_attorney: bool
        If available observer should be an attorney
    params: dict, optional
        Dictionary of parameters

    Returns
    -------
    precinct: pd.DataFrame
        The updated precinct DataFrame
    observers: pd.DataFrame
        The updated observers DataFrame

    Note
    ----
    You don't really need the returned values since the original input
    dataframes are updated.

    #TODO: Should fix this side effect
    """

    if params is None:
        params = load_yaml_config()[location]

    missing_observer = precinct[params["precinct_observer"]].isna()
    precinct.loc[
        missing_observer, params["precinct_observer"]
    ] = get_available_observers(
        observers, missing_observer.sum(), params["observer_availability"], is_attorney,
    )

    precinct.loc[
        missing_observer & ~precinct[params["precinct_observer"]].isna(),
        params["precinct_is_legal"],
    ] = is_attorney
    return precinct, observers


def run_ordered_assignment(precinct, observers):
    """
    Assign observers as per priority and availability. Returns the same
    datasets as input just with updated assignments

    Parameters
    ---------
    precinct: pd.DataFrame
        The precinct data
    observers: pd.DataFrame
        The observers data

    Returns
    -------
    precinct: pd.DataFrame
    observers: pd.DataFrame

    """
    assign_observers(precinct, observers, "inside", True)
    assign_observers(precinct, observers, "outside_am", True)
    assign_observers(precinct, observers, "outside_pm", True)
    assign_observers(precinct, observers, "inside", False)
    assign_observers(precinct, observers, "outside_am", False)
    assign_observers(precinct, observers, "outside_pm", False)

    return precinct, observers


if __name__ == "__main__":

    observers = get_observer_dataset()
    precinct = get_precinct_dataset()

    run_ordered_assignment(precinct, observers)

    precinct.to_excel(
        Path(__file__).parent / "../data/01_output/assigned_precincts.xlsx",
        index=False,
        encoding="utf-8",
    )

    print(precinct)

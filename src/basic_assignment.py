import gspread
import pandas as pd
import numpy as np
import yaml
import re

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

    observers_df["inside_all_day"] = observers_df["election_day"] == "Inside"

    observers_df["outside_AM"] = observers_df["election_day"].isin(
        ["Outside AM", "Outside All Day"]
    )
    observers_df["outside_PM"] = observers_df["election_day"].isin(
        ["Outside PM", "Outside All Day"]
    )
    observers_df["outside_all_day"] = observers_df["election_day"] == "Outside All Day"

    return observers_df


def clean_observer_df(observers_df):
    """
    Cleans and formats observers dataframe
    """

    valid_post_codes = load_yaml_config()["valid_post_codes"]

    # clean phone number
    observers_df["phone_number"] = (
        observers_df["phone_number"].apply(lambda x: re.sub("[^0-9]", "", x))
        # observers_df["phone_number"].str.replace("-", "").replace(" ", "")
    )

    # clean name by removing spaces at the end
    observers_df["name"] = observers_df["name"].str.strip()

    # clean email address
    observers_df["email"] = observers_df["email"].str.lower()

    # drop totally missing rows
    observers_df = observers_df.loc[~observers_df[["name"]].isna().all(axis=1)]

    # drop rovers
    observers_df = observers_df.loc[~(observers_df.is_rover == "1")].drop(
        "is_rover", axis=1
    )

    # clean post-codes - keep to first 5 and cast as int
    observers_df["post_code"] = (
        observers_df["post_code"].astype(str).apply(lambda x: int(x.split("-")[0]))
    )
    observers_df["from_county"] = False
    observers_df.loc[
        observers_df.post_code.isin(valid_post_codes), "from_county"
    ] = True

    # drop duplicates
    observers_df = observers_df.sort_values("date_entered")
    observers_df = observers_df.drop_duplicates(["name"], keep="last")
    observers_df = observers_df.drop_duplicates(["email"], keep="last")

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
    observer_df = observer_df.sort_values(
        ["ev_2020_experience", "outside_all_day"], ascending=False
    )

    return observer_df


def get_precinct_dataset():
    """
    Load precinct excel sheet. Note that it much define "Priority" column
    """

    precinct = pd.read_excel(
        Path(__file__).parent / "../data/00_raw/PollingPlaceDetails.xls"
    )
    precinct = precinct.sort_values("Priority")
    precinct = precinct.fillna("")
    return precinct


def get_available_observers(
    observers_df, n_required, location, need_legal_background, need_from_county
):
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
        Must be one of "inside_all_day", "outside_AM", "outside_PM", "outside_all_day"
    need_legal_background: bool
        If observer must have legal expertise
    need_from_county: bool
        If observer must be from the county

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

    if location == "outside_AM":
        assignment_cols = ["assigned_am"]
    elif location == "outside_PM":
        assignment_cols = ["assigned_pm"]
    else:
        assignment_cols = ["assigned_pm", "assigned_am"]

    assigned = observers_df[assignment_cols].isna().all(axis=1)

    available_mask = (
        (observers_df[location])
        & (observers_df["legal_background"] == need_legal_background)
        & assigned
    )

    if need_from_county:
        available_mask = available_mask & (observers_df["from_county"])

    available_names = observers_df[available_mask]["name"].values
    observers_df.loc[available_mask, assignment_cols] = True

    if len(available_names) < n_required:
        available_names = np.pad(
            available_names, (0, n_required - len(available_names)), constant_values="",
        )
    elif len(available_names) > n_required:
        available_names = available_names[:n_required]

    if (location == "outside_all_day") and (len(available_names) > 0):
        print(location, len(available_names), n_required)
        available_names = (
            np.repeat(available_names, 2).reshape(n_required, -1).squeeze()
        )
        print(available_names.shape)
    return available_names


def assign_observers(precinct, observers, location, is_attorney, params=None):
    """
    Assign free observers to precincts.

    Parameters
    ----------
    precinct: pd.DataFrame
        The precinct DataFrame
    observers: pd.DataFrame
        The observers DataFrame
    location: string
        one of "inside", "outside_am", "outside_pm", "outside_both"
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

    from_county = params["from_county"]
    missing_observer = (precinct[params["precinct_observer"]] == "").all(axis=1)
    print(
        location,
        is_attorney,
        "PRECINCT SHAPE: ",
        precinct.loc[missing_observer, params["precinct_observer"]].shape,
    )
    precinct.loc[
        missing_observer, params["precinct_observer"]
    ] = get_available_observers(
        observers,
        missing_observer.sum(),
        params["observer_availability"],
        is_attorney,
        from_county,
    )

    precinct.loc[missing_observer, params["precinct_is_legal"]] = is_attorney

    observers_allocated = observers.merge(
        precinct[[params["precinct_observer"][0], "Polling Place Name"]],
        left_on="name",
        right_on=params["precinct_observer"][0],
        how="left",
    )
    observers[params["observer_loc"]] = observers_allocated["Polling Place Name"].values

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
    assign_observers(precinct, observers, "outside_both", True)
    assign_observers(precinct, observers, "outside_am", True)
    assign_observers(precinct, observers, "outside_pm", True)
    assign_observers(precinct, observers, "inside", False)
    assign_observers(precinct, observers, "outside_both", False)
    assign_observers(precinct, observers, "outside_am", False)
    assign_observers(precinct, observers, "outside_pm", False)

    return precinct, observers


def output_by_shift(precinct, observers, rename_dict, params):
    """
    """

    outside_pm_df = precinct.merge(
        observers, left_on=params["observer_col"], right_on="name", how="left",
    )

    outside_pm_df.rename(
        columns=rename_dict, inplace=True,
    )

    assert outside_pm_df.shape[0] == precinct.shape[0], "Mismatch in number of records"

    outside_pm_df["County"] = params["county"]
    outside_pm_df["Date"] = params["date"]
    outside_pm_df["Start Time"] = params["start_time"]
    outside_pm_df["End Time"] = params["end_time"]
    outside_pm_df["Area"] = params["area"]

    return outside_pm_df[
        [
            "County",
            "Rank",
            "LocationName",
            "Date",
            "Start Time",
            "End Time",
            "Area",
            "Name",
            "Phone Number",
            "Email Address",
        ]
    ]


def get_lbj_csv(precinct, observers):
    """
    """

    precinct_cols = [
        "Priority",
        "Polling Place Name",
        "inside_observer",
        "outside_am_observer",
        "outside_pm_observer",
    ]

    observer_cols = ["name", "phone_number", "email"]

    output_df = pd.DataFrame()
    for shift in [
        "outside_am_output",
        "outside_pm_output",
        "inside_am_output",
        "inside_pm_output",
    ]:
        params = load_yaml_config()
        output_df = output_df.append(
            output_by_shift(
                precinct[precinct_cols],
                observers[observer_cols],
                params["rename_columns"],
                params[shift],
            )
        )

    return output_df


if __name__ == "__main__":

    observers = get_observer_dataset()
    precinct = get_precinct_dataset()

    run_ordered_assignment(precinct, observers)

    precinct.to_excel(
        Path(__file__).parent / "../data/01_output/assigned_precincts.xlsx",
        index=False,
        encoding="utf-8",
    )

    observers.to_excel(
        Path(__file__).parent / "../data/01_output/assigned_observers.xlsx",
        index=False,
        encoding="utf-8",
    )

    lbj_output = get_lbj_csv(precinct, observers)

    lbj_output.to_excel(
        Path(__file__).parent / "../data/01_output/lbj_output.xlsx",
        index=False,
        encoding="utf-8",
    )

    print(lbj_output)

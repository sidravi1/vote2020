import gspread

import pandas as pd
import numpy as np
import yaml

from pathlib import Path


def load_yaml_config():

    file_name = Path(__file__).parent / "../config/parameters.yml"
    with file_name.open() as params_file:
        params = yaml.full_load(params_file)

    return params


def add_availability_columns(df):

    df["inside_all_day"] = df["election_day"].str.contains("ALL DAY - INSIDE")
    df["outside_AM"] = df["election_day"].str.contains("OUTSIDE AM")
    df["outside_PM"] = df["election_day"].str.contains("OUTSIDE PM")
    df["outside_allday"] = df["outside_AM"] & df["outside_PM"]

    return df


def clean_observer_df(df):

    # clean phone number
    df["phone_number"] = df["phone_number"].str.replace("-", "").replace(" ", "")

    # drop duplicates
    df = df.sort_values("date_entered")
    df = df.drop_duplicates(["name", "phone_number"], keep="last")

    return df


def get_observer_dataset():

    gc = gspread.oauth()
    sh = gc.open("R5-Wake-Poll Observer Google Form (Responses)")

    legal_background = sh.sheet1.col_values(25)[1:]
    election_day = sh.sheet1.col_values(24)[1:]
    name = sh.sheet1.col_values(3)[1:]
    phone_number = sh.sheet1.col_values(4)[1:]
    date_entered = sh.sheet1.col_values(1)[1:]

    max_length = max([len(legal_background), len(election_day)])

    legal_background += ["No"] * (max_length - len(legal_background))
    legal_background = [(lb == "Yes") for lb in legal_background]
    election_day += ["None"] * (max_length - len(election_day))

    df = pd.DataFrame(
        {
            "name": name,
            "phone_number": phone_number,
            "date_entered": date_entered,
            "legal_background": legal_background,
            "election_day": election_day,
            "assigned_am": np.nan,
            "assigned_pm": np.nan,
        }
    )

    df = add_availability_columns(df)
    df = clean_observer_df(df)

    df = df.sort_values("outside_allday", ascending=False)
    return df


def get_precinct_dataset():

    df = pd.read_excel("../data/PollingPlaceDetails.xls")
    df = df.sort_values("Priority")
    return df


def get_available_observers(observers_df, n_required, location, need_legal_background):

    # TODO : Fix assignment when working morning and evening

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


if __name__ == "__main__":

    params = load_yaml_config()
    observers = get_observer_dataset()
    precinct = get_precinct_dataset()

    # assign inside attorneys

    assign_observers(precinct, observers, "inside", True)
    assign_observers(precinct, observers, "outside_am", True)
    assign_observers(precinct, observers, "outside_pm", True)
    assign_observers(precinct, observers, "inside", False)
    assign_observers(precinct, observers, "outside_am", False)
    assign_observers(precinct, observers, "outside_pm", False)

    precinct.to_excel(
        Path(__file__).parent / "../data/01_output/assigned_precincts.xlsx",
        index=False,
        encoding="utf-8",
    )
    print(precinct)

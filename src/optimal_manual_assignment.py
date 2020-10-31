import src.basic_assignment as ba
import pandas as pd

from pathlib import Path
from src.optimal_assignment import optimise_assignment


def get_manual_precinct_allocation():
    """
    """

    precinct = pd.read_excel(
        Path(__file__).parent
        / "../data/02_optimisation_input/assigned_precincts_reassigned_lawyerup.xlsx"
    )

    return precinct


if __name__ == "__main__":

    observers = ba.get_observer_dataset()
    precinct = get_manual_precinct_allocation().fillna("")

    # inside legal
    mask = precinct["inside_legal"] & (precinct["inside_observer"] != "")

    precinct_subset = precinct[mask]
    precinct.loc[mask, "inside_observer"] = optimise_assignment(
        precinct_subset, observers, "inside_observer"
    )

    # inside not-legal
    mask = (~precinct["inside_legal"]) & (precinct["inside_observer"] != "")

    precinct_subset = precinct[mask]
    precinct.loc[mask, "inside_observer"] = optimise_assignment(
        precinct_subset, observers, "inside_observer"
    )

    # outside legal all day
    mask = (
        (precinct["outside_am_legal"])
        & (precinct["outside_am_observer"] == precinct["outside_pm_observer"])
        & (precinct["outside_am_observer"] != "")
    )

    precinct_subset = precinct[mask]
    optimised_observer_list = optimise_assignment(
        precinct_subset, observers, "outside_am_observer"
    )
    precinct.loc[mask, "outside_am_observer"] = optimised_observer_list
    precinct.loc[mask, "outside_pm_observer"] = optimised_observer_list

    # outside am only legal
    mask = (
        (precinct["outside_am_legal"])
        & (precinct["outside_am_observer"] != precinct["outside_pm_observer"])
        & (precinct["outside_am_observer"] != "")
    )

    precinct_subset = precinct[mask]
    optimised_observer_list = optimise_assignment(
        precinct_subset, observers, "outside_am_observer"
    )
    precinct.loc[mask, "outside_am_observer"] = optimised_observer_list

    # outside pm only legal
    mask = (
        (precinct["outside_pm_legal"])
        & (precinct["outside_am_observer"] != precinct["outside_pm_observer"])
        & (precinct["outside_pm_observer"] != "")
    )

    precinct_subset = precinct[mask]
    optimised_observer_list = optimise_assignment(
        precinct_subset, observers, "outside_pm_observer"
    )
    precinct.loc[mask, "outside_pm_observer"] = optimised_observer_list

    # outside not-legal all day
    mask = (
        (~precinct["outside_am_legal"])
        & (precinct["outside_am_observer"] == precinct["outside_pm_observer"])
        & (precinct["outside_am_observer"] != "")
    )
    precinct_subset = precinct[mask]
    optimised_observer_list = optimise_assignment(
        precinct_subset, observers, "outside_am_observer"
    )
    precinct.loc[mask, "outside_am_observer"] = optimised_observer_list
    precinct.loc[mask, "outside_pm_observer"] = optimised_observer_list

    # outside am only not-legal
    mask = (
        (~precinct["outside_am_legal"])
        & (precinct["outside_am_observer"] != precinct["outside_pm_observer"])
        & (precinct["outside_am_observer"] != "")
    )
    precinct_subset = precinct[mask]
    optimised_observer_list = optimise_assignment(
        precinct_subset, observers, "outside_am_observer"
    )
    precinct.loc[mask, "outside_am_observer"] = optimised_observer_list

    # Outside pm only not-legal
    mask = (
        (~precinct["outside_pm_legal"])
        & (precinct["outside_am_observer"] != precinct["outside_pm_observer"])
        & (precinct["outside_pm_observer"] != "")
    )
    precinct_subset = precinct[mask]
    optimised_observer_list = optimise_assignment(
        precinct_subset, observers, "outside_pm_observer"
    )
    precinct.loc[mask, "outside_pm_observer"] = optimised_observer_list

    observers_allocated = observers.merge(
        precinct[["inside_observer", "Polling Place Name"]],
        left_on="name",
        right_on="inside_observer",
        how="left",
    )
    observers["inside_location"] = observers_allocated["Polling Place Name"].values

    observers_allocated = observers.merge(
        precinct[["outside_am_observer", "Polling Place Name"]],
        left_on="name",
        right_on="outside_am_observer",
        how="left",
    )
    observers["outside_am_location"] = observers_allocated["Polling Place Name"].values

    observers_allocated = observers.merge(
        precinct[["outside_pm_observer", "Polling Place Name"]],
        left_on="name",
        right_on="outside_pm_observer",
        how="left",
    )
    observers["outside_pm_location"] = observers_allocated["Polling Place Name"].values

    precinct.to_excel(
        Path(__file__).parent
        / "../data/01_output/manual_optimised_assigned_precincts.xlsx",
        index=False,
        encoding="utf-8",
    )
    observers.to_excel(
        Path(__file__).parent
        / "../data/01_output/manual_optimised_assigned_observers.xlsx",
        index=False,
        encoding="utf-8",
    )

    lbj_output = ba.get_lbj_csv(precinct, observers)

    lbj_output.to_excel(
        Path(__file__).parent / "../data/01_output/lbj_output_manual.xlsx",
        index=False,
        encoding="utf-8",
    )

    print(lbj_output)

import pandas as pd
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt

import src.basic_assignment as ba

from pathlib import Path


class PreferenceNetwork:
    """
    This class is a wrapper around a nx.DiGraph() and allows
    for easy setup and projections of the bipartite graph
    """

    def __init__(self, preference_edges, ownership_edges):

        G = nx.DiGraph()

        for node in ownership_edges["Polling Place Name"]:
            G.add_node(node, node_type="pollstation", node_color="darkred")
        for node in ownership_edges.iloc[:, 1]:
            G.add_node(node, node_type="observer", node_color="dodgerblue")
        for node in preference_edges["preference"]:
            G.add_node(node, node_type="pollstation", node_color="darkred")
        for node in preference_edges["observer"]:
            G.add_node(node, node_type="observer", node_color="dodgerblue")

        G.add_edges_from(ownership_edges.itertuples(index=False, name=None))
        G.add_edges_from(preference_edges.itertuples(index=False, name=None))

        self.G = G

    def draw(self):
        """
        Returns a matplotliDb plot of the graph
        """
        f, ax = plt.subplots(figsize=(10, 7))

        pos = nx.spring_layout(self.G)
        nx.draw_networkx_edges(self.G, pos, alpha=0.2)
        nx.draw_networkx_nodes(
            self.G,
            pos,
            node_color=nx.get_node_attributes(self.G, "node_color").values(),
            node_size=50,
        )

        return f

    def __repr__(self):
        """
        Calls the networkx.DiGraph __repr__ method
        """
        return self.G.__repr__()

    def get_projection(self, node_type="observer"):
        """
        Get projection of bipartite network for `node_type`
        """

        obs_nodes = [
            x for x, y in self.G.nodes(data=True) if y["node_type"] == node_type
        ]
        G_proj = nx.bipartite.projected_graph(self.G, obs_nodes)

        return G_proj

    def get_projection_adj(self, node_type="observer"):
        """
        Get the adjacency matrix for the projection of the bipartite network
        """
        G_proj = self.get_projection(node_type)
        adjacency_matrix = pd.DataFrame(G_proj.adjacency(), columns=["from", "to"])
        adjacency_matrix["to"] = adjacency_matrix["to"].apply(lambda x: list(x.keys()))

        return adjacency_matrix


def get_zipcode_distance(zip1, zip2):
    """
    Just looks at the difference between zipcodes.
    Fancier version may use an api to get actual distance.
    """

    return np.abs(int(zip1) - int(zip2))


def resolve_cycle(pref_network, verbose=False):
    """
    Assign preferences as per the cycle

    Parameters
    ----------
    pref_network: PreferenceNetwork
        A PreferenceNetwork object
    verbose: bool, optional
        If debug statements should be printed. False by default

    Returns
    -------
    preferences: dict
        Matched preferences
    """

    G_proj = pref_network.get_projection()
    adj_df = pref_network.get_projection_adj()

    # find self cycles
    self_mask = adj_df["to"].apply(len) == 0
    print(adj_df)
    preferences = {}
    if self_mask.any():
        observers_matched = adj_df.loc[self_mask, "from"]
        if verbose:
            print("Self: ", list(observers_matched))
        preferences.update(dict(pref_network.G.out_edges(list(observers_matched))))
    else:
        try:
            for observer, _ in nx.find_cycle(G_proj):
                if verbose:
                    print("Cycle : ", observer)
                preferences.update(dict(pref_network.G.out_edges([observer])))
        except nx.NetworkXNoCycle:
            pass

    return preferences


def get_matched_sets(distance_df, merged_df, column_to_optimise, verbose=False):
    """
    Iterate through the nodes and their preferences, looking for cycles and resolving
    these. See https://en.wikipedia.org/wiki/Top_trading_cycle

    Parameters
    ----------
    distance_df: pd.DataFrame
        A dataframe of distances between polling locations (cols) and observers (rows)
    merged_df: pd.DataFrame
        A dataframe with polling locations with post code of observer added
    column_to_optimise: string
        Specifies the observer columns that needs to be optimised
        Must be one of 'inside_observer', 'outside_am_observer', 'outside_pm_observer'
    verbose: bool, optional
        If debug data should be printed to screen

    Returns
    -------
    matched_set: dict
        The optimised match list.
        A dictionary with keys as observers and values as polling locations

    """

    matched_set = {}
    while len(distance_df) > 0:
        if verbose:
            print(" >>>>>> ", len(distance_df))
        preference = np.argmin(distance_df.values, axis=1)
        preference_edges = pd.DataFrame(
            {
                "observer": distance_df.index,
                "preference": distance_df.columns[preference],
            }
        )
        ownership_edges = merged_df.loc[
            merged_df["Polling Place Name"].isin(distance_df.columns[preference]),
            ["Polling Place Name", column_to_optimise],
        ]
        print(preference_edges, ownership_edges)
        PN = PreferenceNetwork(preference_edges, ownership_edges)
        matched_pairs = resolve_cycle(PN, verbose)
        matched_set.update(matched_pairs)

        distance_df = distance_df.drop(matched_pairs.keys())
        distance_df = distance_df.drop(matched_pairs.values(), axis=1)
        print(matched_set)
    return matched_set


def optimise_assignment(precinct, observers, column_to_optimise):
    """
    Creates a distance matrix and runs the top-trading algorithm.

    Parameters
    -----------
    precinct: pd.DataFrame
        The dataframe of polling station precincts to optimise
    observers: pd.DataFrame
        The list of all observers
    column_to_optimise: string
        Specifies the observer columns that needs to be optimised
        Must be one of 'inside_observer', 'outside_am_observer', 'outside_pm_observer'

    Returns
    -------
    np.array
        The ordered list of observers for `column_to_optimise` for
        the precincts provided

    """
    if len(precinct) == 0:
        return pd.Series([], name=column_to_optimise)

    merged_df = precinct.merge(
        observers[["name", "post_code"]], left_on=column_to_optimise, right_on="name"
    )
    precinct_list = merged_df[["Polling Place Name", "Zip"]]
    observer_list = merged_df[[column_to_optimise, "post_code"]]
    distance = np.abs(
        precinct_list.Zip.values[np.newaxis, :]
        - observer_list.post_code.values[:, np.newaxis]
    )
    distance_df = pd.DataFrame(
        distance,
        index=observer_list[column_to_optimise],
        columns=precinct_list["Polling Place Name"],
    )

    merged_df["current_distance"] = np.abs(merged_df["Zip"] - merged_df["post_code"])

    matched_set = get_matched_sets(distance_df, merged_df, column_to_optimise, True)

    df = pd.DataFrame([matched_set], index=["pollingstation"]).T.reset_index()
    df.columns = [column_to_optimise, "pollingstation"]
    precinct[["Pct", "Polling Place Name"]].merge(
        df, left_on="Polling Place Name", right_on="pollingstation"
    ).drop("pollingstation", axis=1)

    return df[column_to_optimise].values


if __name__ == "__main__":

    observers = ba.get_observer_dataset()
    precinct = ba.get_precinct_dataset()
    precinct, observers = ba.run_ordered_assignment(precinct, observers)

    # Inside legal

    precinct_subset = precinct[
        precinct["inside_legal"] & (precinct["inside_observer"] != "")
    ]
    precinct.loc[
        (precinct["inside_legal"]) & (precinct["inside_observer"] != ""),
        "inside_observer",
    ] = optimise_assignment(precinct_subset, observers, "inside_observer")

    # Outside both legal

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

    # Outside am only legal

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

    # Outside pm only legal

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

    # Inside non-legal

    precinct_subset = precinct[
        (~precinct["inside_legal"]) & (precinct["inside_observer"] != "")
    ]

    precinct.loc[
        (~precinct["inside_legal"]) & (precinct["inside_observer"] != ""),
        "inside_observer",
    ] = optimise_assignment(precinct_subset, observers, "inside_observer")

    # Outside both not-legal

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

    # Outside am only legal

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

    # Outside pm only legal

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
        Path(__file__).parent / "../data/01_output/optimised_assigned_precincts.xlsx",
        index=False,
        encoding="utf-8",
    )
    observers.to_excel(
        Path(__file__).parent / "../data/01_output/optimised_assigned_observers.xlsx",
        index=False,
        encoding="utf-8",
    )

    print(precinct)

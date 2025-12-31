"""
Defines useful helper functions which create nice figures
using Matplotlib.

All functions written here receive a Matplotlib Axes instance as an argument
and plot on that axis. They do not interact with Matplotlib Figure instances.
Thus, the user must instantiate all subplots and close all figures
separately.
"""

import pandas as pd


def add_column_numbers(table: pd.DataFrame):
    df = table.copy()
    arrays = [df.columns.get_level_values(n) for n in range(df.columns.nlevels)] + [
        [f"({column_number})" for column_number in range(1, 1 + len(df.columns))]
    ]
    df.columns = pd.MultiIndex.from_arrays(arrays)

    return df

from datetime import datetime, timedelta
from random import randrange

import pandas as pd
from nemreader import output_as_data_frames

from nemwriter import NEM12


def test_dataframe_export():
    """Create export from dataframe"""
    num_intervals = 288
    index = [
        datetime(2004, 4, 1) + timedelta(minutes=5 * x)
        for x in range(1, num_intervals + 1)
    ]
    e1 = [randrange(1, 10) for x in range(1, num_intervals + 1)]
    e2 = [randrange(1, 5) for x in range(1, num_intervals + 1)]
    s1 = pd.Series(data=e1, index=index, name="E1")
    s2 = pd.Series(data=e2, index=index, name="E2")
    df = pd.concat([s1, s2], axis=1)

    nmi = "A123"
    m = NEM12(to_participant=nmi)
    m.add_dataframe(nmi=nmi, df=df, uoms={"E1": "kWh", "E2": "kWh"})
    output_file = f"tests/{m.nem_filename()}.zip"
    fp = m.output_zip(file_path=output_file)
    assert fp.name == f"{m.nem_filename()}.zip"

    dfs = output_as_data_frames(output_file)
    nmi2, df2 = dfs[0]  # Return data for first NMI in file
    assert nmi == nmi2
    assert [float(x) for x in df["E1"]] == [float(x) for x in df2["E1"]]
    per1 = [x.strftime("%Y%m%d%H%M") for x in df.index]
    per2 = [x.strftime("%Y%m%d%H%M") for x in df2["t_end"]]
    assert per1 == per2


def test_df_export_missing_intervals():
    """Create export from dataframe with missing intervals"""
    interval = 30
    num_intervals = int(60 / interval) * 24
    index = [
        datetime(2004, 4, 2) + timedelta(minutes=interval * x)
        for x in range(1, num_intervals + 1)
    ]
    e1 = [randrange(1, 10) for x in range(1, num_intervals + 1)]
    e2 = [randrange(1, 5) for x in range(1, num_intervals + 1)]
    s1 = pd.Series(data=e1, index=index, name="E1")
    s2 = pd.Series(data=e2, index=index, name="E2")
    df = pd.concat([s1, s2], axis=1)

    # Drop some dataframe rows to break things
    df = df.drop(index[0])
    df = df.drop(index[1])
    df = df.drop(index[3])
    df = df.drop(index[4])
    df = df.drop(index[-1])

    nmi = "B123"
    m = NEM12(to_participant=nmi)
    m.add_dataframe(nmi=nmi, df=df, uoms={"E1": "kWh", "E2": "kWh"})
    output_file = f"tests/{m.nem_filename()}.zip"
    fp = m.output_zip(file_path=output_file)
    assert fp.name == f"{m.nem_filename()}.zip"

    dfs = output_as_data_frames(output_file)
    nmi2, df2 = dfs[0]  # Return data for first NMI in file
    df2 = df2.loc[df2["quality_method"].isin(["A"])]  # Remove the Nulls
    assert nmi == nmi2
    assert [float(x) for x in df["E1"]] == [float(x) for x in df2["E1"]]
    per1 = [x.strftime("%Y%m%d%H%M") for x in df.index]
    per2 = [x.strftime("%Y%m%d%H%M") for x in df2["t_end"]]
    assert per1 == per2


def test_df_different_intervals():
    """Create export from dataframe with different interval lenghts"""
    interval = 30
    num_intervals = int(60 / interval) * 24
    index = [
        datetime(2004, 4, 2) + timedelta(minutes=interval * x)
        for x in range(1, num_intervals + 1)
    ]
    e1 = [randrange(1, 10) for x in range(1, num_intervals + 1)]
    e2 = [randrange(1, 5) for x in range(1, num_intervals + 1)]
    s1 = pd.Series(data=e1, index=index, name="E1")
    s2 = pd.Series(data=e2, index=index, name="E2")

    interval2 = 10
    num_intervals2 = int(60 / interval2) * 24
    index2 = [
        datetime(2004, 4, 2) + timedelta(minutes=interval2 * x)
        for x in range(1, num_intervals2 + 1)
    ]
    v1 = [randrange(22000, 24000) for x in range(1, num_intervals2 + 1)]
    v1 = [x / 100 for x in v1]
    s3 = pd.Series(data=v1, index=index2, name="V1")
    df = pd.concat([s1, s2, s3], axis=1)

    nmi = "C123"
    m = NEM12(to_participant=nmi)
    m.add_dataframe(nmi=nmi, df=df, uoms={"E1": "kWh", "E2": "kWh"})
    output_file = f"tests/{m.nem_filename()}.zip"
    fp = m.output_zip(file_path=output_file)
    assert fp.name == f"{m.nem_filename()}.zip"

    dfs = output_as_data_frames(output_file)
    nmi2, df2 = dfs[0]  # Return data for first NMI in file
    assert nmi == nmi2
    assert df["E1"].count() == df2["E1"].count()
    assert df["V1"].count() == df2["V1"].count()

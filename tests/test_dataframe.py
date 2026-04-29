from datetime import datetime, timedelta
from random import randrange

import polars as pl
from nemreader import output_as_data_frames

from nemwriter import NEM12

EXAMPLE_START = datetime(2004, 4, 2)


def example_energy_dataframe(
    interval: int = 5, start: datetime = EXAMPLE_START
) -> pl.DataFrame:
    """Generate some example energy data in a dataframe for testing"""
    num_intervals = int(60 / interval) * 24
    timestamps = [
        start + timedelta(minutes=interval * x) for x in range(1, num_intervals + 1)
    ]
    e1 = [randrange(1, 10) for x in range(1, num_intervals + 1)]
    e2 = [randrange(1, 5) for x in range(1, num_intervals + 1)]
    df = pl.DataFrame(
        {
            "t_end": timestamps,
            "E1": e1,
            "E2": e2,
        }
    )
    return df


def example_voltage_dataframe(
    interval: int = 10, start: datetime = EXAMPLE_START
) -> pl.DataFrame:
    """Generate some example voltage data in a dataframe for testing"""
    num_intervals = int(60 / interval) * 24
    timestamps = [
        start + timedelta(minutes=interval * x) for x in range(1, num_intervals + 1)
    ]
    v1 = [randrange(22000, 24000) for x in range(1, num_intervals + 1)]
    v1 = [x / 100 for x in v1]
    df = pl.DataFrame(
        {
            "t_end": timestamps,
            "V1": v1,
        }
    )
    return df


def test_dataframe_export():
    """Create export from dataframe"""

    df = example_energy_dataframe()

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
    per1 = [x.strftime("%Y%m%d%H%M") for x in df["t_end"]]
    per2 = [x.strftime("%Y%m%d%H%M") for x in df2["t_end"]]
    assert per1 == per2


def test_df_export_missing_intervals():
    """Create export from dataframe with missing intervals"""
    df = example_energy_dataframe(interval=30)

    # Drop some dataframe rows to break things
    indices_to_drop = [0, 1, 3, 4, 10]
    df = df.filter(~pl.arange(0, pl.count()).is_in(indices_to_drop))

    nmi = "B123"
    m = NEM12(to_participant=nmi)
    m.add_dataframe(nmi=nmi, df=df, uoms={"E1": "kWh", "E2": "kWh"})
    output_file = f"tests/{m.nem_filename()}.zip"
    fp = m.output_zip(file_path=output_file)
    assert fp.name == f"{m.nem_filename()}.zip"

    dfs = output_as_data_frames(output_file)
    nmi2, df2 = dfs[0]  # Return data for first NMI in file
    df2 = df2.filter(pl.col("quality_method").is_in(["A"]))  # Remove the Nulls
    assert nmi == nmi2
    assert [float(x) for x in df["E1"]] == [float(x) for x in df2["E1"]]
    per1 = [x.strftime("%Y%m%d%H%M") for x in df["t_end"]]
    per2 = [x.strftime("%Y%m%d%H%M") for x in df2["t_end"]]
    assert per1 == per2


def test_df_different_intervals():
    """Create export from dataframe with different interval lenghts"""

    df1 = example_energy_dataframe(interval=30)
    df2 = example_voltage_dataframe(interval=10)

    # Join on t_end, using outer join to preserve all timestamps
    df = df1.join(df2, on="t_end", how="full", coalesce=True)

    nmi = "C123"
    m = NEM12(to_participant=nmi)
    m.add_dataframe(nmi=nmi, df=df, uoms={"E1": "kWh", "E2": "kWh"})
    output_file = f"tests/{m.nem_filename()}.zip"
    fp = m.output_zip(file_path=output_file)
    assert fp.name == f"{m.nem_filename()}.zip"

    dfs = output_as_data_frames(output_file)
    nmi2, df2_result = dfs[0]  # Return data for first NMI in file
    assert nmi == nmi2
    # Count non-null E1 values
    assert df["E1"].count() == df2_result["E1"].count()
    # V1 might not be present in all rows due to different intervals
    assert df["V1"].count() == df2_result["V1"].count()

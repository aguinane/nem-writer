import csv
import pandas as pd
from datetime import datetime, timedelta
from random import randrange
from nemwriter import NEM12


TEST_FILES = [
    "examples/actual_interval.csv",
    "examples/multiple_quality.csv",
]


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

    m = NEM12(to_participant="A123")

    m.add_dataframe(nmi="A123", df=df, uoms={"E1": "kWh", "E2": "kWh"})
    output_file = f"tests/{m.nem_filename()}.zip"
    fp = m.output_zip(file_path=output_file)
    assert fp.name == f"{m.nem_filename()}.zip"

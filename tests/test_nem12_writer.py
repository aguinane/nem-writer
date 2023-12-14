import csv
from datetime import datetime, timedelta

from nemwriter import NEM12


def test_add_readings_adds_300_row_timestamps():
    """
    update_datetime and msats_load_datetime should be added to the end of 300 rows if provided.
    """
    # Create a NEM12 CSV with one 300 row of 30-minute reads.
    nem12 = NEM12(to_participant="123")
    readings = [
        [datetime(2004, 4, 18, 0, 0) + timedelta(minutes=30 * (i + 1)), i, "A"]
        for i in range(24 * 2)
    ]
    update_datetime = datetime(2004, 4, 20, 9, 1, 3, 1)
    msats_load_datetime = datetime(2004, 4, 19, 1, 23, 40, 2)

    nem12.add_readings(
        nmi="123",
        nmi_configuration="E1B1B2",
        nmi_suffix="E1",
        uom="kWh",
        readings=readings,
        update_datetime=update_datetime,
        msats_load_datetime=msats_load_datetime,
    )
    output_file = f"tests/{nem12.nem_filename()}.csv"
    nem12.output_csv(output_file)

    with open(output_file) as f:
        csv_rows = list(csv.reader(f))

    row = csv_rows[2]
    # Check we're looking at the 300 row and it's the expected length.
    assert row[0] == "300" and len(row) == 55
    # The timestamps should appear in the last two columns in DateTime(14) format as
    # defined in the Meter Data File Format Specification.
    assert row[53] == "20040420090103"
    assert row[54] == "20040419012340"

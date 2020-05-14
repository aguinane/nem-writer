import os
import sys
import csv
from pathlib import Path
import pytest
import nemreader as nr

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from nemwriter import NEM12

TEST_FILES = [
    "examples/actual_interval.csv",
    "examples/multiple_quality.csv",
]


def import_export_nem12(input_file):
    """ Create export from import """
    # Read in example
    ex = nr.read_nem_file(input_file)

    # Create output file
    file_stem = Path(input_file).stem
    output_file = "tests/test_output_{}.csv".format(file_stem)
    m = NEM12(to_participant=ex.header.to_participant)

    # Add in interval readings
    to_load = []
    for nmi in ex.readings:
        nmi_configuration = "".join(list(ex.transactions[nmi].keys()))
        for channel in ex.readings[nmi]:
            # Build list of readings
            for read in ex.readings[nmi][channel]:
                to_load.append(
                    [
                        read.t_end,
                        read.read_value,
                        read.quality_method,
                        read.event_code,
                        read.event_desc,
                    ]
                )

            # Get common atributes from last reading
            last = ex.readings[nmi][channel][-1:][0]
            uom = last.uom
            interval_length = int((last.t_end - last.t_start).seconds / 60)

            m.add_readings(
                nmi=nmi,
                nmi_configuration=nmi_configuration,
                nmi_suffix=channel,
                uom=uom,
                interval_length=interval_length,
                readings=to_load,
            )

    # Export to file
    output = m.nem_output(file_name=output_file)
    return output


@pytest.mark.parametrize("example_file", TEST_FILES)
def test_importexport_nem12(example_file):
    """ Read in a NEM12 file and check if it is exported the same """

    output_file = import_export_nem12(example_file)

    # Compare files
    original = []
    with open(example_file, newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=",", quotechar='"')
        for row in reader:
            original.append(row)

    output = []
    with open(output_file, newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=",", quotechar='"')
        for row in reader:
            output.append(row)

    # Compare rows
    for i, row in enumerate(original):
        record_indicator = row[0]
        if record_indicator not in ["100", "200"]:
            for j, col in enumerate(row):
                if j not in [4, 5, 53, 54]:
                    assert col.rstrip("0") == output[i][j].rstrip(
                        "0"
                    ), "[{i},{j}] did not match".format(i=i, j=j)

import csv

import nemreader as nr
import pytest

from nemwriter import NEM13

TEST_FILES = [
    "examples/actual_accumulated.csv",
]


def import_export_nem13(input_file):
    """Create export from import"""

    # Read in example
    ex = nr.read_nem_file(input_file)

    # Create output file
    m = NEM13(to_participant=ex.header.to_participant)

    # Add in interval readings
    for nmi in ex.readings:
        nmi_configuration = "".join(list(ex.transactions[nmi].keys()))
        for channel in ex.readings[nmi]:
            # Build list of readings
            for read in ex.readings[nmi][channel]:
                m.add_reading(
                    nmi=nmi,
                    nmi_configuration=nmi_configuration,
                    register_id=None,
                    nmi_suffix=channel,
                    previous_read=read.val_start,
                    previous_read_date=read.t_start,
                    current_read=read.val_end,
                    current_read_date=read.t_end,
                    current_quality_method=read.quality_method,
                    quantity=read.read_value,
                    uom=read.uom,
                )

    # Export to file
    output_file = f"tests/{m.nem_filename()}.csv"
    output = m.output_csv(file_path=output_file)
    return output


def cleanse_val(val):
    new_val = val
    if val[-2:] == ".0":
        new_val = val[:-2]
    else:
        new_val = val
    return new_val


@pytest.mark.parametrize("example_file", TEST_FILES)
def test_importexport_nem13(example_file):
    """Read in a NEM13 file and check if it is exported the same"""

    output_file = import_export_nem13(example_file)

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
                if j not in [3, 5, 6, 10, 21, 22]:
                    assert cleanse_val(col) == cleanse_val(
                        output[i][j]
                    ), "[{i},{j}] did not match".format(i=i, j=j)

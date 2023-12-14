import csv

import nemreader as nr
import pytest

from nemwriter import NEM12

TEST_FILES = [
    "examples/actual_interval.csv",
    "examples/multiple_quality.csv",
    "examples/interval_change.csv",
]


def import_export_nem12(input_file, zip_output=False):
    """Create export from import"""
    # Read in example
    ex = nr.read_nem_file(input_file)

    # Create output file
    m = NEM12(to_participant=ex.header.to_participant)

    # Add in interval readings

    for nmi in ex.readings:
        nmi_configuration = "".join(list(ex.transactions[nmi].keys()))
        for nmi_suffix in ex.readings[nmi]:
            to_load = []
            # Build list of readings
            for read in ex.readings[nmi][nmi_suffix]:
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
            last = ex.readings[nmi][nmi_suffix][-1:][0]
            uom = last.uom

            m.add_readings(
                nmi=nmi,
                nmi_configuration=nmi_configuration,
                nmi_suffix=nmi_suffix,
                uom=uom,
                readings=to_load,
            )

    # Export to file
    if zip_output:
        output_file = f"tests/{m.nem_filename()}.zip"
        output = m.output_zip(file_path=output_file)
    else:
        output_file = f"tests/{m.nem_filename()}.csv"
        output = m.output_csv(file_path=output_file)
    return output


def test_importexport_zippednem12():
    """Check that a zipped NEM12 is unchanged"""

    example_file = "examples/year_example.csv"
    output_file = import_export_nem12(example_file, zip_output=True)

    f1 = nr.read_nem_file(example_file)
    f2 = nr.read_nem_file(output_file)

    nmi = "NMIxxxxxxx"
    ch = "E1"
    for i, read1 in enumerate(f1.readings[nmi][ch]):
        read2 = f2.readings[nmi][ch][i]
        # Cannot compare all attributes as meter serial number is not included
        assert read1.t_start == read2.t_start
        assert read1.read_value == read2.read_value
        assert read1.quality_method == read2.quality_method
        assert read1.event_code == read2.event_code
        assert read1.event_desc == read2.event_desc


@pytest.mark.parametrize("example_file", TEST_FILES)
def test_importexport_nem12(example_file):
    """Read in a NEM12 file and check if it is exported the same"""

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
        num_cols = len(row)
        if record_indicator not in ["100", "200"]:
            for j, col in enumerate(row):
                if j not in [4, 5] and j < (num_cols - 5):
                    try:
                        clean_i = float(col)
                    except ValueError:
                        clean_i = col
                    try:
                        clean_j = float(output[i][j])
                    except ValueError:
                        clean_j = output[i][j]

                    assert clean_i == clean_j, "Row {i} Col {j} did not match".format(
                        i=i, j=j
                    )

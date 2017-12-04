import os
import sys
import csv
import nemreader as nr
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))
from nemwriter import NEM12


def test_basic_example():
    export_nem12('actual_interval')


def test_quality_example():
    export_nem12('multiple_quality')


def export_nem12(example):
    """ Read in a NEM12 file and check if it is exported the same """
    # Read in example
    example_file = 'tests/{}.csv'.format(example)
    ex = nr.read_nem_file(example_file)

    # Create output file
    output_file = 'tests/test_output_{}.csv'.format(example)
    m = NEM12(to_participant=ex.header.to_participant)

    # Add in interval readings
    to_load = []
    for nmi in ex.readings:
        nmi_configuration = ''.join(list(ex.transactions[nmi].keys()))
        for channel in ex.readings[nmi]:
            # Build list of readings
            for read in ex.readings[nmi][channel]:
                to_load.append([read.t_end, read.read_value,
                                read.quality_method, read.event])

            # Get common atributes from last reading
            last = ex.readings[nmi][channel][-1:][0]
            uom = last.uom
            interval_length = int((last.t_end - last.t_start).seconds / 60)

            ch = m.add_readings(nmi=nmi,
                                nmi_configuration=nmi_configuration,
                                nmi_suffix=channel, uom=uom,
                                interval_length=interval_length,
                                readings=to_load)

    # Export to file
    output = m.nem_output(file_name=output_file)

    # Compare files
    original = []
    with open(example_file, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in reader:
            original.append(row)

    output = []
    with open(output_file, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in reader:
            output.append(row)

    # Compare rows
    for i, row in enumerate(original):
        record_indicator = row[0]
        if record_indicator not in ['100', '200']:
            assert row == output[i]

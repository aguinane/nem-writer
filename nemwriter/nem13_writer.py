"""
    nemwriter.nem_writer
    ~~~~~
    Write meter readings to MDFF format
"""

import csv
import datetime
from typing import Optional, Generator


class NEM13(object):
    """ An NEM file object """

    def __init__(
        self, to_participant: str, from_participant: Optional[str] = None
    ) -> None:

        version_header = "NEM13"
        self.file_time = datetime.datetime.now().strftime("%Y%m%d%H%M")
        self.from_participant = from_participant
        self.to_participant = to_participant
        self.header = [
            100,
            version_header,
            self.file_time,
            from_participant,
            to_participant,
        ]

        self.meters = dict()

    def add_reading(
        self,
        nmi,
        nmi_configuration,
        register_id,
        nmi_suffix,
        previous_read,
        previous_read_date,
        current_read,
        current_read_date,
        quantity,
        mdm_datastream_identitfier=None,
        meter_serial_number=None,
        direction_indicator="E",
        previous_quality_method=None,
        previous_reason_code=None,
        previous_reason_desc=None,
        current_quality_method=None,
        current_reason_code=None,
        current_reason_desc=None,
        uom="kWh",
        next_scheduled_read_date=None,
        update_date=None,
        mstats_load_date=None,
    ):

        if nmi not in self.meters:
            self.meters[nmi] = dict()

        if nmi_suffix not in self.meters[nmi]:
            self.meters[nmi][nmi_suffix] = list()

        data_record = [
            250,
            nmi,
            nmi_configuration,
            register_id,
            nmi_suffix,
            mdm_datastream_identitfier,
            meter_serial_number,
            direction_indicator,
            previous_read,
            previous_read_date.strftime("%Y%m%d%H%M%S"),
            previous_quality_method,
            previous_reason_code,
            previous_reason_desc,
            current_read,
            current_read_date.strftime("%Y%m%d%H%M%S"),
            current_quality_method,
            current_reason_code,
            current_reason_desc,
            quantity,
            uom,
            next_scheduled_read_date,
            update_date,
            mstats_load_date,
        ]

        self.meters[nmi][nmi_suffix].append(data_record)

    def __repr__(self):
        return "<NEM13 Builder {} {}>".format(self.file_time, self.to_participant)

    def build_output(self) -> Generator[list, None, None]:
        """ Emit rows for NEM file """
        yield self.header
        for nmi in self.meters:
            for ch in self.meters[nmi]:
                readings = self.meters[nmi][ch]
                for reading in readings:
                    yield reading
        yield [900]  # End of data row

    def nem_output(self, file_path="nem13_output.csv") -> str:
        """ Output NEM file """
        with open(file_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
            for row in self.build_output():
                writer.writerow(row)
        return file_path

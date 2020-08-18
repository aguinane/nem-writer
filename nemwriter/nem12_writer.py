"""
    nemwriter.nem_writer
    ~~~~~
    Write meter readings to MDFF format
"""

import csv
import datetime
from io import StringIO
from zipfile import ZipFile, ZipInfo
from zipfile import ZIP_DEFLATED
from pathlib import Path
from typing import Optional, Generator


class NEM12(object):
    """ An NEM file object """

    def __init__(
        self, to_participant: str, from_participant: Optional[str] = None
    ) -> None:

        version_header = "NEM12"
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

    def __repr__(self):
        return "<NEM12 Builder {} {}>".format(self.file_time, self.to_participant)

    def add_readings(
        self,
        nmi,
        nmi_configuration,
        nmi_suffix,
        uom,
        interval_length,
        readings,
        register_id=None,
        mdm_datastream_identitfier=None,
        meter_serial_number=None,
        next_scheduled_read_date=None,
    ):

        if nmi not in self.meters:
            self.meters[nmi] = dict()

        self.meters[nmi][nmi_suffix] = list()

        channel = []
        channel.append(
            [
                200,
                nmi,
                nmi_configuration,
                register_id,
                nmi_suffix,
                mdm_datastream_identitfier,
                meter_serial_number,
                uom,
                interval_length,
                next_scheduled_read_date,
            ]
        )

        interval_delta = datetime.timedelta(seconds=60 * interval_length)
        reading_dict = dict()
        for reading in readings:
            # Input: end, val, quality, event_code, event_desc
            # Output: pos, start, end, val, quality, event_code, event_desc
            end = reading[0]
            val = reading[1]
            try:
                quality = reading[2]
            except IndexError:
                quality = None
            try:
                event_code = reading[3]
            except IndexError:
                event_code = None
            try:
                event_desc = reading[4]
            except IndexError:
                event_desc = None

            start = end - interval_delta
            pos = self.get_interval_pos(start, interval_length)
            date = start.strftime("%Y%m%d")
            if date not in reading_dict:
                reading_dict[date] = dict()
            row = (pos, start, end, val, quality, event_code, event_desc)
            reading_dict[date][pos] = row

        channel.append(reading_dict)

        self.meters[nmi][nmi_suffix] = channel

    @staticmethod
    def get_interval_pos(start: int, interval_length: int) -> int:
        """ Get position of time interval """
        num_intervals = 60 * 24 / interval_length
        minutes = (start.hour) * 60 + start.minute
        day_progress = minutes / (60 * 24)
        return int(day_progress * num_intervals)

    @staticmethod
    def get_num_intervals(interval_length: int) -> int:
        """ Get the number of intervals in a day """
        return int(60 * 24 / interval_length)

    def build_output(self) -> Generator[list, None, None]:
        """ Emit rows for NEM file """
        yield self.header
        for nmi in sorted(self.meters):
            for ch in sorted(self.meters[nmi]):
                channel_header = self.meters[nmi][ch][0]
                interval_length = channel_header[8]
                yield channel_header
                readings = self.meters[nmi][ch][1]
                for day in readings:
                    daily_readings = readings[day]
                    for row in self.get_daily_rows(
                        day, daily_readings, interval_length
                    ):
                        yield row
        yield [900]  # End of data row

    def get_daily_rows(
        self, day: str, daily_readings: list, interval_length: int
    ) -> Generator[list, None, None]:
        """ Emit 300 row for the day data and 400 rows if required """
        day_row = [300, day]
        day_events = []
        num_pos = self.get_num_intervals(interval_length)
        for pos in range(0, num_pos):
            try:
                read = daily_readings[pos]
                # pos, start, end, val, quality, event_code, event_desc
                val = read[3]
                quality = read[4]
                event_code = read[5]
                event_desc = read[6]
                try:
                    prev = day_events[-1]
                except IndexError:  # First row
                    prev = (None, None, None, None)
                if (quality, event_code, event_desc) == (prev[1], prev[2], prev[3],):
                    pass
                else:
                    event_record = (pos, quality, event_code, event_desc)
                    day_events.append(event_record)
            except KeyError:
                val = None
            day_row.append(val)

        event_rows = []
        if len(day_events) == 1:
            # Same quality for all records
            first_event = day_events[0]
            quality_method = first_event[1]
            event_code = first_event[2]
            event_desc = first_event[3]
        else:
            quality_method = "V"
            event_code = None
            event_desc = None
            # Create an event row
            for i, (pos, quality, code, desc) in enumerate(day_events):
                try:
                    end_pos = day_events[i + 1][0]
                except IndexError:
                    end_pos = self.get_num_intervals(interval_length)
                event_row = [
                    "400",
                    pos + 1,
                    end_pos,
                    quality,
                    code,
                    desc,
                ]
                event_rows.append(event_row)
        update_time = None
        MSTATS_time = None

        day_row_end = [
            quality_method,
            event_code,
            event_desc,
            update_time,
            MSTATS_time,
        ]
        day_row += day_row_end
        yield day_row
        # Emit event (400) rows if they exist
        for event_row in event_rows:
            yield event_row

    def nem_filename(self) -> str:
        """ Return suggested NEM filename """
        nmis = list(self.meters.keys())
        first_nmi = nmis[0]
        nmi_suffix = list(self.meters[first_nmi].keys())[0]
        days = list(self.meters[first_nmi][nmi_suffix][-1].keys())
        start = days[0]
        end = days[-1]
        if len(nmis) > 1:
            uid = f"{start}_{end}"
        else:
            uid = f"{first_nmi}_{start}_{end}"
        file_name = f"NEM12#{uid}#{self.from_participant}#{self.to_participant}"
        return file_name

    def output_csv(self, file_path="") -> str:
        """ Output NEM file """
        if not file_path:
            file_path = f"{self.nem_filename()}.csv"
        with open(file_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
            for row in self.build_output():
                writer.writerow(row)
        return file_path

    def output_zip(self, file_path="") -> str:
        """ Output NEM file """
        if not file_path:
            file_path = f"{self.nem_filename()}.zip"
        file_path = Path(file_path)

        with StringIO(newline="") as stream:
            writer = csv.writer(stream, quoting=csv.QUOTE_MINIMAL)
            for row in self.build_output():
                writer.writerow(row)
            csv_str = stream.getvalue()

        with ZipFile(file_path, "w", compression=ZIP_DEFLATED) as zip_archive:
            file1 = ZipInfo(f"{file_path.stem}.csv")
            zip_archive.writestr(file1, csv_str, compress_type=ZIP_DEFLATED)
        return file_path

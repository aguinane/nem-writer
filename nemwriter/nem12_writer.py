"""
    nemwriter.nem_writer
    ~~~~~
    Write meter readings to MDFF format
"""

import csv
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Dict, Generator, Iterable, Optional, Union
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

import numpy as np
from pandas import DataFrame

UOMS = {"E1": "kWh", "E2": "kWh", "B1": "kWh"}


def convert_to_channels(df: DataFrame) -> Dict[str, list]:
    """Convert dataframe to lists of channel data
    Assumes the dataframe index is the end of the metering interval
    """
    d = {}

    read_ends = df.index.tolist()
    channels = list(df.columns)

    if "Quality" in channels:
        qualities = df["Quality"].tolist()
        channels.remove("Quality")
    else:
        qualities = ["A" for x in read_ends]

    if "EventDesc" in channels:
        eventdescs = df["EventDesc"].tolist()
        channels.remove("EventDesc")
    else:
        eventdescs = [None for x in read_ends]

    # Input: end, val, quality, event_code, event_desc
    for channel in channels:
        ch_data = []
        for i, val in enumerate(df[channel].tolist()):
            if np.isnan(val):
                continue  # Skip Nulls
            end = read_ends[i]
            quality = qualities[i]
            event_code = None
            event_desc = eventdescs[i]
            read = (end, val, quality, event_code, event_desc)
            ch_data.append(read)
        d[channel] = ch_data
    return d


def remove_zero_decimal(x: float) -> Union[float, int]:
    """Make integer when decimal is a zero to shrink file size"""
    if x != int(x):
        return x
    return int(x)


class NEM12(object):
    """An NEM file object"""

    def __init__(
        self, to_participant: str, from_participant: Optional[str] = None
    ) -> None:
        version_header = "NEM12"
        self.file_time = datetime.now().strftime("%Y%m%d%H%M")
        self.from_participant = from_participant
        self.to_participant = to_participant
        self.header = [
            100,
            version_header,
            self.file_time,
            from_participant,
            to_participant,
        ]

        self.meters = {}
        self.days = []

    def __repr__(self):
        return "<NEM12 Builder {} {}>".format(self.file_time, self.to_participant)

    @property
    def is_empty(self) -> bool:
        if not self.meters:
            return True
        return False

    def add_readings(
        self,
        nmi: str,
        nmi_configuration: str,
        nmi_suffix: str,
        uom: str,
        readings: Iterable[Union[list, tuple]],
        register_id: str = "",
        mdm_datastream_identitfier: str = "",
        meter_serial_number: str = "",
        next_scheduled_read_date: Optional[datetime] = None,
        update_datetime: Optional[datetime] = None,
        msats_load_datetime: Optional[datetime] = None,
    ):
        if nmi not in self.meters:
            self.meters[nmi] = {}

        if nmi_suffix not in self.meters[nmi]:
            self.meters[nmi][nmi_suffix] = []

        daily_readings = {}
        for reading in readings:
            end = reading[0]
            start = end - timedelta(seconds=5)
            date = start.strftime("%Y%m%d")

            if date not in daily_readings:
                daily_readings[date] = [reading]
            else:
                daily_readings[date].append(reading)

        dates = [x for x in daily_readings.keys()]
        self.days = dates

        last_header = []
        reading_dict = dict()
        for date in dates:
            # Determine the interval length
            interval_length = 5  # Assume default in case needed
            interval_delta = timedelta(minutes=5)
            interval_lengths = []
            for i, _ in enumerate(daily_readings[date]):
                if i == 0:
                    continue
                first = daily_readings[date][i - 1]
                second = daily_readings[date][i]
                interval_delta = second[0] - first[0]
                interval_length = int(interval_delta.seconds / 60)
                interval_lengths.append(interval_length)
            if interval_lengths:
                interval_length = min(interval_lengths)

            channel_header = [
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

            if channel_header != last_header:
                self.meters[nmi][nmi_suffix].append(channel_header)

            last_header = channel_header

            rows = []
            for reading in daily_readings[date]:
                # Input: end, val, quality, event_code, event_desc
                # Output: pos, start, end, val, quality, event_code, event_desc
                end = reading[0]
                val = reading[1]
                val = remove_zero_decimal(val)  # Make int to make file smaller
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
                if date not in reading_dict:
                    reading_dict[date] = dict()
                row = (pos, start, end, val, quality, event_code, event_desc)
                rows.append(row)

            for row in self.get_daily_rows(
                date, rows, interval_length, update_datetime, msats_load_datetime
            ):
                self.meters[nmi][nmi_suffix].append(row)

    def add_dataframe(
        self,
        nmi: str,
        df: DataFrame,
        uoms: Dict[str, str] = UOMS,
        meter_serial_number: str = "",
    ):
        """Add readings from pandas dataframe"""

        channels = convert_to_channels(df)
        channel_config = "".join(channels.keys())
        for nmi_suffix in channels.keys():
            uom = uoms.get(nmi_suffix, "")
            self.add_readings(
                nmi=nmi,
                nmi_configuration=channel_config,
                nmi_suffix=nmi_suffix,
                uom=uom,
                readings=channels[nmi_suffix],
                meter_serial_number=meter_serial_number,
            )

    @staticmethod
    def get_interval_pos(start: int, interval_length: int) -> int:
        """Get position of time interval"""
        num_intervals = 60 * 24 / interval_length
        minutes = (start.hour) * 60 + start.minute
        day_progress = minutes / (60 * 24)
        return int(day_progress * num_intervals)

    @staticmethod
    def get_num_intervals(interval_length: int) -> int:
        """Get the number of intervals in a day"""
        return int(60 * 24 / interval_length)

    def build_output(self) -> Generator[list, None, None]:
        """Emit rows for NEM file"""
        yield self.header
        for nmi in sorted(self.meters):
            suffixes = list(self.meters[nmi].keys())
            for ch in sorted(suffixes):
                for row in self.meters[nmi][ch]:
                    yield row
        yield [900]  # End of data row

    def get_daily_rows(
        self,
        day: str,
        daily_readings: list,
        interval_length: int,
        update_datetime: Optional[datetime],
        msats_load_datetime: Optional[datetime],
    ) -> Generator[list, None, None]:
        """Emit 300 row for the day data and 400 rows if required"""

        day_reads_dict = {}
        for read in daily_readings:
            pos = read[0]  # pos is first item in tuple
            day_reads_dict[pos] = read

        day_row = [300, day]
        day_events = []
        num_pos = self.get_num_intervals(interval_length)

        for pos in range(0, num_pos):
            try:
                read = day_reads_dict[pos]
                # pos, start, end, val, quality, event_code, event_desc
                val = read[3]
                quality = read[4]
                event_code = read[5]
                event_desc = read[6]
            except KeyError:
                val = 0
                quality = "N"
                event_code = None
                event_desc = None

            try:
                prev = day_events[-1]
            except IndexError:  # First row
                prev = (None, None, None, None)

            if (quality, event_code, event_desc) == (
                prev[1],
                prev[2],
                prev[3],
            ):
                pass
            else:
                event_record = (pos, quality, event_code, event_desc)
                day_events.append(event_record)
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

        datetime_format = "%Y%m%d%H%M%S"

        update_time = None
        if update_datetime is not None:
            update_time = update_datetime.strftime(datetime_format)

        msats_time = None
        if msats_load_datetime is not None:
            msats_time = msats_load_datetime.strftime(datetime_format)

        day_row_end = [
            quality_method,
            event_code,
            event_desc,
            update_time,
            msats_time,
        ]
        day_row += day_row_end
        yield day_row
        # Emit event (400) rows if they exist
        for event_row in event_rows:
            yield event_row

    def nem_filename(self) -> str:
        """Return suggested NEM filename"""
        start = self.days[0]
        end = self.days[-1]
        nmis = list(self.meters.keys())
        first_nmi = nmis[0]
        if len(nmis) == 1:
            uid = f"{first_nmi}_{start}_{end}"
        else:
            uid = f"{start}_{end}"
        file_name = f"NEM12#{uid}#{self.from_participant}#{self.to_participant}"
        return file_name

    def output_csv(self, file_path="") -> str:
        """Output NEM file"""
        if self.is_empty:
            raise ValueError("No readings to output")

        if not file_path:
            file_path = f"{self.nem_filename()}.csv"
        with open(file_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
            for row in self.build_output():
                writer.writerow(row)
        return file_path

    def output_zip(self, file_path="") -> str:
        """Output NEM file"""
        if self.is_empty:
            raise ValueError("No readings to output")

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

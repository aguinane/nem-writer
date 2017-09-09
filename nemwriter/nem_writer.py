"""
    nemwriter.nem_writer
    ~~~~~
    Write meter readings to MDFF format
"""

import csv
import datetime


class NEM12(object):
    """ An NEM file object """

    def __init__(self, to_participant: str, from_participant=None) -> None:

        version_header = 'NEM12'
        self.file_time = datetime.datetime.now().strftime('%Y%m%d%H%M')
        self.from_participant = from_participant
        self.to_participant = to_participant
        self.header = [100, version_header, self.file_time,
                       from_participant, to_participant]

        self.meters = dict()

    def add_readings(self, nmi, nmi_configuration, nmi_suffix,
                     uom, interval_length,
                     readings,
                     register_id=None,
                     mdm_datastream_identitfier=None,
                     meter_serial_number=None,
                     next_scheduled_read_date=None):

        if nmi not in self.meters:
            self.meters[nmi] = dict()

        self.meters[nmi][nmi_suffix] = list()

        channel = []
        channel.append([200, nmi,
                        nmi_configuration,
                        register_id,
                        nmi_suffix,
                        mdm_datastream_identitfier,
                        meter_serial_number,
                        uom,
                        interval_length,
                        next_scheduled_read_date]
                       )

        interval_delta = datetime.timedelta(seconds=60 * interval_length)
        reading_dict = dict()
        for reading in readings:
            end = reading[0]
            val = reading[1]
            try:
                quality = reading[2]
            except IndexError:
                quality = None
            try:
                event = reading[3]
            except IndexError:
                event = None

            start = end - interval_delta
            pos = self.get_interval_pos(start, interval_length)
            date = start.strftime('%Y%m%d')
            if date not in reading_dict:
                reading_dict[date] = dict()
            row = (pos, start, end, val, quality, event)
            reading_dict[date][pos] = row

        channel.append(reading_dict)

        self.meters[nmi][nmi_suffix] = channel

    @staticmethod
    def get_interval_pos(start, interval_length):
        """ Get position of time interval """
        num_intervals = 60 * 24 / interval_length
        minutes = (start.hour) * 60 + start.minute
        day_progress = minutes / (60 * 24)
        return int(day_progress * num_intervals)

    def __repr__(self):
        return "<NEM12 {} {}>".format(self.file_time, self.to_participant)

    def nem_output(self, file_name='ouput.csv'):
        """ Output NEM file """
        with open(file_name, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(self.header)
            for nmi in self.meters:
                for ch in self.meters[nmi]:
                    channel_header = self.meters[nmi][ch][0]
                    writer.writerow(channel_header)
                    readings = self.meters[nmi][ch][1]
                    for day in readings:
                        day_row = [300, day]
                        day_events = []
                        for pos in range(0, 48):
                            try:
                                read = readings[day][pos]
                                end = readings[day][pos][2]
                                val = readings[day][pos][3]
                                quality = readings[day][pos][4]
                                code = None
                                event = readings[day][pos][5]
                                day_row.append(val)

                                try:
                                    prev = day_events[-1]
                                except IndexError:  # First row
                                    prev = (None, None, None, None)
                                if (quality, code, event) == (prev[1], prev[2], prev[3]):
                                    pass
                                else:
                                    event_record = (pos, quality, code, event)
                                    day_events.append(event_record)

                            except KeyError:
                                day_row.append(None)
                        event_rows = []
                        if len(day_events) == 1:
                            quality_method = day_events[0][1]
                            reason_code = day_events[0][2]
                        else:
                            quality_method = 'V'
                            reason_code = None
                            for i, (pos, quality, code, event) in enumerate(day_events):
                                try:
                                    end_pos = day_events[i+1][0]
                                except IndexError:
                                    end_pos = 48
                                event_row = ['400', pos+1, end_pos,
                                             quality, code, event]
                                event_rows.append(event_row)

                        reason_desc = None
                        update_time = None
                        MSTATS_time = None
                        day_row += [quality_method, reason_code, reason_desc,
                                    update_time, MSTATS_time]
                        writer.writerow(day_row)
                        if event_rows:
                            for event_row in event_rows:
                                writer.writerow(event_row)

            writer.writerow([900])  # End of data row
            return csvfile

import os
import sys
path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "python")
if path not in sys.path:
    sys.path.append(path)
from cbrs_events import InLineEventPrinter
from cbrs_events import CsvEventPrinter
import unittest
import sys
import io
from unittest.mock import Mock
import contextlib


class TestInLineEventPrinter(unittest.TestCase):
    def setUp(self):
        self.schema_parser = Mock()
        self.schema_parser.get_headers.return_value = ["EVENT_NAME", "EVENT_OCCURRED_ON", "EVENT_VERSION"]
        self.event_decompressor = Mock()
        self.event_decompressor.get_processed_value.return_value = "2023,10"
        self.event_printer = InLineEventPrinter(self.schema_parser, self.event_decompressor)

    def test_print_event(self):
        expected_result = """EVENT_NAME: EVENT1
EVENT_OCCURRED_ON: svc-4-dpmediation
EVENT_VERSION: 2023,10

"""
        with io.StringIO() as buf:
            with contextlib.redirect_stdout(buf):
                sys.stdout = buf
                self.event_printer.print_event('EVENT1', "svc-4-dpmediation", ["Event1", "2023,10"])
            self.assertEqual(buf.getvalue(), expected_result)


class SeparatorEventPrinterTest(unittest.TestCase):
    def setUp(self):
        self.schema_parser = Mock()
        self.schema_parser.get_headers.return_value = ["EVENT_NAME", "EVENT_OCCURRED_ON", "EVENT_VERSION"]
        self.schema_parser.get_headers_csv.return_value = ["EVENT_NAME", "EVENT_OCCURRED_ON", "EVENT_VERSION"]
        self.event_decompressor = Mock()
        self.event_decompressor.get_processed_value.return_value = "2023,10"
        self.schema_parser.get_csv_parser_dic.return_value = {'EVENT_OCCURRED_ON': 0, 'EVENT_ID': 1, 'EVENT_VERSION': 2,
                                                              'EVENT_TIME': 3, 'EVENT_SOURCE': 4,
                                                              'CBSD_SERIAL_NUMBER': 5, 'CBSD_ID': 6,
                                                              'GROUP_ID': 7, 'REQUESTED_CHANNELS': 28,
                                                              'AVAILABLE_CHANNELS_TYPE': 29, 'RESPONSE_MAXEIRP': 30,
                                                              'RESPONSE_CODE': 62, 'RESPONSE_MESSAGE': 63,
                                                              'RESPONSE_DATA': 64}

    def test_that_can_sort_values_for_csv_output_1(self):
        schema_parser = Mock()
        schema_parser.get_headers.return_value = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'HEADER1', 'HEADER3', 'HEADER6']
        schema_parser.get_headers_csv.return_value = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'HEADER1', 'HEADER2',
                                                      'HEADER3', 'HEADER4', 'HEADER5', 'HEADER6']
        event_decompressor = Mock()
        event_decompressor.get_processed_value.return_value = "2023,10"
        schema_parser.get_csv_parser_dic.return_value = {'HEADER1': 0, 'HEADER2': 1, 'HEADER3': 2, 'HEADER4': 3,
                                                         'HEADER5': 4, 'HEADER6': 5}

        event_printer = CsvEventPrinter(schema_parser, event_decompressor, "@")
        values = ['VALUE1', 'VALUE3', 'VALUE6']
        event_version = "2023,10"
        result = event_printer.sort_values(values, event_version, None, None)
        expected_result = ['VALUE1', '', 'VALUE3', '', '', 'VALUE6']
        self.assertEqual(expected_result, result)

import json
import os
import sys
from unittest import TestCase

path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "python")
if path not in sys.path:
    sys.path.append(path)

from end_to_end_expected_results import ALL_2023_10, ALL_DDP_2023_10, DATE_TEST_DDP_2023_10, ONE_GROUP_2023, \
    ONE_GROUP_2023_10_DDP_CSV, ALL_2023_10_CSV, ALL_2023_15_CSV, ALL_2023_16_CSV, ALL_2024_04_CSV, ALL_2024_05_CSV
import cbrs_events
import sys
import io
import contextlib
from unittest.mock import patch
from freezegun import freeze_time


class TestEndToEnd(TestCase):

    def test_can_print_info_when_no_parameters(self):
        expected_result = """Usage: 
      cbrs_events all|"<group>" [--DDP <pathToDayLogs> ] [--separator] [separator default values is ',']
      cbrs_events all|"<group>" "<startTime>" "<endTime>" [--DDP <pathToDayLogs> ]  [--separator] [separator default values is ',']
      
      Date and Time Format:
        The date and time format for specific searches is YYYY-MM-DDThh:mm:ss in ISO8601 format with a specified UTC offset.
        For example, 2015-08-05T12:00:00-04:00 represents 12 PM with a UTC offset of -04:00, indicating Eastern Standard Time (EST, UTC-4).

    Warning: Maximum limit of events is 10000 for the non-DDP version.
    Warning: When searching DDP, there is a large amount of data to search; it may take 1 to 2 minutes to complete.
"""
        with io.StringIO() as buf:
            with contextlib.redirect_stdout(buf):
                sys.stdout = buf
                try:
                    cbrs_events.main()
                except:
                    pass
            self.assertEqual(buf.getvalue(), expected_result)

    def test_that_prints_info_when_wrong_parameters(self):
        expected_result = """Usage: 
      cbrs_events all|"<group>" [--DDP <pathToDayLogs> ] [--separator] [separator default values is ',']
      cbrs_events all|"<group>" "<startTime>" "<endTime>" [--DDP <pathToDayLogs> ]  [--separator] [separator default values is ',']
      
      Date and Time Format:
        The date and time format for specific searches is YYYY-MM-DDThh:mm:ss in ISO8601 format with a specified UTC offset.
        For example, 2015-08-05T12:00:00-04:00 represents 12 PM with a UTC offset of -04:00, indicating Eastern Standard Time (EST, UTC-4).

    Warning: Maximum limit of events is 10000 for the non-DDP version.
    Warning: When searching DDP, there is a large amount of data to search; it may take 1 to 2 minutes to complete.
"""
        original_argv = sys.argv
        test_args = ['any', 'any']

        try:
            sys.argv = [__file__] + test_args
            with io.StringIO() as buf:
                with contextlib.redirect_stdout(buf):
                    sys.stdout = buf
                    try:
                        cbrs_events.main()
                    except:
                        pass
                self.assertEqual(buf.getvalue(), expected_result)

        finally:
            sys.argv = original_argv

    @patch('requests.get')
    @freeze_time("2023-01-01 12:00:00", tz_offset=1)
    def test_that_prints_events_all_2024_05_CSV(self, mock_get):
        self.maxDiff = None
        response = self.create_mock_response_for_24_05()
        mock_get.return_value = response
        original_argv = sys.argv
        test_args = ['all', '--separator', '@']
        with io.StringIO() as buf:
            with contextlib.redirect_stdout(buf):
                try:
                    sys.argv = [__file__] + test_args
                    cbrs_events.main()
                finally:
                    sys.argv = original_argv
                result_string = buf.getvalue()
        for line, expected in zip(result_string.split("\n"), ALL_2024_05_CSV.split("\n")):
            self.assertEqual(line, expected)

    @patch('requests.get')
    @freeze_time("2023-01-01 12:00:00", tz_offset=1)
    def test_that_prints_events_all_2024_04_CSV(self, mock_get):
        self.maxDiff = None
        response = self.create_mock_response_for_24_04()
        mock_get.return_value = response
        original_argv = sys.argv
        test_args = ['all', '--separator', '@']
        with io.StringIO() as buf:
            with contextlib.redirect_stdout(buf):
                try:
                    sys.argv = [__file__] + test_args
                    cbrs_events.main()
                finally:
                    sys.argv = original_argv
                result_string = buf.getvalue()
        for line, expected in zip(result_string.split("\n"), ALL_2024_04_CSV.split("\n")):
            self.assertEqual(line, expected)

    @patch('requests.get')
    @freeze_time("2023-01-01 12:00:00", tz_offset=1)
    def test_that_prints_events_all_2023_16_CSV(self, mock_get):
        response = self.create_mock_response_for_16()
        mock_get.return_value = response
        original_argv = sys.argv
        test_args = ['all', '--separator', '@']
        with io.StringIO() as buf:
            with contextlib.redirect_stdout(buf):
                try:
                    sys.argv = [__file__] + test_args
                    cbrs_events.main()
                finally:
                    sys.argv = original_argv
                result_string = buf.getvalue()
        for line, expected in zip(result_string.split("\n"), ALL_2023_16_CSV.split("\n")):
            self.assertEqual(line, expected)
            
    @patch('requests.get')
    @freeze_time("2023-01-01 12:00:00", tz_offset=1)
    def test_that_prints_events_all_2023_15_CSV(self, mock_get):
        response = self.create_mock_response_for_15()
        mock_get.return_value = response
        original_argv = sys.argv
        test_args = ['all', '--separator', '@']
        with io.StringIO() as buf:
            with contextlib.redirect_stdout(buf):
                try:
                    sys.argv = [__file__] + test_args
                    cbrs_events.main()
                finally:
                    sys.argv = original_argv
                result_string = buf.getvalue()
        for line, expected in zip(result_string.split("\n"), ALL_2023_15_CSV.split("\n")):
            self.assertEqual(line, expected)

    @patch('requests.get')
    @freeze_time("2023-01-01 12:00:00", tz_offset=1)
    def test_that_prints_events_all_2023_10(self, mock_get):
        response = self.create_mock_response()
        mock_get.return_value = response
        original_argv = sys.argv
        test_args = ['all']
        with io.StringIO() as buf:
            with contextlib.redirect_stdout(buf):
                try:
                    sys.argv = [__file__] + test_args
                    cbrs_events.main()
                finally:
                    sys.argv = original_argv
                result_string = buf.getvalue()
        for line, expected in zip(result_string.split("\n"), ALL_2023_10.split("\n")):
            self.assertEqual(line, expected)

    @patch('requests.get')
    @freeze_time("2023-01-01 12:00:00", tz_offset=1)
    def test_that_prints_events_all_2023_10_CSV(self, mock_get):
        response = self.create_mock_response()
        mock_get.return_value = response
        original_argv = sys.argv
        test_args = ['all', '--separator', '@']
        with io.StringIO() as buf:
            with contextlib.redirect_stdout(buf):
                try:
                    sys.argv = [__file__] + test_args
                    cbrs_events.main()
                finally:
                    sys.argv = original_argv
                result_string = buf.getvalue()
        for line, expected in zip(result_string.split("\n"), ALL_2023_10_CSV.split("\n")):
            self.assertEqual(line, expected)

    @patch('requests.get')
    @freeze_time("2023-01-01 12:00:00", tz_offset=1)
    def test_that_prints_events_one_group_2023_10(self, mock_get):
        response = self.create_mock_response()
        mock_get.return_value = response
        original_argv = sys.argv
        test_args = ['NR03gNodeBRadio00002:1,2']
        with io.StringIO() as buf:
            with contextlib.redirect_stdout(buf):
                try:
                    sys.argv = [__file__] + test_args
                    cbrs_events.main()
                finally:
                    sys.argv = original_argv
                result_string = buf.getvalue()
        for line, expected in zip(result_string.split("\n"), ONE_GROUP_2023.split("\n")):
            self.assertEqual(line, expected)

    def create_mock_response(self):
        class Response:
            def __init__(self, data):
                self.data = data
            def json(self):
                return self.data
        with open("test_data/response.json", "r") as file:
            file_contents = file.read()
            parsed_data = json.loads(file_contents)
        response = Response(parsed_data)
        return response

    def create_mock_response_for_24_05(self):
        class Response:
            def __init__(self, data):
                self.data = data
            def json(self):
                return self.data
        with open("test_data/response_24_05.json", "r") as file:
            file_contents = file.read()
            parsed_data = json.loads(file_contents)
        response = Response(parsed_data)
        return response

    def create_mock_response_for_24_04(self):
        class Response:
            def __init__(self, data):
                self.data = data
            def json(self):
                return self.data
        with open("test_data/response_24_04.json", "r") as file:
            file_contents = file.read()
            parsed_data = json.loads(file_contents)
        response = Response(parsed_data)
        return response

    def create_mock_response_for_16(self):
        class Response:
            def __init__(self, data):
                self.data = data
            def json(self):
                return self.data
        with open("test_data/response_16.json", "r") as file:
            file_contents = file.read()
            parsed_data = json.loads(file_contents)
        response = Response(parsed_data)
        return response
    
    def create_mock_response_for_15(self):
        class Response:
            def __init__(self, data):
                self.data = data
            def json(self):
                return self.data
        with open("test_data/response_15.json", "r") as file:
            file_contents = file.read()
            parsed_data = json.loads(file_contents)
        response = Response(parsed_data)
        return response

    def test_that_prints_events_all_2023_10_DDP(self):
        original_argv = sys.argv
        test_args = ['all', '--DDP', 'test_logs_end_to_end']
        with io.StringIO() as buf:
            with contextlib.redirect_stdout(buf):
                try:
                    sys.argv = [__file__] + test_args
                    cbrs_events.main()
                finally:
                    sys.argv = original_argv
                result_string = buf.getvalue()
        for line, expected in zip(ALL_DDP_2023_10.split("\n"), result_string.split("\n")):
            self.assertEqual(line, expected)

    def test_that_prints_events_all_with_date_2023_10_DDP(self):
        original_argv = sys.argv
        test_args = ['all', '2023-09-01T01:07:47+01:00', '2023-09-01T01:07:48+01:00', '--DDP', 'test_logs_end_to_end']
        with io.StringIO() as buf:
            with contextlib.redirect_stdout(buf):
                try:
                    sys.argv = [__file__] + test_args
                    cbrs_events.main()
                finally:
                    sys.argv = original_argv
                result_string = buf.getvalue()
        for line, expected in zip(result_string.split("\n"), DATE_TEST_DDP_2023_10.split("\n")):
            self.assertEqual(line, expected)

    def test_that_prints_events_one_group_2023_10_DDP_CSV(self):
        with freeze_time("2023-01-01 12:00:00", tz_offset=1):
            original_argv = sys.argv
            test_args = [
                'Europe|Ireland|NETSimW|LTE09dg2ERBS00002:LTE09dg2ERBS00002-1,LTE09dg2ERBS00002-2,LTE09dg2ERBS00002-3,LTE09dg2ERBS00002-4,LTE09dg2ERBS00002-5,LTE09dg2ERBS00002-6',
                '--DDP', 'test_logs_end_to_end', '--separator']
            with io.StringIO() as buf:
                with contextlib.redirect_stdout(buf):
                    try:
                        sys.argv = [__file__] + test_args
                        cbrs_events.main()
                    finally:
                        sys.argv = original_argv
                    result_string = buf.getvalue()
            for line, expected in zip(result_string.split("\n"), ONE_GROUP_2023_10_DDP_CSV.split("\n")):
                self.assertEqual(line, expected)

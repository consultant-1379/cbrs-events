###########################################################################
# COPYRIGHT Ericsson 2023
#
# The copyright to the computer program(s) herein is the property of
# Ericsson Inc. The programs may be used and/or copied only with written
# permission from Ericsson Inc. or in accordance with the terms and
# conditions stipulated in the agreement/contract under which the
# program(s) have been supplied.
###########################################################################

import csv
import re
import datetime
import argparse
import time
import os
import logging
import sys
import gzip
import requests as requests


class DateParserHelper:
    """
    Provides utility methods for parsing and comparing dates.
    """

    def __init__(self):
        pass

    @staticmethod
    def generate_comparative_list(date_string):
        """
        Generate a list that can be used to compare the dates, it does not consider microseconds.
        :param date_string: A string representing a date with the format  YYYY-MM-DDThh:mm:ss+00:00 or YYYY-MM-DDThh:mm.ss.sss+00:00
        :return: List representation of the date with [year, month, day, hour, minute, second]
        """
        date, offset_symbol, hours, minutes = DateParserHelper.split_date(date_string)
        date_elements = DateParserHelper.extract_date_elements(date)
        multiplier = -1 if offset_symbol == "+" else 1
        total_minutes = multiplier * (int(hours) * 60 + int(minutes))
        # Adjusting hours
        date_elements[3] += total_minutes // 60
        # Adjusting minutes
        date_elements[4] += total_minutes % 60
        # Correcting for minutes out of bounds
        date_elements[3] += date_elements[4] // 60
        date_elements[4] %= 60
        # Correcting for hours out of bounds
        date_elements[2] += date_elements[3] // 24
        date_elements[3] %= 24
        return date_elements

    @staticmethod
    def extract_date_elements(date_str):
        """
        Extracts year, month, day, hour, minute, and second from a date string.
        :param date_str: A string representing a date with the format  YYYY-MM-DDThh:mm:ss
        :return:  List containing individual date elements as integers.
        """
        # Extracting year, month, day, hour, minute, second
        year = int(date_str[:4])
        month = int(date_str[5:7])
        day = int(date_str[8:10])
        hour = int(date_str[11:13])
        minute = int(date_str[14:16])
        second = int(date_str[17:19])
        return [year, month, day, hour, minute, second]

    @staticmethod
    def split_date(date_string):
        """
        Split date string into date, offset symbol, hours and  minutes
        :param date_string: A string representing a date with the format  YYYY-MM-DDThh:mm:ss+00:00 or YYYY-MM-DDThh:mm.ss.sss+00:00
        :return: Tuple containing individual date components.
        """
        if '+' in date_string:
            offset_symbol = '+'
        else:
            offset_symbol = '-'
        if '.' in date_string:
            date, rest = date_string.split('.')
            microseconds, offset = rest.split(offset_symbol)
        else:
            date, offset = date_string.rsplit(offset_symbol, 1)
        hours, minutes = offset.split(":")
        return date, offset_symbol, hours, minutes


class EventSchemaParser:
    """
    A class for parsing event schemas and retrieving the corresponding schema for a given version.
    """
    SCRIPT_PATH = os.path.dirname(os.path.abspath(__file__))
    SCHEMA_DIR = os.path.join(SCRIPT_PATH, 'resources', 'schemas')
    CSV_SCHEMA_DIR = os.path.join(SCRIPT_PATH, 'resources', 'schemas_csv_output')

    def __init__(self):
        self.version_to_correspondingSchema_dic = self._load_schemas()
        self.version_to_corresponding_csv_schema_dic, \
            self.value_to_position_in_csv_schema_dic = self._load_csv_schemas()

    def _load_schemas(self):
        """
        Load schema files from the specified directory and build the version-to-schema dictionary.
        """
        version_to_corresponding_schema_dic = {}
        for schema_file_name in os.listdir(self.SCHEMA_DIR):
            version = self._extract_version_from_file_name(schema_file_name)
            if version is None:
                continue
            file_path = os.path.join(self.SCHEMA_DIR, schema_file_name)
            event_name_to_headers_dic = {}
            if os.path.isfile(file_path):
                with open(file_path, 'r') as open_file:
                    csvreader = csv.reader(open_file)
                    for row in csvreader:
                        if row:
                            event_name_to_headers_dic[row[0]] = self._remove_empty_strings_at_the_end(row)
                version_to_corresponding_schema_dic[version] = event_name_to_headers_dic
        return version_to_corresponding_schema_dic

    def _load_csv_schemas(self):
        version_to_corresponding_csv_schema_dic = {}
        value_to_position_in_csv_schema_dic = {}
        for schema_file_name in os.listdir(self.CSV_SCHEMA_DIR):
            version = self._extract_version_from_file_name(schema_file_name)
            if version is None:
                continue
            file_path = os.path.join(self.CSV_SCHEMA_DIR, schema_file_name)
            if os.path.isfile(file_path):
                with open(file_path, 'r') as open_file:
                    csvreader = csv.reader(open_file)
                    headers_row = next(csvreader, None)
                    if headers_row:
                        version_to_corresponding_csv_schema_dic[version] = self._remove_empty_strings_at_the_end(
                            headers_row)
                        temp_dic = {}
                        headers_with_out_name_and_occurred_on = version_to_corresponding_csv_schema_dic[version][
                                                                2:]
                        for header in headers_with_out_name_and_occurred_on:
                            temp_dic[header] = headers_with_out_name_and_occurred_on.index(header.strip())
                        value_to_position_in_csv_schema_dic[version] = temp_dic
        return version_to_corresponding_csv_schema_dic, value_to_position_in_csv_schema_dic

    @staticmethod
    def _extract_version_from_file_name(file_name):
        """
         Extract the version from the given schema file name.
         Returns:
             str: The extracted version in the format 'year,sprint', or None if the file name is invalid.
         """
        parts = file_name[:-4].split("_")
        if len(parts) >= 2:
            value = "{},{}".format(parts[-2], parts[-1])
            return value
        else:
            logging.error("Invalid schema file name: %s", file_name)
            return None

    @staticmethod
    def _remove_empty_strings_at_the_end(row):
        """
          Remove empty strings from the end of a row.
          Args:
              row (list): The row to process.
          Returns:
              list: The row with empty strings removed from the end.
          """
        headers = row  # [1:]
        while headers and headers[-1] == "":
            headers.pop()
        return headers

    def get_schema(self, version):
        """
        Get the corresponding schema for the given version.
        Args:
            version (str): The version in the format 'year,sprint'.
        Returns:
            dict: A dictionary mapping event names to their corresponding headers (schema).
        Raises:
            KeyError: If the specified version is not found in the loaded schemas.
        """
        return self.version_to_correspondingSchema_dic[version]

    def get_headers(self, event_version, event_name):
        schema_dict = self.version_to_correspondingSchema_dic.get(event_version)
        if schema_dict is None:
            return "Warning: Version {} not found in schemas.".format(event_version)
        event_schema = schema_dict.get(event_name)
        if event_schema is None:
            return "Warning: Event {} not found in schema for version {}".format(event_name, event_version)
        return event_schema[1:]

    def get_schema_csv(self, version):
        return self.version_to_corresponding_csv_schema_dic.get(version)

    def get_headers_csv(self, event_version, event_name):
        schema_dict = self.version_to_correspondingSchema_dic.get(event_version)
        if schema_dict is None:
            return "Warning: Version {} not found in schemas.".format(event_version)
        event_schema = schema_dict.get(event_name)
        if event_schema is None:
            return "Warning: Event {} not found in schema for version {}".format(event_name, event_version)

        schema_dict_csv = self.version_to_corresponding_csv_schema_dic.get(event_version)
        if schema_dict_csv is None:
            return "Warning: Version {} not found in schemas.".format(event_version)
        return schema_dict_csv

    def get_csv_parser_dic(self, event_version):
        return self.value_to_position_in_csv_schema_dic[event_version]


# pepe remove
class ArgumentParser:
    """
    Parse the user input.
    """
    HELP = """Usage: 
      cbrs_events all|"<group>" [--DDP <pathToDayLogs> ] [--separator] [separator default values is ',']
      cbrs_events all|"<group>" "<startTime>" "<endTime>" [--DDP <pathToDayLogs> ]  [--separator] [separator default values is ',']
      
      Date and Time Format:
        The date and time format for specific searches is YYYY-MM-DDThh:mm:ss in ISO8601 format with a specified UTC offset.
        For example, 2015-08-05T12:00:00-04:00 represents 12 PM with a UTC offset of -04:00, indicating Eastern Standard Time (EST, UTC-4).

    Warning: Maximum limit of events is 10000 for the non-DDP version.
    Warning: When searching DDP, there is a large amount of data to search; it may take 1 to 2 minutes to complete."""

    OFFSET_CHARACTERS_COUNT = 6
    DATE_WITH_OFFSET_MIN_LENGHT = 19
    ALL = "all"
    MINUS = '-'
    PLUS = '+'
    START_OFFSET_MINUTES = 2
    END_OFFSET_HOUR = 3
    START_OFFSET_HOUR = 5
    INPUT_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"

    def __init__(self):
        self.parser = self.ArgumentsParserWitCustomErrorMessage(description=self.HELP)
        self.parser.add_argument('param', nargs='*', help=self.HELP)
        self.parser.add_argument('-s', '--separator', nargs='?', const=',', help=self.HELP)
        self.parser.add_argument('-d', '--DDP', nargs='?', const='.', help=self.HELP)

    def parse_args(self):
        args = self.parser.parse_args()
        group_id, starting_date, end_data = self.process_arguments(args.param)
        return group_id, starting_date, end_data, args.separator, args.DDP

    def process_arguments(self, input_params):
        if len(input_params) not in (1, 3):
            self.parser.error("")
        self.process_input(input_params)
        if len(input_params) == 1:
            return input_params[0], None, None
        if self._is_valid_date(input_params[1]) is False:
            self.parser.error("")
        if len(input_params) == 3:
            return input_params[0], input_params[1].strip(), input_params[2].strip()

    def process_input(self, input_params):
        if len(input_params[0]) == 1 or not self.contains_letter(input_params[0]):
            self.parser.error("")
        if input_params[0] == self.ALL:
            input_params[0] = "*"

    def contains_letter(self, input_str):
        return any(char.isalpha() for char in input_str)

    def _is_valid_date(self, date_string):
        try:
            if len(date_string) > self.DATE_WITH_OFFSET_MIN_LENGHT:
                dt = datetime.datetime.strptime(date_string[:-self.OFFSET_CHARACTERS_COUNT], self.INPUT_DATE_FORMAT)
                offset_hours = int(date_string[-self.START_OFFSET_HOUR:-self.END_OFFSET_HOUR])
                offset_minutes = int(date_string[-self.START_OFFSET_MINUTES:])
                offset = datetime.timedelta(hours=offset_hours, minutes=offset_minutes)
                if date_string[-self.OFFSET_CHARACTERS_COUNT] == self.PLUS:
                    dt = dt - offset
                elif date_string[-self.OFFSET_CHARACTERS_COUNT] == self.MINUS:
                    dt = dt + offset
            else:
                dt = datetime.datetime.strptime(date_string, self.INPUT_DATE_FORMAT)
            return True

        except ValueError:
            return False

    class ArgumentsParserWitCustomErrorMessage(argparse.ArgumentParser):
        def error(self, message):
            self._print_help_and_exit()

        def print_help(self):
            self._print_help_and_exit()

        @staticmethod
        def _print_help_and_exit():
            sys.stdout.write(ArgumentParser.HELP)
            sys.stdout.write("\n")
            sys.exit(-2)


class HttpRequester:
    """
    Class to make http requests.
    """
    BASE_URL = 'http://elasticsearch:9200/enm_logs*/_search'
    # The host is removed from the query because elastic search limitations
    # This should be re-added when the problem is fixed
    # Original queries:
    # BASE_QUERY_1 = 'host:"*dpmediation*" AND program:"JBOSS" AND message:"event-information" AND message:";{0};" '
    # BASE_QUERY_2 = 'host:"*dpmediation*" AND program:"JBOSS" AND message:"event-information" AND message:";{0};" AND ' \
    #                'timestamp:["{1}" TO "{2}"]'
    BASE_QUERY_NO_DATE = 'program:"JBOSS" AND message:"event-information" AND message:";{0};" '
    BASE_QUERY_WITH_DATE = 'program:"JBOSS" AND message:"event-information" AND message:";{0};" AND ' \
                           'timestamp:["{1}" TO "{2}"]'

    def __init__(self):
        self.params = {
            'size': '10000',
            'sort': 'timestamp:desc'
        }

    def get_events(self, group_id, starting_date, end_date):
        if starting_date is None:
            self.params['q'] = HttpRequester.BASE_QUERY_NO_DATE.format(
                self.trim_the_groupid_to_networkelement_substring(group_id))
        else:
            self.params['q'] = HttpRequester.BASE_QUERY_WITH_DATE.format(
                self.trim_the_groupid_to_networkelement_substring(group_id),
                starting_date, end_date)
        return requests.get(HttpRequester.BASE_URL, params=self.params)

    @staticmethod
    def trim_the_groupid_to_networkelement_substring(input_string):
        parts = input_string.rsplit(':', 1)
        if len(parts) == 2:
            return parts[0] + ':'
        else:
            return input_string


class BaseEventIterator:
    EVENT_INFORMATION_PATTERN = r'"event-information":"([^"]+)"'
    EVENT_NAME_PATTERN = r'EV_(\w+)'
    HOST_PATTERN = r'\'host\':\'([^"]+)\''

    def __init__(self):
        pass

    def __iter__(self):
        return self

    def __next__(self):
        pass

    def next(self):
        return self.__next__()


class ElasticSearchIterator(BaseEventIterator):
    """
    Class used to iterate over the response from elastic search
    """

    def __init__(self, response, group_id=None):
        BaseEventIterator.__init__(self)
        self.events = response.json()["hits"]["hits"]
        self.index = len(self.events) - 1
        self.group_id = group_id
        self.total_events = self.get_total_events_in_request(response)

    @staticmethod
    def get_total_events_in_request(response):
        try:
            return int(response.json()['hits']['total'])
        except (ValueError, KeyError, TypeError):
            pass
        try:
            return int(response.json()['hits']['total']['value'])
        except (ValueError, KeyError, TypeError):
            pass
        ConsoleUtilities.print_line_to_stdout("Warning: Can't extract total events from response.")
        return 0

    def __iter__(self):
        return self

    def __next__(self):
        while self.index >= 0:
            event_message = self.events[self.index]['_source']['message']
            event_information = re.search(self.EVENT_INFORMATION_PATTERN, event_message).group(1)
            event_name = re.search(self.EVENT_NAME_PATTERN, event_message).group()
            event_occurred_on = self.events[self.index]['_source']['host']
            if event_information and event_name:
                if self.group_id == "*" or self.group_id in event_information:
                    self.index -= 1
                    return event_name, event_occurred_on, event_information.split(';')
                else:
                    self.index -= 1
            else:
                sys.stderr.write("ERROR - No event information found in {}".format(self.events[self.index]))
                self.index -= 1

        raise StopIteration

    def next(self):
        return self.__next__()


class DDPIterator(BaseEventIterator):
    """
    Class used to iterate through ddp logs
    """

    def __init__(self, path, group_id=None, start_date=None, end_date=None):
        """
        Create an iterator to extract events from a DDP folder.
        :param path: path to folder with logs files
        :param group_id: group id
        :param start_date: starting date
        :param end_date:  end date
        """
        BaseEventIterator.__init__(self)
        if path == ".":
            self.path = os.getcwd()
        else:
            self.path = path
        self.log_files = self.filter_log_files(path)
        self.current_file_idx = 0
        self.current_gz_file = None
        self.lines = None
        self.group_id = group_id
        self.line_index = 0
        self.start_date_list_representation = None
        self.end_date_list_representation = None
        if start_date is not None:
            self.start_date_list_representation = DateParserHelper.generate_comparative_list(start_date)
            self.end_date_list_representation = DateParserHelper.generate_comparative_list(end_date)
        self.hasPrintSearchingPrompt = False

    def __iter__(self):
        return self

    def __next__(self):
        while self.current_file_idx < len(self.log_files):
            self._print_searching_for_event()
            if self.current_gz_file is None:
                current_log_file = self.log_files[self.current_file_idx]
                gz_file_path = os.path.join(self.path, current_log_file)
                self.current_gz_file = gzip.open(gz_file_path, 'rt')
            line = self.current_gz_file.readline()
            if line:
                try:
                    split_data = line.split('@')
                    date_string = split_data[0]
                    host = split_data[1]
                except IndexError:
                    continue
                if self.start_date_list_representation is not None:
                    date_list_representation = DateParserHelper.generate_comparative_list(date_string)
                    if date_list_representation < self.start_date_list_representation:
                        continue
                    if date_list_representation > self.end_date_list_representation:
                        raise StopIteration()
                event_information_match = re.search(self.EVENT_INFORMATION_PATTERN, line)
                if event_information_match is not None:
                    event_information = event_information_match.group(1)
                    if self.group_id == "*" or self.group_id in event_information:
                        self._clear_searching_for_events()
                        event_name_search_result = re.search(self.EVENT_NAME_PATTERN, line)
                        if event_name_search_result is not None:
                            event_name = event_name_search_result.group()
                            return event_name, host, event_information.split(";")
                        else:
                            continue
                else:
                    continue
            else:
                self.current_gz_file.close()
                self.current_gz_file = None
                self.current_file_idx += 1
        raise StopIteration()

    def _clear_searching_for_events(self):
        if self.hasPrintSearchingPrompt:
            ConsoleUtilities.clear_stderr()
            self.hasPrintSearchingPrompt = False

    def _print_searching_for_event(self):
        if self.hasPrintSearchingPrompt is False:
            ConsoleUtilities.print_to_stderr("Searching for events.")
            self.hasPrintSearchingPrompt = True

    def next(self):
        return self.__next__()

    @staticmethod
    def filter_log_files(directory_path):
        all_files = os.listdir(directory_path)
        log_files = [file for file in all_files if file.endswith('.csv.gz')]
        return log_files

    def get_total_file_size(self):
        file_paths = [os.path.join(self.path, log_file) for log_file in self.log_files]
        return sum(os.path.getsize(path) for path in file_paths)


class EventDecompressor:
    """
    Decompress the values of the compressed events.
    """

    VERSIONS = {"2023,5", "2023,10", "2023,15", "2023,16", "2024,04", "2024,05"}
    VERSION_TO_COMPRESSED_HEADERS_DIC = {
        "2023,5": {
            "TIME_HEADERS_SET": {"EVENT_TIME"},
            "TIME_HEADERS_UTC": {"TRANSMIT_EXPIRE_TIME", "GRANT_EXPIRE_TIME"},
            "MEAS_ARRAY_FIELDS": {},
            "RADIO_TECHNOLOGY_FIELDS": {},
            "CHANNEL_TYPE_FIELDS": {"CHANNEL_TYPE"},
            "MAXEIRP_FIELDS": {"RESPONSE_MAXEIRP"},
            "FIELDS_WITH_EMPTY_STRING_ARRAY": {"ADDITIONAL_CBSD_ID", "ADDITIONAL_CBSD_SERIAL_NUMBERS"},
            "CEll_PRIORITY_FIELDS": {},
            "ARRAY_CHANNEL_FIELDS": {},
            "CELL_ID_FIELDS": {},
            "BANDWIDTH_FIELDS": {},
            "FIELDS_WITH_DEFAULT_INT_MIN_1": {},
        },
        "2023,10": {
            "TIME_HEADERS_SET": {"EVENT_TIME", "TRANSMIT_EXPIRE_TIME",
                                 "GRANT_EXPIRE_TIME"},
            "TIME_HEADERS_UTC": {"TRANSMIT_EXPIRE_TIME", "GRANT_EXPIRE_TIME"},
            "MEAS_ARRAY_FIELDS": {"MEAS_CAPABILITY_AVAILABLE", "MEAS_REPORT_REQUESTED", "MEAS_REQUESTED",
                                  "MEAS_CAPABILITY", "MEASUREMENT_REPORT_REQUESTED", "MEAS_REPORT",
                                  "MEAS_REPORT_REQUESTED"},
            "RADIO_TECHNOLOGY_FIELDS": {"RADIO_TECHNOLOGY"},
            "CHANNEL_TYPE_FIELDS": {"CHANNEL_TYPE", "AVAILABLE_CHANNELS_TYPE"},
            "MAXEIRP_FIELDS": {"RESPONSE_MAXEIRP"},
            "FIELDS_WITH_EMPTY_STRING_ARRAY": {"ADDITIONAL_CBSD_ID", "ADDITIONAL_CBSD_SERIAL_NUMBERS"},
            "CEll_PRIORITY_FIELDS": {},
            "ARRAY_CHANNEL_FIELDS": {},
            "CELL_ID_FIELDS": {},
            "BANDWIDTH_FIELDS": {},
            "FIELDS_WITH_DEFAULT_INT_MIN_1": {},
        },
        "2023,15": {
            "TIME_HEADERS_SET": {"EVENT_TIME", "TRANSMIT_EXPIRE_TIME",
                                 "GRANT_EXPIRE_TIME"},
            "TIME_HEADERS_UTC": {"TRANSMIT_EXPIRE_TIME", "GRANT_EXPIRE_TIME"},
            "MEAS_ARRAY_FIELDS": {"MEAS_CAPABILITY_AVAILABLE", "MEAS_REPORT_REQUESTED", "MEAS_REQUESTED",
                                  "MEAS_CAPABILITY", "MEASUREMENT_REPORT_REQUESTED", "MEAS_REPORT",
                                  "MEAS_REPORT_REQUESTED"},
            "RADIO_TECHNOLOGY_FIELDS": {"RADIO_TECHNOLOGY", "CELL_1_RAT", "CELL_2_RAT", "CELL_3_RAT", "CELL_4_RAT",
                                        "CELL_5_RAT", "CELL_6_RAT"},
            "CHANNEL_TYPE_FIELDS": {"CHANNEL_TYPE", "AVAILABLE_CHANNELS_TYPE"},
            "MAXEIRP_FIELDS": {"RESPONSE_MAXEIRP", "AUTHORIZED_MAX_EIRP", "CELL_1_ACTIVE_MAX_EIRP",
                               "CELL_2_ACTIVE_MAX_EIRP", "CELL_3_ACTIVE_MAX_EIRP", "CELL_4_ACTIVE_MAX_EIRP",
                               "CELL_5_ACTIVE_MAX_EIRP", "CELL_6_ACTIVE_MAX_EIRP", "CELL_1_ASSIGNED_MAX_EIRP",
                               "CELL_2_ASSIGNED_MAX_EIRP", "CELL_3_ASSIGNED_MAX_EIRP", "CELL_4_ASSIGNED_MAX_EIRP",
                               "CELL_5_ASSIGNED_MAX_EIRP", "CELL_6_ASSIGNED_MAX_EIRP"},
            "CEll_PRIORITY_FIELDS": {"CELL_1_PRIORITY", "CELL_2_PRIORITY", "CELL_3_PRIORITY", "CELL_4_PRIORITY",
                                     "CELL_5_PRIORITY", "CELL_6_PRIORITY"},
            "FIELDS_WITH_EMPTY_STRING_ARRAY": {"ADDITIONAL_CBSD_ID", "ADDITIONAL_CBSD_SERIAL_NUMBERS"},
            "ARRAY_CHANNEL_FIELDS": {"CELL_1_ACTIVE_CHANNELS", "CELL_2_ACTIVE_CHANNELS",
                                     "CELL_3_ACTIVE_CHANNELS",
                                     "CELL_4_ACTIVE_CHANNELS", "CELL_5_ACTIVE_CHANNELS",
                                     "CELL_6_ACTIVE_CHANNELS", "CELL_1_ASSIGNED_CHANNELS",
                                     "CELL_2_ASSIGNED_CHANNELS", "CELL_3_ASSIGNED_CHANNELS",
                                     "CELL_4_ASSIGNED_CHANNELS", "CELL_5_ASSIGNED_CHANNELS",
                                     "CELL_6_ASSIGNED_CHANNELS"},
            "CELL_ID_FIELDS": {},
            "BANDWIDTH_FIELDS": {},
            "FIELDS_WITH_DEFAULT_INT_MIN_1": {},
        },
        "2023,16": {
            "TIME_HEADERS_SET": {"EVENT_TIME", "TRANSMIT_EXPIRE_TIME",
                                 "GRANT_EXPIRE_TIME"},
            "TIME_HEADERS_UTC": {"TRANSMIT_EXPIRE_TIME", "GRANT_EXPIRE_TIME"},
            "MEAS_ARRAY_FIELDS": {"MEAS_CAPABILITY_AVAILABLE", "MEAS_REPORT_REQUESTED", "MEAS_REQUESTED",
                                  "MEAS_CAPABILITY", "MEASUREMENT_REPORT_REQUESTED", "MEAS_REPORT",
                                  "MEAS_REPORT_REQUESTED"},
            "RADIO_TECHNOLOGY_FIELDS": {"RADIO_TECHNOLOGY", "CELL_1_RAT", "CELL_2_RAT", "CELL_3_RAT", "CELL_4_RAT",
                                        "CELL_5_RAT", "CELL_6_RAT"},
            "CHANNEL_TYPE_FIELDS": {"CHANNEL_TYPE", "AVAILABLE_CHANNELS_TYPE"},
            "MAXEIRP_FIELDS": {"RESPONSE_MAXEIRP", "AUTHORIZED_MAX_EIRP", "CELL_1_ACTIVE_MAX_EIRP",
                               "CELL_2_ACTIVE_MAX_EIRP", "CELL_3_ACTIVE_MAX_EIRP", "CELL_4_ACTIVE_MAX_EIRP",
                               "CELL_5_ACTIVE_MAX_EIRP", "CELL_6_ACTIVE_MAX_EIRP", "CELL_1_ASSIGNED_MAX_EIRP",
                               "CELL_2_ASSIGNED_MAX_EIRP", "CELL_3_ASSIGNED_MAX_EIRP", "CELL_4_ASSIGNED_MAX_EIRP",
                               "CELL_5_ASSIGNED_MAX_EIRP", "CELL_6_ASSIGNED_MAX_EIRP"},
            "CEll_PRIORITY_FIELDS": {"CELL_1_PRIORITY", "CELL_2_PRIORITY", "CELL_3_PRIORITY", "CELL_4_PRIORITY",
                                     "CELL_5_PRIORITY", "CELL_6_PRIORITY"},
            "FIELDS_WITH_EMPTY_STRING_ARRAY": {"ADDITIONAL_CBSD_ID", "ADDITIONAL_CBSD_SERIAL_NUMBERS"},
            "ARRAY_CHANNEL_FIELDS": {"AUTHORIZED_CHANNELS", "CELL_1_ACTIVE_CHANNELS", "CELL_2_ACTIVE_CHANNELS",
                                     "CELL_3_ACTIVE_CHANNELS",
                                     "CELL_4_ACTIVE_CHANNELS", "CELL_5_ACTIVE_CHANNELS",
                                     "CELL_6_ACTIVE_CHANNELS", "CELL_1_ASSIGNED_CHANNELS",
                                     "CELL_2_ASSIGNED_CHANNELS", "CELL_3_ASSIGNED_CHANNELS",
                                     "CELL_4_ASSIGNED_CHANNELS", "CELL_5_ASSIGNED_CHANNELS",
                                     "CELL_6_ASSIGNED_CHANNELS"},
            "CELL_ID_FIELDS": {"CELL_1_ID", "CELL_2_ID", "CELL_3_ID", "CELL_4_ID", "CELL_5_ID", "CELL_6_ID"},
            "BANDWIDTH_FIELDS": {"CELL_1_PREFERRED_BANDWIDTH", "CELL_2_PREFERRED_BANDWIDTH",
                                 "CELL_3_PREFERRED_BANDWIDTH",
                                 "CELL_4_PREFERRED_BANDWIDTH", "CELL_5_PREFERRED_BANDWIDTH",
                                 "CELL_6_PREFERRED_BANDWIDTH"},
            "FIELDS_WITH_DEFAULT_INT_MIN_1": {"CELL_1_RANK", "CELL_2_RANK", "CELL_3_RANK", "CELL_4_RANK",
                                              "CELL_5_RANK", "CELL_6_RANK"},

        },
        "2024,04": {
            "TIME_HEADERS_SET": {"EVENT_TIME", "TRANSMIT_EXPIRE_TIME",
                                 "GRANT_EXPIRE_TIME"},
            "TIME_HEADERS_UTC": {"TRANSMIT_EXPIRE_TIME", "GRANT_EXPIRE_TIME"},
            "MEAS_ARRAY_FIELDS": {"MEAS_CAPABILITY_AVAILABLE", "MEAS_REPORT_REQUESTED", "MEAS_REQUESTED",
                                  "MEAS_CAPABILITY", "MEASUREMENT_REPORT_REQUESTED", "MEAS_REPORT",
                                  "MEAS_REPORT_REQUESTED"},
            "RADIO_TECHNOLOGY_FIELDS": {"RADIO_TECHNOLOGY", "CELL_1_RAT", "CELL_2_RAT", "CELL_3_RAT", "CELL_4_RAT",
                                        "CELL_5_RAT", "CELL_6_RAT"},
            "CHANNEL_TYPE_FIELDS": {"CHANNEL_TYPE", "AVAILABLE_CHANNELS_TYPE"},
            "MAXEIRP_FIELDS": {"RESPONSE_MAXEIRP", "AUTHORIZED_MAX_EIRP", "CELL_1_ACTIVE_MAX_EIRP",
                               "CELL_2_ACTIVE_MAX_EIRP", "CELL_3_ACTIVE_MAX_EIRP", "CELL_4_ACTIVE_MAX_EIRP",
                               "CELL_5_ACTIVE_MAX_EIRP", "CELL_6_ACTIVE_MAX_EIRP", "CELL_1_ASSIGNED_MAX_EIRP",
                               "CELL_2_ASSIGNED_MAX_EIRP", "CELL_3_ASSIGNED_MAX_EIRP", "CELL_4_ASSIGNED_MAX_EIRP",
                               "CELL_5_ASSIGNED_MAX_EIRP", "CELL_6_ASSIGNED_MAX_EIRP"},
            "CEll_PRIORITY_FIELDS": {"CELL_1_PRIORITY", "CELL_2_PRIORITY", "CELL_3_PRIORITY", "CELL_4_PRIORITY",
                                     "CELL_5_PRIORITY", "CELL_6_PRIORITY"},
            "FIELDS_WITH_EMPTY_STRING_ARRAY": {"ADDITIONAL_CBSD_ID", "ADDITIONAL_CBSD_SERIAL_NUMBERS"},
            "ARRAY_CHANNEL_FIELDS": {"AUTHORIZED_CHANNELS", "CELL_1_ACTIVE_CHANNELS", "CELL_2_ACTIVE_CHANNELS",
                                     "CELL_3_ACTIVE_CHANNELS",
                                     "CELL_4_ACTIVE_CHANNELS", "CELL_5_ACTIVE_CHANNELS",
                                     "CELL_6_ACTIVE_CHANNELS", "CELL_1_ASSIGNED_CHANNELS",
                                     "CELL_2_ASSIGNED_CHANNELS", "CELL_3_ASSIGNED_CHANNELS",
                                     "CELL_4_ASSIGNED_CHANNELS", "CELL_5_ASSIGNED_CHANNELS",
                                     "CELL_6_ASSIGNED_CHANNELS"},
            "CELL_ID_FIELDS": {"CELL_1_ID", "CELL_2_ID", "CELL_3_ID", "CELL_4_ID", "CELL_5_ID", "CELL_6_ID"},
            "BANDWIDTH_FIELDS": {"CELL_1_PREFERRED_BANDWIDTH", "CELL_2_PREFERRED_BANDWIDTH",
                                 "CELL_3_PREFERRED_BANDWIDTH",
                                 "CELL_4_PREFERRED_BANDWIDTH", "CELL_5_PREFERRED_BANDWIDTH",
                                 "CELL_6_PREFERRED_BANDWIDTH"},
            "FIELDS_WITH_DEFAULT_INT_MIN_1": {"CELL_1_RANK", "CELL_2_RANK", "CELL_3_RANK", "CELL_4_RANK",
                                              "CELL_5_RANK", "CELL_6_RANK"}
        },
        "2024,05": {
                    "TIME_HEADERS_SET": {"EVENT_TIME", "TRANSMIT_EXPIRE_TIME",
                                         "GRANT_EXPIRE_TIME"},
                    "TIME_HEADERS_UTC": {"TRANSMIT_EXPIRE_TIME", "GRANT_EXPIRE_TIME"},
                    "MEAS_ARRAY_FIELDS": {"MEAS_CAPABILITY_AVAILABLE", "MEAS_REPORT_REQUESTED", "MEAS_REQUESTED",
                                          "MEAS_CAPABILITY", "MEASUREMENT_REPORT_REQUESTED", "MEAS_REPORT",
                                          "MEAS_REPORT_REQUESTED"},
                    "RADIO_TECHNOLOGY_FIELDS": {"RADIO_TECHNOLOGY", "CELL_1_RAT", "CELL_2_RAT", "CELL_3_RAT", "CELL_4_RAT",
                                                "CELL_5_RAT", "CELL_6_RAT"},
                    "CHANNEL_TYPE_FIELDS": {"CHANNEL_TYPE", "RESPONSE_CHANNEL_TYPE", "AVAILABLE_CHANNELS_TYPE"},
                    "MAXEIRP_FIELDS": {"RESPONSE_MAXEIRP", "AUTHORIZED_MAX_EIRP", "CELL_1_ACTIVE_MAX_EIRP",
                                       "CELL_2_ACTIVE_MAX_EIRP", "CELL_3_ACTIVE_MAX_EIRP", "CELL_4_ACTIVE_MAX_EIRP",
                                       "CELL_5_ACTIVE_MAX_EIRP", "CELL_6_ACTIVE_MAX_EIRP", "CELL_1_ASSIGNED_MAX_EIRP",
                                       "CELL_2_ASSIGNED_MAX_EIRP", "CELL_3_ASSIGNED_MAX_EIRP", "CELL_4_ASSIGNED_MAX_EIRP",
                                       "CELL_5_ASSIGNED_MAX_EIRP", "CELL_6_ASSIGNED_MAX_EIRP"},
                    "CEll_PRIORITY_FIELDS": {"CELL_1_PRIORITY", "CELL_2_PRIORITY", "CELL_3_PRIORITY", "CELL_4_PRIORITY",
                                             "CELL_5_PRIORITY", "CELL_6_PRIORITY"},
                    "FIELDS_WITH_EMPTY_STRING_ARRAY": {"ADDITIONAL_CBSD_ID", "ADDITIONAL_CBSD_SERIAL_NUMBERS"},
                    "ARRAY_CHANNEL_FIELDS": {"AUTHORIZED_CHANNELS", "CELL_1_ACTIVE_CHANNELS", "CELL_2_ACTIVE_CHANNELS",
                                             "CELL_3_ACTIVE_CHANNELS",
                                             "CELL_4_ACTIVE_CHANNELS", "CELL_5_ACTIVE_CHANNELS",
                                             "CELL_6_ACTIVE_CHANNELS", "CELL_1_ASSIGNED_CHANNELS",
                                             "CELL_2_ASSIGNED_CHANNELS", "CELL_3_ASSIGNED_CHANNELS",
                                             "CELL_4_ASSIGNED_CHANNELS", "CELL_5_ASSIGNED_CHANNELS",
                                             "CELL_6_ASSIGNED_CHANNELS"},
                    "CELL_ID_FIELDS": {"CELL_1_ID", "CELL_2_ID", "CELL_3_ID", "CELL_4_ID", "CELL_5_ID", "CELL_6_ID"},
                    "BANDWIDTH_FIELDS": {"CELL_1_PREFERRED_BANDWIDTH", "CELL_2_PREFERRED_BANDWIDTH",
                                         "CELL_3_PREFERRED_BANDWIDTH",
                                         "CELL_4_PREFERRED_BANDWIDTH", "CELL_5_PREFERRED_BANDWIDTH",
                                         "CELL_6_PREFERRED_BANDWIDTH"},
                    "FIELDS_WITH_DEFAULT_INT_MIN_1": {"CELL_1_RANK", "CELL_2_RANK", "CELL_3_RANK", "CELL_4_RANK",
                                                      "CELL_5_RANK", "CELL_6_RANK"}
        }
    }

    MEAS_VALUES_DIC = {"[0,0]": "", "[1,0]": "[RECEIVED_POWER_WITHOUT_GRANT]", "[0,2]": "[RECEIVED_POWER_WITH_GRANT]",
                       "[1,2]": "[RECEIVED_POWER_WITHOUT_GRANT,RECEIVED_POWER_WITH_GRANT]"}

    INT_TO_RADIO_TECHNOLOGY_DIC = {-1: "NOT_PRESENT", 1: "E_UTRA", 2: "NR"}

    INT_TO_CHANNEL_TYPE_DIC = {-1: "NOT_PRESENT", 1: "GAA", 2: "PAL"}

    INT_TO_CELL_PRIORITY = {-1: "NOT_PRESENT", 1: "NORMAL", 2: "HIGH"}

    VALUE_NOT_PRESENT = "NOT_PRESENT"

    def __init__(self, reference_date_to_calculate_offsets_ddp=None):
        offset_seconds1, offset_string2 = self.extract_offset_seconds(reference_date_to_calculate_offsets_ddp)
        self.offset_seconds = offset_seconds1
        self.offset_string = offset_string2
        self.method_mapper = {}
        self.init_method_mapper()

    def extract_offset_seconds(self, date_string):
        if date_string is None:
            return None, None
        offset_string = date_string[-6:]
        if offset_string[0] in ('+', '-'):
            sign = -1 if offset_string[0] == '-' else 1
            hours = int(offset_string[1:3])
            minutes = int(offset_string[4:6])
            offset_seconds = sign * ((hours * 3600) + (minutes * 60))
            return offset_seconds, offset_string
        else:
            return None, None

    def init_method_mapper(self):
        for version in self.VERSIONS:
            if self.offset_seconds is None:
                version_methods = {
                    header_field: self._format_date for header_field in
                    self.VERSION_TO_COMPRESSED_HEADERS_DIC[version]["TIME_HEADERS_SET"]
                }
            else:
                version_methods = {
                    header_field: self._format_date_with_custom_offset for header_field in
                    self.VERSION_TO_COMPRESSED_HEADERS_DIC[version]["TIME_HEADERS_SET"]
                }
            if self.offset_seconds is None:
                version_methods.update({
                    header_field: self._format_date_to_utc for header_field in
                    self.VERSION_TO_COMPRESSED_HEADERS_DIC[version]["TIME_HEADERS_UTC"]
                })
            else:
                version_methods.update({
                    header_field: self._format_date_with_custom_offset_utc for header_field in
                    self.VERSION_TO_COMPRESSED_HEADERS_DIC[version]["TIME_HEADERS_UTC"]
                })
            version_methods.update({
                header_field: self.decompress_meas for header_field in
                self.VERSION_TO_COMPRESSED_HEADERS_DIC[version]["MEAS_ARRAY_FIELDS"]
            })
            version_methods.update({
                header_field: self._decompress_radio_technology for header_field in
                self.VERSION_TO_COMPRESSED_HEADERS_DIC[version]["RADIO_TECHNOLOGY_FIELDS"]
            })
            version_methods.update({
                header_field: self._decompress_channel_type for header_field in
                self.VERSION_TO_COMPRESSED_HEADERS_DIC[version]["CHANNEL_TYPE_FIELDS"]
            })
            version_methods.update({
                header_field: self._decompress_maxeirp for header_field in
                self.VERSION_TO_COMPRESSED_HEADERS_DIC[version]["MAXEIRP_FIELDS"]
            })
            version_methods.update({
                header_field: self._decompress_cell_priority for header_field in
                self.VERSION_TO_COMPRESSED_HEADERS_DIC[version]["CEll_PRIORITY_FIELDS"]
            })
            version_methods.update({
                header_field: self._replace_empty_string_arrays for header_field in
                self.VERSION_TO_COMPRESSED_HEADERS_DIC[version]["FIELDS_WITH_EMPTY_STRING_ARRAY"]
            })
            version_methods.update({
                header_field: self._replace_empty_int_arrays for header_field in
                self.VERSION_TO_COMPRESSED_HEADERS_DIC[version]["ARRAY_CHANNEL_FIELDS"]
            })
            version_methods.update({
                header_field: self._replace_default_bandwidth for header_field in
                self.VERSION_TO_COMPRESSED_HEADERS_DIC[version]["BANDWIDTH_FIELDS"]
            })
            version_methods.update({
                header_field: self._replace_default_int_minus_1 for header_field in
                self.VERSION_TO_COMPRESSED_HEADERS_DIC[version]["FIELDS_WITH_DEFAULT_INT_MIN_1"]
            })
            version_methods.update({
                header_field: self._replace_default_string for header_field in
                self.VERSION_TO_COMPRESSED_HEADERS_DIC[version]["CELL_ID_FIELDS"]
            })

            self.method_mapper[version] = version_methods

    def _format_date(self, field_value):
        if field_value == "0" or field_value == "":
            return self.VALUE_NOT_PRESENT
        datetime_obj = datetime.datetime.fromtimestamp(int(field_value) / 1000)
        date_time_actual = datetime.datetime.now().replace(second=0, microsecond=0)
        date_time_utc = datetime.datetime.utcnow().replace(second=0, microsecond=0)
        off_set_hours = date_time_actual.hour - date_time_utc.hour
        if date_time_actual.day != date_time_utc.day:
            off_set_hours = off_set_hours - 24
        if off_set_hours < 0:
            offset_sign = "-"
        else:
            offset_sign = "+"
        value = datetime_obj.strftime("%Y-%m-%dT%H:%M:%S%z") + offset_sign + "{:02}:{:02}".format(int(abs(off_set_hours)), 0)
        return value

    def _format_date_to_utc(self, field_value):
        if field_value == "0" or field_value == "":
            return self.VALUE_NOT_PRESENT
        datetime_obj = datetime.datetime.utcfromtimestamp(int(field_value) / 1000)
        value = datetime_obj.strftime("%Y-%m-%dT%H:%M:%S%z") + "Z"
        return value

    def _format_date_with_custom_offset(self, field_value):
        if field_value == "0" or field_value == "":
            return self.VALUE_NOT_PRESENT
        datetime_obj = datetime.datetime.utcfromtimestamp(int(field_value) / 1000)
        time_object = datetime_obj + datetime.timedelta(seconds=self.offset_seconds)
        value = time_object.strftime("%Y-%m-%dT%H:%M:%S%z") + self.offset_string
        return value

    def _format_date_with_custom_offset_utc(self, field_value):
        if field_value == "0" or field_value == "":
            return self.VALUE_NOT_PRESENT
        datetime_obj = datetime.datetime.utcfromtimestamp(int(field_value) / 1000)
        value = datetime_obj.strftime("%Y-%m-%dT%H:%M:%S%z") + "Z"
        return value

    @classmethod
    def decompress_meas(cls, field_value):
        return EventDecompressor.MEAS_VALUES_DIC.get(field_value.strip(), "")

    def get_processed_value(self, field_name, field_value, event_version):
        """
        Decompress the value if it is compressed; if it is not a compressed value, it will return the same value.

        :param field_name: The name if the field, ex CBSD_SERIAL_NUMBER.
        :param field_name: The name if the field, ex CBSD_SERIAL_NUMBER.
        :param field_value: The value if the field, ex: D829153166.
        :param event_version: Version of the event, ex 2023,10.
        :return: The processed value.
        """
        version_methods = self.method_mapper[event_version]
        method = version_methods.get(field_name)
        return field_value if method is None else method(field_value)

    def _decompress_radio_technology(self, field_value):
        if field_value == "":
            return self.VALUE_NOT_PRESENT
        return self.INT_TO_RADIO_TECHNOLOGY_DIC[int(field_value)]

    def _decompress_channel_type(self, field_value):
        if field_value == "":
            return self.VALUE_NOT_PRESENT
        if field_value.startswith('[') and field_value.endswith(']'):
            list_value = field_value[1:-1].split(',')
            result = []
            for element in list_value:
                result.append(self.INT_TO_CHANNEL_TYPE_DIC[int(element)])
            return "[" + ", ".join(result) + "]"
        else:
            return self.INT_TO_CHANNEL_TYPE_DIC[int(field_value)]

    def _decompress_maxeirp(self, maxeirp):
        if maxeirp == "[0.0]":
            return self.VALUE_NOT_PRESENT
        if maxeirp.startswith('[') and maxeirp.endswith(']'):
            list_value = maxeirp[1:-1].split(',')
            result = []
            for element in list_value:
                result.append(self._getMaxEirp(element))
            return "[" + ", ".join(result) + "]"
        else:
            return self._getMaxEirp(maxeirp)

    def _replace_empty_string_arrays(self, array):
        if array == "[]":
            return self.VALUE_NOT_PRESENT
        return array

    def _replace_empty_int_arrays(self, array):
        if array == "[0]":
            return self.VALUE_NOT_PRESENT
        list_value = array[1:-1].split(',')
        result = []
        for channel in list_value:
            channel = str(channel).strip()
            if channel == "-1":
                result.append(self.VALUE_NOT_PRESENT)
            else:
                result.append(channel)
        return "[" + ", ".join(result) + "]"

    def _getMaxEirp(self, maxeirp):
        if "-138" in maxeirp:
            return self.VALUE_NOT_PRESENT
        else:
            return maxeirp

    def _decompress_cell_priority(self, priority_int):
        return self.INT_TO_CELL_PRIORITY[int(priority_int)]

    def _replace_default_bandwidth(self, band_width):
        if band_width == "0":
            return self.VALUE_NOT_PRESENT
        return band_width

    def _replace_default_int_minus_1(self, int_value):
        if int_value.strip() == "-1":
            return self.VALUE_NOT_PRESENT
        return int_value

    def _replace_default_string(self, string_value):
        if string_value == "":
            return self.VALUE_NOT_PRESENT
        return string_value


class BaseEventPrinter:
    """
    Base class for printing the event data.
    """

    def __init__(self, schema_parser, event_decompressor):
        self.schema_parser = schema_parser
        self.event_decompressor = event_decompressor
        self.event_headers_list = []

    def print_event(self, event_name, event_occurred_on, event_values_list):
        """
        Print the event.
        :param event_occurred_on:
        :param event_name: Event name, ex EV_SAS_REGISTRATION
        :param event_values_list:  List of values to be printed for the respective event.
        :return: None, it prints the events to stdout
        """
        event_version = event_values_list[1]
        self.event_headers_list = self.get_headers(event_version, event_name)
        if isinstance(self.event_headers_list, str):
            ConsoleUtilities.print_line_to_stdout(self.event_headers_list)
            return
        self.print_headers()
        self.print_field_that_is_not_in_event_information(0, event_name)
        self.print_field_that_is_not_in_event_information(1, event_occurred_on)
        headers_offset = 2
        event_values_list = self.sort_values(event_values_list, event_version, event_name, self.event_headers_list)
        for header_index in range(headers_offset, len(self.event_headers_list)):
            value = self.event_decompressor.get_processed_value(self.event_headers_list[header_index],
                                                                event_values_list[header_index - headers_offset],
                                                                event_version)

            self.print_value(value, header_index)
        self.print_after_event()

    def print_after_event(self):
        pass

    def get_headers(self, event_version, event_name):
        return []

    def print_field_that_is_not_in_event_information(self, index, event_name):
        pass

    def print_value(self, value, header_index):
        pass

    def print_headers(self):
        pass

    def sort_values(self, values, event_version, event_name, headers):
        pass


class InLineEventPrinter(BaseEventPrinter):
    """
    Class is used to print the event data in the form of a name-value pair.
    """

    def __init__(self, schema_parser, event_decompressor):
        BaseEventPrinter.__init__(self, schema_parser, event_decompressor)

    def print_after_event(self):
        ConsoleUtilities.print_new_line()

    def print_field_that_is_not_in_event_information(self, index, event_name):
        ConsoleUtilities.print_to_console_in_line(self.event_headers_list[index], event_name)

    def print_value(self, value, header_index):
        ConsoleUtilities.print_to_console_in_line(self.event_headers_list[header_index], value)

    def print_headers(self):
        pass

    def get_headers(self, event_version, event_name):
        return self.schema_parser.get_headers(str(event_version), str(event_name))

    def sort_values(self, values, event_version, event_name, header):
        return values


class CsvEventPrinter(BaseEventPrinter):
    """
    Prints event on one line using a default separator "," or selected separator.
    """
    shouldPrintHeader = True

    def __init__(self, schema_parser, event_decompressor, separator):
        BaseEventPrinter.__init__(self, schema_parser, event_decompressor)
        self.separator = separator

    def print_after_event(self):
        pass

    def print_field_that_is_not_in_event_information(self, index, event_name):
        ConsoleUtilities.print_to_console_with_separator(event_name, self.separator)

    def print_value(self, value, header_index):
        if header_index + 1 == len(self.event_headers_list):
            ConsoleUtilities.print_to_console_with_separator(value)
        else:
            ConsoleUtilities.print_to_console_with_separator(value, self.separator)

    def print_headers(self):
        if self.shouldPrintHeader:
            ConsoleUtilities.print_to_console_with_separator(self.separator.join(self.event_headers_list))
            self.shouldPrintHeader = False

    def sort_values(self, values, event_version, event_name, headers):
        parser_dic = self.schema_parser.get_csv_parser_dic(event_version)
        values_headers = self.schema_parser.get_headers_csv(str(event_version), str(event_name))
        event_headers = self.schema_parser.get_headers(str(event_version), str(event_name))
        event_headers_with_out_first_two = event_headers[2:]
        sorted_values = ["" for _ in range(len(values_headers) - 2)]
        for header, value in zip(event_headers_with_out_first_two, values):
            position = parser_dic[header]
            sorted_values[position] = value
        return sorted_values

    def get_headers(self, event_version, event_name):
        return self.schema_parser.get_headers_csv(str(event_version), str(event_name))


class ConsoleUtilities:

    @staticmethod
    def print_to_console_with_separator(value, separator="\n"):
        ConsoleUtilities.print_with_catching_broken_pipe_exception_for_head("{0}{1}".format(value, separator))

    @staticmethod
    def print_to_console_in_line(header, value):
        ConsoleUtilities.print_with_catching_broken_pipe_exception_for_head("{0}: {1}\n".format(header, value))

    @staticmethod
    def print_line_to_stdout(string):
        ConsoleUtilities.print_with_catching_broken_pipe_exception_for_head(string + "\n")

    @staticmethod
    def print_with_catching_broken_pipe_exception_for_head(string_to_print):
        try:
            sys.stdout.write(string_to_print)
        except IOError:
            # For broken pipe, for example head -2
            ConsoleUtilities.clear_stderr()
            ConsoleUtilities.exit_2()

    @staticmethod
    def print_new_line():
        sys.stdout.write("\n")

    @staticmethod
    def print_to_stderr(message):
        sys.stderr.write(message + "\r")
        sys.stderr.flush()

    @staticmethod
    def clear_stderr():
        sys.stderr.write(" " * 100 + "\r")  # 100 spaces to ensure the entire line is cleared
        sys.stderr.flush()

    @staticmethod
    def exit_2():
        sys.stdout.close()
        sys.stderr.close()
        sys.exit(2)

    @staticmethod
    def print_warning_if_number_of_events_exceed_max(total_events, max_events):
        if total_events >= max_events:
            sys.stderr.write(
                "Warning: Total events exceeds {}, \nyou may want to reduce the size of your search using <startTime> <endTime>, see command usage.\n".format(
                    max_events))
            ConsoleUtilities.print_continue_warning()

    @staticmethod
    def print_continue_warning():
        sys.stderr.write("Press any key to continue or 'c' to cancel: \n")
        sys.stderr.flush()
        choice = ConsoleUtilities.get_user_input()
        sys.stdout.flush()
        if choice.lower() == 'c':
            ConsoleUtilities.exit_2()

    @staticmethod
    def get_user_input():
        if sys.platform.startswith('win'):
            import msvcrt  # For run unit tests in windows
            return msvcrt.getch().decode()
        else:
            import termios
            import tty
            fd = sys.stdin.fileno()
            old_settings = termios.tcgetattr(fd)
            try:
                tty.setraw(fd)
                return sys.stdin.read(1)
            finally:
                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

    @staticmethod
    def print_warning_if_logs_size_exceed_limit(total_file_size, limit):
        if total_file_size >= limit:
            sys.stderr.write("Warning: There is a large amount of data to search; it may take 1 to"
                             " 2 minutes to complete.\n")

    @staticmethod
    def read_first_line_and_get_date_string(file_path, file_name):
        try:
            full_file_path = os.path.join(file_path, file_name)
            with gzip.open(full_file_path, 'rt') as gzfile:
                first_line = gzfile.readline()
                return first_line.split("@")[0]
        except IOError as e:
            print("Error:", e)
            return None


class CbrsEvents:
    """
     The CbrsEvents class provides methods for parsing and printing events,
     it queries elastic search.

     Attributes:
     http_requester: An object for making HTTP requests.
     argument_parser: An object to parse arguments.
     event_schema_parser: An object to parse event schemas.
     event_decompressor: An object to decompress events.
     """

    def __init__(self, http_requester, argument_parser, event_schema_parser):
        self.http_requester = http_requester
        self.argument_parser = argument_parser
        self.event_schema_parser = event_schema_parser
        logging.basicConfig(level=logging.ERROR)

    def process(self):
        group_id, starting_date, end_data, separator, folder_logs_when_ddp = self.argument_parser.parse_args()

        if folder_logs_when_ddp is None:
            events_response = self.http_requester.get_events(group_id, starting_date, end_data)
            event_iterator = ElasticSearchIterator(events_response, group_id)
            total_events = event_iterator.total_events
            ConsoleUtilities.print_warning_if_number_of_events_exceed_max(total_events, 10000)
            event_decompressor = EventDecompressor()
        else:
            event_iterator = DDPIterator(folder_logs_when_ddp, group_id, starting_date, end_data)
            total_file_size = event_iterator.get_total_file_size()
            ConsoleUtilities.print_warning_if_logs_size_exceed_limit(total_file_size, 48176299)
            log_files = DDPIterator.filter_log_files(folder_logs_when_ddp)
            reference_date_string = ConsoleUtilities.read_first_line_and_get_date_string(folder_logs_when_ddp,
                                                                                         log_files[0])
            event_decompressor = EventDecompressor(reference_date_string)
        event_printer = InLineEventPrinter(self.event_schema_parser, event_decompressor) if separator is None else \
            CsvEventPrinter(self.event_schema_parser, event_decompressor, separator)

        events_found = False
        for event_name, event_occurred_on, event_values_list in event_iterator:
            # TODO: We need to make a decision on what we will do with CBS output because this event has too
            #  many event headers.
            if separator is not None and event_name == "EV_CAA":
                ConsoleUtilities.print_to_stderr("Warning: Skipping EV_CAA event")
                continue
            event_printer.print_event(event_name, event_occurred_on, event_values_list)
            events_found = True
        if not events_found:
            ConsoleUtilities.clear_stderr()
            to_print = "No events found for " + group_id if group_id != "*" else "No events found"
            ConsoleUtilities.print_line_to_stdout(to_print)


def main():
    try:
        events = CbrsEvents(HttpRequester(), ArgumentParser(), EventSchemaParser())
        events.process()
        ConsoleUtilities.clear_stderr()
    except KeyboardInterrupt:
        ConsoleUtilities.clear_stderr()


if __name__ == "__main__":
    main()

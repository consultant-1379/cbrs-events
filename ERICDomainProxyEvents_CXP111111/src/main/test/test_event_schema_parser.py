import os
import sys
path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "python")
if path not in sys.path:
    sys.path.append(path)
from unittest import TestCase
import logging
import cbrs_events
from cbrs_events import EventSchemaParser


class TestEventSchemaParser(TestCase):

    @classmethod
    def setUpClass(self):
        # Set the logging level to INFO
        logging.basicConfig(level=logging.DEBUG)
        self.obj_under_test = cbrs_events.EventSchemaParser()

    def test_that_can_load_schemas(self):
        TOTAL_EVENTS_2023_5 = 5
        TOTAL_EVENTS_2023_10 = 18
        TOTAL_EVENTS_2023_15 = 20
        TOTAL_EVENTS_2023_16 = 20
        TOTAL_EVENTS_2024_04 = 20
        TOTAL_EVENTS_2024_05 = 21

        parser = EventSchemaParser.__new__(EventSchemaParser)
        events = parser._load_schemas()
        events_23_05 = events["2023,5"]
        self.assertEqual(len(events_23_05), TOTAL_EVENTS_2023_5)
        events_23_10 = events["2023,10"]
        self.assertEqual(len(events_23_10), TOTAL_EVENTS_2023_10)
        events_23_15 = events["2023,15"]
        self.assertEqual(len(events_23_15), TOTAL_EVENTS_2023_15)
        events_23_16 = events["2023,16"]
        self.assertEqual(len(events_23_16), TOTAL_EVENTS_2023_16)
        events_24_04 = events["2024,04"]
        self.assertEqual(len(events_24_04), TOTAL_EVENTS_2024_04)
        events_24_05 = events["2024,05"]
        self.assertEqual(len(events_24_05), TOTAL_EVENTS_2024_05)

    def test_that_can_load_schemas_csv(self):
        TOTAL_HEADERS_2023_5 = 43
        TOTAL_HEADERS_2023_10 = 64
        TOTAL_HEADERS_2023_15 = 64
        TOTAL_HEADERS_2023_16 = 67
        TOTAL_HEADERS_2024_04 = 67
        TOTAL_HEADERS_2024_05 = 70

        parser = EventSchemaParser.__new__(EventSchemaParser)
        events, mapper = parser._load_csv_schemas()
        events_23_05 = events["2023,5"]
        self.assertEqual(len(events_23_05), TOTAL_HEADERS_2023_5)
        events_23_10 = events["2023,10"]
        self.assertEqual(len(events_23_10), TOTAL_HEADERS_2023_10)
        events_23_15 = events["2023,15"]
        self.assertEqual(len(events_23_15), TOTAL_HEADERS_2023_15)
        events_23_16 = events["2023,16"]
        self.assertEqual(len(events_23_16), TOTAL_HEADERS_2023_16)
        events_24_04 = events["2024,04"]
        self.assertEqual(len(events_24_04), TOTAL_HEADERS_2024_04)
        events_24_05 = events["2024,05"]
        self.assertEqual(len(events_24_05), TOTAL_HEADERS_2024_05)

    # there should be one header in the headers_csv for each unique header in the all events headers
    def test_that_csv_schema_match_event_schema_2024_05(self):
        parser = EventSchemaParser.__new__(EventSchemaParser)
        events = parser._load_schemas()
        events_csv, mapper = parser._load_csv_schemas()
        unique_headers_2024_05 = set()
        events_24_05 = events["2024,05"]
        for event_name, event_headers in events_24_05.items():
            if event_name == "EV_CAA":
                continue
            for header in event_headers[1:]:
                unique_headers_2024_05.add(header)
        events_24_05_csv = events_csv["2024,05"]
        self.assertEqual(len(events_24_05_csv), len(unique_headers_2024_05))

    def test_that_csv_schema_match_event_schema_2024_04(self):
        parser = EventSchemaParser.__new__(EventSchemaParser)
        events = parser._load_schemas()
        events_csv, mapper = parser._load_csv_schemas()
        unique_headers_2024_04 = set()
        events_24_04 = events["2024,04"]
        for event_name, event_headers in events_24_04.items():
            if event_name == "EV_CAA":
                continue
            for header in event_headers[1:]:
                unique_headers_2024_04.add(header)
        events_24_04_csv = events_csv["2024,04"]
        self.assertEqual(len(events_24_04_csv), len(unique_headers_2024_04))

    def test_that_csv_schema_match_event_schema_2023_16(self):
        parser = EventSchemaParser.__new__(EventSchemaParser)
        events = parser._load_schemas()
        events_csv, mapper = parser._load_csv_schemas()
        unique_headers_2023_16 = set()
        events_23_16 = events["2023,16"]
        for event_name, event_headers in events_23_16.items():
            if event_name == "EV_CAA":
                continue
            for header in event_headers[1:]:
                unique_headers_2023_16.add(header)
        events_23_16_csv = events_csv["2023,16"]
        self.assertEqual(len(events_23_16_csv), len(unique_headers_2023_16))

    def test_that_csv_schema_match_event_schema_2023_15(self):
        parser = EventSchemaParser.__new__(EventSchemaParser)
        events = parser._load_schemas()
        events_csv, mapper = parser._load_csv_schemas()
        unique_headers_2023_15 = set()
        events_23_15 = events["2023,15"]
        for event_name, event_headers in events_23_15.items():
            if event_name == "EV_CAA":
                continue
            for header in event_headers[1:]:
                unique_headers_2023_15.add(header)
        events_23_15_csv = events_csv["2023,15"]
        self.assertEqual(len(events_23_15_csv), len(unique_headers_2023_15))

    def test_that_csv_schema_match_event_schema_2023_10(self):
        parser = EventSchemaParser.__new__(EventSchemaParser)
        events = parser._load_schemas()
        events_csv, mapper = parser._load_csv_schemas()
        unique_headers_2023_10 = set()
        events_23_10 = events["2023,10"]
        for event_name, event_headers in events_23_10.items():
            for header in event_headers[1:]:
                unique_headers_2023_10.add(header)
        events_23_10_csv = events_csv["2023,10"]
        self.assertEqual(len(events_23_10_csv), len(unique_headers_2023_10))

    def test_that_csv_schema_match_event_schema_2023_5(self):
        parser = EventSchemaParser.__new__(EventSchemaParser)
        events = parser._load_schemas()
        events_csv, mapper = parser._load_csv_schemas()
        unique_headers_2023_5 = set()
        events_23_5 = events["2023,5"]
        for event_name, event_headers in events_23_5.items():
            for header in event_headers[1:]:
                unique_headers_2023_5.add(header)
        events_23_5_csv = events_csv["2023,5"]
        self.assertEqual(len(events_23_5_csv), len(unique_headers_2023_5))

    def test_that_can_parse_schema_2024_05_for_EV_GROUP_RECOVERY_REG_CONSISTENCY(self):
        headers = self.obj_under_test.get_headers("2024,05", "EV_GROUP_RECOVERY_REG_CONSISTENCY")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'GROUP_ID', 'TOPOLOGY_DATA_VALID', 'COORDINATOR_DATA_VALID']
        self.assertEqual(expected_result, headers)

    def test_that_get_nr_activation_headers_csv_return_error_message_when_event_is_not_in_schema(self):
        expected_result = self.obj_under_test.get_headers_csv("2024,05", "EV_NR_CARRIER_ACTIVATION")
        self.assertEqual(expected_result, "Warning: Event EV_NR_CARRIER_ACTIVATION not found in schema for version 2024,05")

    def test_that_get_lte_activation_headers_csv_return_error_message_when_event_is_not_in_schema(self):
        expected_result = self.obj_under_test.get_headers_csv("2024,05", "EV_LTE_CELL_ACTIVATION")
        self.assertEqual(expected_result, "Warning: Event EV_LTE_CELL_ACTIVATION not found in schema for version 2024,05")

    def test_that_can_parse_schema_2024_05_for_EV_CELL_CARRIER_TRANSMISSION(self):
        headers = self.obj_under_test.get_headers("2024,05", "EV_CELL_CARRIER_TRANSMISSION")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'RAT_TYPE', 'CELL_STATE', 'FAILURE_REASON', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_05_for_EV_CAA(self):
        headers = self.obj_under_test.get_headers("2024,05", "EV_CAA")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'ADDITIONAL_CBSD_ID', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'AUTHORIZED_CHANNELS', 'AUTHORIZED_MAX_EIRP', 'CHANNEL_TYPE', 'ANTENNA_GAIN', 'CELL_1_ID', 'CELL_1_RAT', 'CELL_1_PREFERRED_BANDWIDTH', 'CELL_1_PRIORITY', 'CELL_1_RANK', 'CELL_1_ACTIVE', 'CELL_1_ACTIVE_CHANNELS', 'CELL_1_ACTIVE_MAX_EIRP', 'CELL_2_ID', 'CELL_2_RAT', 'CELL_2_PREFERRED_BANDWIDTH', 'CELL_2_PRIORITY', 'CELL_2_RANK', 'CELL_2_ACTIVE', 'CELL_2_ACTIVE_CHANNELS', 'CELL_2_ACTIVE_MAX_EIRP', 'CELL_3_ID', 'CELL_3_RAT', 'CELL_3_PREFERRED_BANDWIDTH', 'CELL_3_PRIORITY', 'CELL_3_RANK', 'CELL_3_ACTIVE', 'CELL_3_ACTIVE_CHANNELS', 'CELL_3_ACTIVE_MAX_EIRP', 'CELL_4_ID', 'CELL_4_RAT', 'CELL_4_PREFERRED_BANDWIDTH', 'CELL_4_PRIORITY', 'CELL_4_RANK', 'CELL_4_ACTIVE', 'CELL_4_ACTIVE_CHANNELS', 'CELL_4_ACTIVE_MAX_EIRP', 'CELL_5_ID', 'CELL_5_RAT', 'CELL_5_PREFERRED_BANDWIDTH', 'CELL_5_PRIORITY', 'CELL_5_RANK', 'CELL_5_ACTIVE', 'CELL_5_ACTIVE_CHANNELS', 'CELL_5_ACTIVE_MAX_EIRP', 'CELL_6_ID', 'CELL_6_RAT', 'CELL_6_PREFERRED_BANDWIDTH', 'CELL_6_PRIORITY', 'CELL_6_RANK', 'CELL_6_ACTIVE', 'CELL_6_ACTIVE_CHANNELS', 'CELL_6_ACTIVE_MAX_EIRP', 'FCC_ID', 'PASSIVE_DAS', 'ALLOW_GAA_WITH_PAL', 'IS_POWER_RECONFIG_ENABLED', 'NUM_DEVICES', 'CELL_1_ASSIGNED_CHANNELS', 'CELL_1_ASSIGNED_MAX_EIRP', 'CELL_2_ASSIGNED_CHANNELS', 'CELL_2_ASSIGNED_MAX_EIRP', 'CELL_3_ASSIGNED_CHANNELS', 'CELL_3_ASSIGNED_MAX_EIRP', 'CELL_4_ASSIGNED_CHANNELS', 'CELL_4_ASSIGNED_MAX_EIRP', 'CELL_5_ASSIGNED_CHANNELS', 'CELL_5_ASSIGNED_MAX_EIRP', 'CELL_6_ASSIGNED_CHANNELS', 'CELL_6_ASSIGNED_MAX_EIRP']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_05_for_EV_NRSC_RECONFIG(self):
        headers = self.obj_under_test.get_headers("2024,05","EV_NRSC_RECONFIG")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'ARFCN_DL', 'ARFCN_UL', 'BSCHANNEL_BW_DL', 'BSCHANNEL_BW_UL', 'CONFIGURED_MAX_TX_POWER', 'MAX_ALLOWED_EIRP_PSD', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_05_for_EV_NRCELLDU_RECONFIG(self):
        headers = self.obj_under_test.get_headers("2024,05","EV_NRCELLDU_RECONFIG")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_05_for_EV_LTE_CELL_DEACTIVATION(self):
        headers = self.obj_under_test.get_headers("2024,05","EV_LTE_CELL_DEACTIVATION")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'TRANSMIT_EXPIRE_TIME_VALUE', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_05_for_EV_NR_CARRIER_DEACTIVATION(self):
        headers = self.obj_under_test.get_headers("2024,05","EV_NR_CARRIER_DEACTIVATION")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'TRANSMIT_EXPIRE_TIME_VALUE', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_05_for_EV_EUTRANCELLTDD_RECONFIG(self):
        headers = self.obj_under_test.get_headers("2024,05","EV_EUTRANCELLTDD_RECONFIG")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'BANDWIDTH', 'EARFCN', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_05_for_EV_SC_RECONFIG(self):
        headers = self.obj_under_test.get_headers("2024,05","EV_SC_RECONFIG")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'CONFIGURED_MAX_TX_POWER', 'MAX_ALLOWED_EIRP_PSD', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_05_for_EV_SAS_REGISTRATION(self):
        headers = self.obj_under_test.get_headers("2024,05","EV_SAS_REGISTRATION")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FCC_ID', 'USER_ID', 'CBSD_CATEGORY', 'RADIO_TECHNOLOGY', 'LATITUDE', 'LONGITUDE', 'HEIGHT', 'HEIGHT_TYPE', 'HORIZONTAL_ACCURACY', 'VERTICAL_ACCURACY', 'INDOOR_DEPLOYMENT', 'ANTENNA_AZIMUTH', 'ANTENNA_DOWNTILT', 'ANTENNA_GAIN', 'EIRP_CAPABILITY', 'ANTENNA_BEAMWIDTH', 'ANTENNA_MODEL', 'MEAS_CAPABILITY_AVAILABLE', 'GROUP_TYPE', 'MEAS_REPORT_REQUESTED', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_05_for_EV_SAS_SPECTRUMINQUIRY(self):
        headers = self.obj_under_test.get_headers("2024,05", "EV_SAS_SPECTRUMINQUIRY")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'REQUESTED_CHANNELS', 'AVAILABLE_CHANNELS_TYPE', 'RESPONSE_MAXEIRP', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_05_for_EV_SAS_GRANT(self):
        headers = self.obj_under_test.get_headers("2024,05", "EV_SAS_GRANT")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'REQUESTED_CHANNEL', 'REQUESTED_MAXEIRP', 'GRANT_ID', 'GRANT_EXPIRE_TIME', 'HEARTBEAT_INTERVAL', 'MEAS_REPORT_REQUESTED', 'RESPONSE_MAXEIRP', 'RESPONSE_CHANNEL', 'RESPONSE_CHANNEL_TYPE', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_05_for_EV_SAS_RELINQUISHMENT(self):
        headers = self.obj_under_test.get_headers("2024,05", "EV_SAS_RELINQUISHMENT")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'GRANT_ID', 'REQUESTED_CHANNEL', 'CHANNEL_TYPE', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_05_for_EV_SAS_DEREGISTRATION(self):
        headers = self.obj_under_test.get_headers("2024,05", "EV_SAS_DEREGISTRATION")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_05_for_EV_SAS_DEREGISTRATION_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2024,05", "EV_SAS_DEREGISTRATION_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_05_for_EV_SAS_HEARTBEAT(self):
        headers = self.obj_under_test.get_headers("2024,05", "EV_SAS_HEARTBEAT")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'GRANT_ID', 'GRANT_RENEW', 'REQUESTED_CHANNEL', 'OPERATION_STATE', 'MEAS_REPORT_RETURNED', 'REQUESTED_MAXEIRP', 'TRANSMIT_EXPIRE_TIME', 'GRANT_EXPIRE_TIME', 'HEARTBEAT_INTERVAL', 'RESPONSE_MAXEIRP', 'RESPONSE_CHANNEL', 'CHANNEL_TYPE', 'MEAS_REPORT_REQUESTED', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_05_for_EV_SAS_HEARTBEAT_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2024,05", "EV_SAS_HEARTBEAT_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'GRANT_ID', 'GRANT_RENEW', 'MEAS_REPORT_RETURNED', 'OPERATION_STATE', 'CHANNEL', 'CHANNEL_TYPE', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_05_for_EV_SAS_SPECTRUMINQUIRY_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2024,05", "EV_SAS_SPECTRUMINQUIRY_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'REQUESTED_CHANNELS', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_05_for_EV_SAS_RELINQUISHMENT_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2024,05", "EV_SAS_RELINQUISHMENT_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'GRANT_ID', 'CHANNEL', 'CHANNEL_TYPE', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_05_for_EV_SAS_REGISTRATION_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2024,05","EV_SAS_REGISTRATION_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'GROUP_ID', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_05_for_EV_SAS_GRANT_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2024,05", "EV_SAS_GRANT_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'CHANNEL', 'MAX_EIRP', 'CHANNEL_TYPE', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_get_nr_activation_headers_csv_return_error_message_when_event_is_not_in_schema(self):
        expected_result = self.obj_under_test.get_headers_csv("2024,04", "EV_NR_CARRIER_ACTIVATION")
        self.assertEqual(expected_result, "Warning: Event EV_NR_CARRIER_ACTIVATION not found in schema for version 2024,04")

    def test_that_get_lte_activation_headers_csv_return_error_message_when_event_is_not_in_schema(self):
        expected_result = self.obj_under_test.get_headers_csv("2024,04", "EV_LTE_CELL_ACTIVATION")
        self.assertEqual(expected_result, "Warning: Event EV_LTE_CELL_ACTIVATION not found in schema for version 2024,04")

    def test_that_can_parse_schema_2024_04_for_EV_CELL_CARRIER_TRANSMISSION(self):
        headers = self.obj_under_test.get_headers("2024,04", "EV_CELL_CARRIER_TRANSMISSION")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'RAT_TYPE', 'CELL_STATE', 'FAILURE_REASON', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_04_for_EV_CAA(self):
        headers = self.obj_under_test.get_headers("2024,04", "EV_CAA")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'ADDITIONAL_CBSD_ID', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'AUTHORIZED_CHANNELS', 'AUTHORIZED_MAX_EIRP', 'CHANNEL_TYPE', 'ANTENNA_GAIN', 'CELL_1_ID', 'CELL_1_RAT', 'CELL_1_PREFERRED_BANDWIDTH', 'CELL_1_PRIORITY', 'CELL_1_RANK', 'CELL_1_ACTIVE', 'CELL_1_ACTIVE_CHANNELS', 'CELL_1_ACTIVE_MAX_EIRP', 'CELL_2_ID', 'CELL_2_RAT', 'CELL_2_PREFERRED_BANDWIDTH', 'CELL_2_PRIORITY', 'CELL_2_RANK', 'CELL_2_ACTIVE', 'CELL_2_ACTIVE_CHANNELS', 'CELL_2_ACTIVE_MAX_EIRP', 'CELL_3_ID', 'CELL_3_RAT', 'CELL_3_PREFERRED_BANDWIDTH', 'CELL_3_PRIORITY', 'CELL_3_RANK', 'CELL_3_ACTIVE', 'CELL_3_ACTIVE_CHANNELS', 'CELL_3_ACTIVE_MAX_EIRP', 'CELL_4_ID', 'CELL_4_RAT', 'CELL_4_PREFERRED_BANDWIDTH', 'CELL_4_PRIORITY', 'CELL_4_RANK', 'CELL_4_ACTIVE', 'CELL_4_ACTIVE_CHANNELS', 'CELL_4_ACTIVE_MAX_EIRP', 'CELL_5_ID', 'CELL_5_RAT', 'CELL_5_PREFERRED_BANDWIDTH', 'CELL_5_PRIORITY', 'CELL_5_RANK', 'CELL_5_ACTIVE', 'CELL_5_ACTIVE_CHANNELS', 'CELL_5_ACTIVE_MAX_EIRP', 'CELL_6_ID', 'CELL_6_RAT', 'CELL_6_PREFERRED_BANDWIDTH', 'CELL_6_PRIORITY', 'CELL_6_RANK', 'CELL_6_ACTIVE', 'CELL_6_ACTIVE_CHANNELS', 'CELL_6_ACTIVE_MAX_EIRP', 'FCC_ID', 'PASSIVE_DAS', 'ALLOW_GAA_WITH_PAL', 'IS_POWER_RECONFIG_ENABLED', 'NUM_DEVICES', 'CELL_1_ASSIGNED_CHANNELS', 'CELL_1_ASSIGNED_MAX_EIRP', 'CELL_2_ASSIGNED_CHANNELS', 'CELL_2_ASSIGNED_MAX_EIRP', 'CELL_3_ASSIGNED_CHANNELS', 'CELL_3_ASSIGNED_MAX_EIRP', 'CELL_4_ASSIGNED_CHANNELS', 'CELL_4_ASSIGNED_MAX_EIRP', 'CELL_5_ASSIGNED_CHANNELS', 'CELL_5_ASSIGNED_MAX_EIRP', 'CELL_6_ASSIGNED_CHANNELS', 'CELL_6_ASSIGNED_MAX_EIRP']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_04_for_EV_NRSC_RECONFIG(self):
        headers = self.obj_under_test.get_headers("2024,04","EV_NRSC_RECONFIG")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'ARFCN_DL', 'ARFCN_UL', 'BSCHANNEL_BW_DL', 'BSCHANNEL_BW_UL', 'CONFIGURED_MAX_TX_POWER', 'MAX_ALLOWED_EIRP_PSD', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_04_for_EV_NRCELLDU_RECONFIG(self):
        headers = self.obj_under_test.get_headers("2024,04","EV_NRCELLDU_RECONFIG")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_04_for_EV_LTE_CELL_DEACTIVATION(self):
        headers = self.obj_under_test.get_headers("2024,04","EV_LTE_CELL_DEACTIVATION")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'TRANSMIT_EXPIRE_TIME_VALUE', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_04_for_EV_NR_CARRIER_DEACTIVATION(self):
        headers = self.obj_under_test.get_headers("2024,04","EV_NR_CARRIER_DEACTIVATION")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'TRANSMIT_EXPIRE_TIME_VALUE', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_04_for_EV_EUTRANCELLTDD_RECONFIG(self):
        headers = self.obj_under_test.get_headers("2024,04","EV_EUTRANCELLTDD_RECONFIG")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'BANDWIDTH', 'EARFCN', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_04_for_EV_SC_RECONFIG(self):
        headers = self.obj_under_test.get_headers("2024,04","EV_SC_RECONFIG")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'CONFIGURED_MAX_TX_POWER', 'MAX_ALLOWED_EIRP_PSD', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_04_for_EV_SAS_REGISTRATION(self):
        headers = self.obj_under_test.get_headers("2024,04","EV_SAS_REGISTRATION")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FCC_ID', 'USER_ID', 'CBSD_CATEGORY', 'RADIO_TECHNOLOGY', 'LATITUDE', 'LONGITUDE', 'HEIGHT', 'HEIGHT_TYPE', 'HORIZONTAL_ACCURACY', 'VERTICAL_ACCURACY', 'INDOOR_DEPLOYMENT', 'ANTENNA_AZIMUTH', 'ANTENNA_DOWNTILT', 'ANTENNA_GAIN', 'EIRP_CAPABILITY', 'ANTENNA_BEAMWIDTH', 'ANTENNA_MODEL', 'MEAS_CAPABILITY_AVAILABLE', 'GROUP_TYPE', 'MEAS_REPORT_REQUESTED', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_04_for_EV_SAS_SPECTRUMINQUIRY(self):
        headers = self.obj_under_test.get_headers("2024,04", "EV_SAS_SPECTRUMINQUIRY")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'REQUESTED_CHANNELS', 'AVAILABLE_CHANNELS_TYPE', 'RESPONSE_MAXEIRP', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_04_for_EV_SAS_GRANT(self):
        headers = self.obj_under_test.get_headers("2024,04", "EV_SAS_GRANT")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE','CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'REQUESTED_CHANNEL', 'REQUESTED_MAXEIRP', 'GRANT_ID', 'GRANT_EXPIRE_TIME', 'HEARTBEAT_INTERVAL', 'MEAS_REPORT_REQUESTED', 'RESPONSE_MAXEIRP', 'RESPONSE_CHANNEL', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_04_for_EV_SAS_RELINQUISHMENT(self):
        headers = self.obj_under_test.get_headers("2024,04", "EV_SAS_RELINQUISHMENT")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'GRANT_ID', 'CHANNEL', 'CHANNEL_TYPE', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_04_for_EV_SAS_DEREGISTRATION(self):
        headers = self.obj_under_test.get_headers("2024,04", "EV_SAS_DEREGISTRATION")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_04_for_EV_SAS_DEREGISTRATION_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2024,04", "EV_SAS_DEREGISTRATION_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_04_for_EV_SAS_HEARTBEAT(self):
        headers = self.obj_under_test.get_headers("2024,04", "EV_SAS_HEARTBEAT")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'GRANT_ID', 'GRANT_RENEW', 'OPERATION_STATE', 'MEAS_REPORT_RETURNED', 'REQUESTED_MAXEIRP', 'TRANSMIT_EXPIRE_TIME', 'GRANT_EXPIRE_TIME', 'HEARTBEAT_INTERVAL', 'RESPONSE_MAXEIRP', 'RESPONSE_CHANNEL', 'CHANNEL_TYPE', 'MEAS_REPORT_REQUESTED', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_04_for_EV_SAS_HEARTBEAT_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2024,04", "EV_SAS_HEARTBEAT_TRANS_ERROR")
        expected_result = ["EVENT_NAME","EVENT_OCCURRED_ON","EVENT_ID","EVENT_VERSION","EVENT_TIME","EVENT_SOURCE","CBSD_SERIAL_NUMBER","CBSD_ID","GROUP_ID","GRANT_ID","GRANT_RENEW","MEAS_REPORT_RETURNED","OPERATION_STATE","ERROR_CODE","ERROR_TYPE","ERROR_DESCRIPTION"]
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_04_for_EV_SAS_SPECTRUMINQUIRY_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2024,04", "EV_SAS_SPECTRUMINQUIRY_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'REQUESTED_CHANNELS', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_04_for_EV_SAS_RELINQUISHMENT_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2024,04", "EV_SAS_RELINQUISHMENT_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'GRANT_ID', 'CHANNEL', 'CHANNEL_TYPE', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_04_for_EV_SAS_REGISTRATION_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2024,04","EV_SAS_REGISTRATION_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'GROUP_ID', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2024_04_for_EV_SAS_GRANT_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2024,04", "EV_SAS_GRANT_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'CHANNEL', 'MAX_EIRP', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_get_nr_activation_headers_csv_return_error_message_when_event_is_not_in_schema(self):
        expected_result = self.obj_under_test.get_headers_csv("2023,16", "EV_NR_CARRIER_ACTIVATION")
        self.assertEqual(expected_result, "Warning: Event EV_NR_CARRIER_ACTIVATION not found in schema for version 2023,16")

    def test_that_get_lte_activation_headers_csv_return_error_message_when_event_is_not_in_schema(self):
        expected_result = self.obj_under_test.get_headers_csv("2023,16", "EV_LTE_CELL_ACTIVATION")
        self.assertEqual(expected_result, "Warning: Event EV_LTE_CELL_ACTIVATION not found in schema for version 2023,16")

    def test_that_can_parse_schema_2023_16_for_EV_CELL_CARRIER_TRANSMISSION(self):
        headers = self.obj_under_test.get_headers("2023,16", "EV_CELL_CARRIER_TRANSMISSION")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'RAT_TYPE', 'CELL_STATE', 'FAILURE_REASON', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_16_for_EV_CAA(self):
        headers = self.obj_under_test.get_headers("2023,16", "EV_CAA")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'ADDITIONAL_CBSD_ID', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'AUTHORIZED_CHANNELS', 'AUTHORIZED_MAX_EIRP', 'CHANNEL_TYPE', 'ANTENNA_GAIN', 'CELL_1_ID', 'CELL_1_RAT', 'CELL_1_PREFERRED_BANDWIDTH', 'CELL_1_PRIORITY', 'CELL_1_RANK', 'CELL_1_ACTIVE', 'CELL_1_ACTIVE_CHANNELS', 'CELL_1_ACTIVE_MAX_EIRP', 'CELL_2_ID', 'CELL_2_RAT', 'CELL_2_PREFERRED_BANDWIDTH', 'CELL_2_PRIORITY', 'CELL_2_RANK', 'CELL_2_ACTIVE', 'CELL_2_ACTIVE_CHANNELS', 'CELL_2_ACTIVE_MAX_EIRP', 'CELL_3_ID', 'CELL_3_RAT', 'CELL_3_PREFERRED_BANDWIDTH', 'CELL_3_PRIORITY', 'CELL_3_RANK', 'CELL_3_ACTIVE', 'CELL_3_ACTIVE_CHANNELS', 'CELL_3_ACTIVE_MAX_EIRP', 'CELL_4_ID', 'CELL_4_RAT', 'CELL_4_PREFERRED_BANDWIDTH', 'CELL_4_PRIORITY', 'CELL_4_RANK', 'CELL_4_ACTIVE', 'CELL_4_ACTIVE_CHANNELS', 'CELL_4_ACTIVE_MAX_EIRP', 'CELL_5_ID', 'CELL_5_RAT', 'CELL_5_PREFERRED_BANDWIDTH', 'CELL_5_PRIORITY', 'CELL_5_RANK', 'CELL_5_ACTIVE', 'CELL_5_ACTIVE_CHANNELS', 'CELL_5_ACTIVE_MAX_EIRP', 'CELL_6_ID', 'CELL_6_RAT', 'CELL_6_PREFERRED_BANDWIDTH', 'CELL_6_PRIORITY', 'CELL_6_RANK', 'CELL_6_ACTIVE', 'CELL_6_ACTIVE_CHANNELS', 'CELL_6_ACTIVE_MAX_EIRP', 'FCC_ID', 'PASSIVE_DAS', 'ALLOW_GAA_WITH_PAL', 'IS_POWER_RECONFIG_ENABLED', 'NUM_DEVICES', 'CELL_1_ASSIGNED_CHANNELS', 'CELL_1_ASSIGNED_MAX_EIRP', 'CELL_2_ASSIGNED_CHANNELS', 'CELL_2_ASSIGNED_MAX_EIRP', 'CELL_3_ASSIGNED_CHANNELS', 'CELL_3_ASSIGNED_MAX_EIRP', 'CELL_4_ASSIGNED_CHANNELS', 'CELL_4_ASSIGNED_MAX_EIRP', 'CELL_5_ASSIGNED_CHANNELS', 'CELL_5_ASSIGNED_MAX_EIRP', 'CELL_6_ASSIGNED_CHANNELS', 'CELL_6_ASSIGNED_MAX_EIRP']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_16_for_EV_NRSC_RECONFIG(self):
        headers = self.obj_under_test.get_headers("2023,16","EV_NRSC_RECONFIG")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'ARFCN_DL', 'ARFCN_UL', 'BSCHANNEL_BW_DL', 'BSCHANNEL_BW_UL', 'CONFIGURED_MAX_TX_POWER', 'MAX_ALLOWED_EIRP_PSD', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_16_for_EV_NRCELLDU_RECONFIG(self):
        headers = self.obj_under_test.get_headers("2023,16","EV_NRCELLDU_RECONFIG")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_16_for_EV_LTE_CELL_DEACTIVATION(self):
        headers = self.obj_under_test.get_headers("2023,16","EV_LTE_CELL_DEACTIVATION")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'TRANSMIT_EXPIRE_TIME_VALUE', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_16_for_EV_NR_CARRIER_DEACTIVATION(self):
        headers = self.obj_under_test.get_headers("2023,16","EV_NR_CARRIER_DEACTIVATION")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'TRANSMIT_EXPIRE_TIME_VALUE', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_16_for_EV_EUTRANCELLTDD_RECONFIG(self):
        headers = self.obj_under_test.get_headers("2023,16","EV_EUTRANCELLTDD_RECONFIG")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'BANDWIDTH', 'EARFCN', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_16_for_EV_SC_RECONFIG(self):
        headers = self.obj_under_test.get_headers("2023,16","EV_SC_RECONFIG")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'CONFIGURED_MAX_TX_POWER', 'MAX_ALLOWED_EIRP_PSD', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_16_for_EV_SAS_REGISTRATION(self):
        headers = self.obj_under_test.get_headers("2023,16","EV_SAS_REGISTRATION")
        expected_result_2023_16_EV_SAS_REGISTRATION  = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FCC_ID', 'USER_ID', 'CBSD_CATEGORY', 'RADIO_TECHNOLOGY', 'LATITUDE', 'LONGITUDE', 'HEIGHT', 'HEIGHT_TYPE', 'HORIZONTAL_ACCURACY', 'VERTICAL_ACCURACY', 'INDOOR_DEPLOYMENT', 'ANTENNA_AZIMUTH', 'ANTENNA_DOWNTILT', 'ANTENNA_GAIN', 'EIRP_CAPABILITY', 'ANTENNA_BEAMWIDTH', 'ANTENNA_MODEL', 'MEAS_CAPABILITY_AVAILABLE', 'GROUP_TYPE', 'MEAS_REPORT_REQUESTED', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result_2023_16_EV_SAS_REGISTRATION, headers)

    def test_that_can_parse_schema_2023_16_for_EV_SAS_SPECTRUMINQUIRY(self):
        headers = self.obj_under_test.get_headers("2023,16", "EV_SAS_SPECTRUMINQUIRY")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'REQUESTED_CHANNELS', 'AVAILABLE_CHANNELS_TYPE', 'RESPONSE_MAXEIRP', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_16_for_EV_SAS_GRANT(self):
        headers = self.obj_under_test.get_headers("2023,16", "EV_SAS_GRANT")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE','CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'REQUESTED_CHANNEL', 'REQUESTED_MAXEIRP', 'GRANT_ID', 'GRANT_EXPIRE_TIME', 'HEARTBEAT_INTERVAL', 'MEAS_REPORT_REQUESTED', 'RESPONSE_MAXEIRP', 'RESPONSE_CHANNEL', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_16_for_EV_SAS_RELINQUISHMENT(self):
        headers = self.obj_under_test.get_headers("2023,16", "EV_SAS_RELINQUISHMENT")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'GRANT_ID', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_16_for_EV_SAS_DEREGISTRATION(self):
        headers = self.obj_under_test.get_headers("2023,16", "EV_SAS_DEREGISTRATION")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_16_for_EV_SAS_DEREGISTRATION_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2023,16", "EV_SAS_DEREGISTRATION_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_16_for_EV_SAS_HEARTBEAT(self):
        headers = self.obj_under_test.get_headers("2023,16", "EV_SAS_HEARTBEAT")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'GRANT_ID', 'GRANT_RENEW', 'OPERATION_STATE', 'MEAS_REPORT_RETURNED', 'REQUESTED_MAXEIRP', 'TRANSMIT_EXPIRE_TIME', 'GRANT_EXPIRE_TIME', 'HEARTBEAT_INTERVAL', 'RESPONSE_MAXEIRP', 'RESPONSE_CHANNEL', 'CHANNEL_TYPE', 'MEAS_REPORT_REQUESTED', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_16_for_EV_SAS_HEARTBEAT_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2023,16", "EV_SAS_HEARTBEAT_TRANS_ERROR")
        expected_result = ["EVENT_NAME","EVENT_OCCURRED_ON","EVENT_ID","EVENT_VERSION","EVENT_TIME","EVENT_SOURCE","CBSD_SERIAL_NUMBER","CBSD_ID","GROUP_ID","GRANT_ID","GRANT_RENEW","MEAS_REPORT_RETURNED","OPERATION_STATE","ERROR_CODE","ERROR_TYPE","ERROR_DESCRIPTION"]
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_16_for_EV_SAS_SPECTRUMINQUIRY_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2023,16", "EV_SAS_SPECTRUMINQUIRY_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'REQUESTED_CHANNELS', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_16_for_EV_SAS_RELINQUISHMENT_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2023,16", "EV_SAS_RELINQUISHMENT_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'GRANT_ID', 'CHANNEL', 'CHANNEL_TYPE', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_16_for_EV_SAS_REGISTRATION_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2023,16","EV_SAS_REGISTRATION_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'GROUP_ID', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_16_for_EV_SAS_GRANT_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2023,16", "EV_SAS_GRANT_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'CHANNEL', 'MAX_EIRP', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_15_for_EV_EUTRANCELLTDD_RECONFIG(self):
        headers = self.obj_under_test.get_headers("2023,15","EV_EUTRANCELLTDD_RECONFIG")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'BANDWIDTH', 'EARFCN', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_15_for_EV_SC_RECONFIG(self):
        headers = self.obj_under_test.get_headers("2023,15","EV_SC_RECONFIG")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'CONFIGURED_MAX_TX_POWER', 'MAX_ALLOWED_EIRP_PSD', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_15_for_EV_LTE_CELL_ACTIVATION(self):
        headers = self.obj_under_test.get_headers("2023,15","EV_LTE_CELL_ACTIVATION")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'TRANSMIT_EXPIRE_TIME_VALUE', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_15_for_EV_NR_CARRIER_ACTIVATION(self):
        headers = self.obj_under_test.get_headers("2023,15","EV_NR_CARRIER_ACTIVATION")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'TRANSMIT_EXPIRE_TIME_VALUE', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_15_for_EV_NR_CARRIER_DEACTIVATION(self):
        headers = self.obj_under_test.get_headers("2023,15","EV_NR_CARRIER_DEACTIVATION")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'TRANSMIT_EXPIRE_TIME_VALUE', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_15_for_EV_LTE_CELL_DEACTIVATION(self):
        headers = self.obj_under_test.get_headers("2023,15","EV_LTE_CELL_DEACTIVATION")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'TRANSMIT_EXPIRE_TIME_VALUE', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_15_for_EV_NRSC_RECONFIG(self):
        headers = self.obj_under_test.get_headers("2023,15","EV_NRSC_RECONFIG")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'ARFCN_DL', 'ARFCN_UL', 'BSCHANNEL_BW_DL', 'BSCHANNEL_BW_UL', 'CONFIGURED_MAX_TX_POWER', 'MAX_ALLOWED_EIRP_PSD', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_15_for_EV_NRCELLDU_RECONFIG(self):
        headers = self.obj_under_test.get_headers("2023,15","EV_NRCELLDU_RECONFIG")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_15_for_EV_SAS_REGISTRATION(self):
        headers = self.obj_under_test.get_headers("2023,15","EV_SAS_REGISTRATION")
        expected_result_2023_15_EV_SAS_REGISTRATION  = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FCC_ID', 'USER_ID', 'CBSD_CATEGORY', 'RADIO_TECHNOLOGY', 'LATITUDE', 'LONGITUDE', 'HEIGHT', 'HEIGHT_TYPE', 'HORIZONTAL_ACCURACY', 'VERTICAL_ACCURACY', 'INDOOR_DEPLOYMENT', 'ANTENNA_AZIMUTH', 'ANTENNA_DOWNTILT', 'ANTENNA_GAIN', 'EIRP_CAPABILITY', 'ANTENNA_BEAMWIDTH', 'ANTENNA_MODEL', 'MEAS_CAPABILITY_AVAILABLE', 'GROUP_TYPE', 'MEAS_REPORT_REQUESTED', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result_2023_15_EV_SAS_REGISTRATION, headers)

    def test_that_can_parse_schema_2023_15_for_EV_SAS_SPECTRUMINQUIRY(self):
        headers = self.obj_under_test.get_headers("2023,15", "EV_SAS_SPECTRUMINQUIRY")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'REQUESTED_CHANNELS', 'AVAILABLE_CHANNELS_TYPE', 'RESPONSE_MAXEIRP', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_15_for_EV_SAS_GRANT(self):
        headers = self.obj_under_test.get_headers("2023,15", "EV_SAS_GRANT")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE','CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'REQUESTED_CHANNEL', 'REQUESTED_MAXEIRP', 'GRANT_ID', 'GRANT_EXPIRE_TIME', 'HEARTBEAT_INTERVAL', 'MEAS_REPORT_REQUESTED', 'RESPONSE_MAXEIRP', 'RESPONSE_CHANNEL', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_15_for_EV_SAS_RELINQUISHMENT(self):
        headers = self.obj_under_test.get_headers("2023,15", "EV_SAS_RELINQUISHMENT")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'GRANT_ID', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_15_for_EV_SAS_DEREGISTRATION(self):
        headers = self.obj_under_test.get_headers("2023,15", "EV_SAS_DEREGISTRATION")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_15_for_EV_SAS_DEREGISTRATION_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2023,15", "EV_SAS_DEREGISTRATION_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_15_for_EV_SAS_HEARTBEAT(self):
        headers = self.obj_under_test.get_headers("2023,15", "EV_SAS_HEARTBEAT")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'GRANT_ID', 'GRANT_RENEW', 'OPERATION_STATE', 'MEAS_REPORT_RETURNED', 'REQUESTED_MAXEIRP', 'TRANSMIT_EXPIRE_TIME', 'GRANT_EXPIRE_TIME', 'HEARTBEAT_INTERVAL', 'RESPONSE_MAXEIRP', 'RESPONSE_CHANNEL', 'CHANNEL_TYPE', 'MEAS_REPORT_REQUESTED', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_15_for_EV_SAS_HEARTBEAT_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2023,15", "EV_SAS_HEARTBEAT_TRANS_ERROR")
        expected_result = ["EVENT_NAME","EVENT_OCCURRED_ON","EVENT_ID","EVENT_VERSION","EVENT_TIME","EVENT_SOURCE","CBSD_SERIAL_NUMBER","CBSD_ID","GROUP_ID","GRANT_ID","GRANT_RENEW","MEAS_REPORT_RETURNED","OPERATION_STATE","ERROR_CODE","ERROR_TYPE","ERROR_DESCRIPTION"]
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_15_for_EV_SAS_SPECTRUMINQUIRY_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2023,15", "EV_SAS_SPECTRUMINQUIRY_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'REQUESTED_CHANNELS', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_15_for_EV_SAS_RELINQUISHMENT_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2023,15", "EV_SAS_RELINQUISHMENT_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'GRANT_ID', 'CHANNEL', 'CHANNEL_TYPE', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_15_for_EV_SAS_REGISTRATION_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2023,15","EV_SAS_REGISTRATION_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'GROUP_ID', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_15_for_EV_SAS_GRANT_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2023,15", "EV_SAS_GRANT_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'CHANNEL', 'MAX_EIRP', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_10_for_EV_NR_CARRIER_DEACTIVATION(self):
        headers = self.obj_under_test.get_headers("2023,10","EV_NR_CARRIER_DEACTIVATION")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'TRANSMIT_EXPIRE_TIME_VALUE', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_10_for_EV_LTE_CELL_DEACTIVATION(self):
        headers = self.obj_under_test.get_headers("2023,10","EV_LTE_CELL_DEACTIVATION")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'TRANSMIT_EXPIRE_TIME_VALUE', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_10_for_EV_NRCELLDU_RECONFIG(self):
        headers = self.obj_under_test.get_headers("2023,10","EV_NRCELLDU_RECONFIG")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_10_for_EV_EUTRANCELLTDD_RECONFIG(self):
        headers = self.obj_under_test.get_headers("2023,10","EV_EUTRANCELLTDD_RECONFIG")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'BANDWIDTH', 'EARFCN', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_10_for_EV_SC_RECONFIG(self):
        headers = self.obj_under_test.get_headers("2023,10","EV_SC_RECONFIG")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'CONFIGURED_MAX_TX_POWER', 'MAX_ALLOWED_EIRP_PSD', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_10_for_EV_SAS_REGISTRATION(self):
        headers = self.obj_under_test.get_headers("2023,10","EV_SAS_REGISTRATION")
        expected_result_2023_10_EV_SAS_REGISTRATION  = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FCC_ID', 'USER_ID', 'CBSD_CATEGORY', 'RADIO_TECHNOLOGY', 'LATITUDE', 'LONGITUDE', 'HEIGHT', 'HEIGHT_TYPE', 'HORIZONTAL_ACCURACY', 'VERTICAL_ACCURACY', 'INDOOR_DEPLOYMENT', 'ANTENNA_AZIMUTH', 'ANTENNA_DOWNTILT', 'ANTENNA_GAIN', 'EIRP_CAPABILITY', 'ANTENNA_BEAMWIDTH', 'ANTENNA_MODEL', 'MEAS_CAPABILITY_AVAILABLE', 'GROUP_TYPE', 'MEAS_REPORT_REQUESTED', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result_2023_10_EV_SAS_REGISTRATION, headers)

    def test_that_can_parse_schema_2023_10_for_EV_SAS_SPECTRUMINQUIRY(self):
        headers = self.obj_under_test.get_headers("2023,10", "EV_SAS_SPECTRUMINQUIRY")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'REQUESTED_CHANNELS', 'AVAILABLE_CHANNELS_TYPE', 'RESPONSE_MAXEIRP', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_10_for_EV_SAS_GRANT(self):
        headers = self.obj_under_test.get_headers("2023,10", "EV_SAS_GRANT")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE','CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'REQUESTED_CHANNEL', 'REQUESTED_MAXEIRP', 'GRANT_ID', 'GRANT_EXPIRE_TIME', 'HEARTBEAT_INTERVAL', 'MEAS_REPORT_REQUESTED', 'RESPONSE_MAXEIRP', 'RESPONSE_CHANNEL', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_10_for_EV_SAS_RELINQUISHMENT(self):
        headers = self.obj_under_test.get_headers("2023,10", "EV_SAS_RELINQUISHMENT")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'GRANT_ID', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_10_for_EV_SAS_DEREGISTRATION(self):
        headers = self.obj_under_test.get_headers("2023,10", "EV_SAS_DEREGISTRATION")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_10_for_EV_SAS_DEREGISTRATION_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2023,10", "EV_SAS_DEREGISTRATION_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_10_for_EV_SAS_HEARTBEAT(self):
        headers = self.obj_under_test.get_headers("2023,10", "EV_SAS_HEARTBEAT")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'GRANT_ID', 'GRANT_RENEW', 'OPERATION_STATE', 'MEAS_REPORT_RETURNED', 'REQUESTED_MAXEIRP', 'TRANSMIT_EXPIRE_TIME', 'GRANT_EXPIRE_TIME', 'HEARTBEAT_INTERVAL', 'RESPONSE_MAXEIRP', 'RESPONSE_CHANNEL', 'CHANNEL_TYPE', 'MEAS_REPORT_REQUESTED', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_10_for_EV_SAS_HEARTBEAT_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2023,10", "EV_SAS_HEARTBEAT_TRANS_ERROR")
        expected_result = ["EVENT_NAME","EVENT_OCCURRED_ON","EVENT_ID","EVENT_VERSION","EVENT_TIME","EVENT_SOURCE","CBSD_SERIAL_NUMBER","CBSD_ID","GROUP_ID","GRANT_ID","GRANT_RENEW","MEAS_REPORT_RETURNED","OPERATION_STATE","ERROR_CODE","ERROR_TYPE","ERROR_DESCRIPTION"]
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_10_for_EV_SAS_SPECTRUMINQUIRY_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2023,10", "EV_SAS_SPECTRUMINQUIRY_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'REQUESTED_CHANNELS', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_10_for_EV_SAS_RELINQUISHMENT_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2023,10", "EV_SAS_RELINQUISHMENT_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'GRANT_ID', 'CHANNEL', 'CHANNEL_TYPE', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_10_for_EV_SAS_REGISTRATION_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2023,10","EV_SAS_REGISTRATION_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'GROUP_ID', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_10_for_EV_SAS_GRANT_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers("2023,10", "EV_SAS_GRANT_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'CHANNEL', 'MAX_EIRP', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION']
        self.assertEqual(expected_result, headers)

    def test_that_can_parse_schema_2023_10_for_EV_EUTRANCELLTDD_RECONFIG(self):
        headers = self.obj_under_test.get_headers("2023,10","EV_EUTRANCELLTDD_RECONFIG")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FDN', 'BANDWIDTH', 'EARFCN', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE']
        self.assertEqual(expected_result, headers)

    def test_tat_can_parse_csv_scema_2023_10_for_EV_SAS_SPECTRUMINQUIRY_TRANS_ERROR(self):
        headers = self.obj_under_test.get_headers_csv("2023,10", "EV_SAS_SPECTRUMINQUIRY_TRANS_ERROR")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FCC_ID', 'USER_ID', 'CBSD_CATEGORY', 'RADIO_TECHNOLOGY', 'LATITUDE', 'LONGITUDE', 'HEIGHT', 'HEIGHT_TYPE', 'HORIZONTAL_ACCURACY', 'VERTICAL_ACCURACY', 'INDOOR_DEPLOYMENT', 'ANTENNA_AZIMUTH', 'ANTENNA_DOWNTILT', 'ANTENNA_GAIN', 'EIRP_CAPABILITY', 'ANTENNA_BEAMWIDTH', 'ANTENNA_MODEL', 'MEAS_CAPABILITY_AVAILABLE', 'GROUP_TYPE', 'MEAS_REPORT_REQUESTED', 'REQUESTED_CHANNELS', 'AVAILABLE_CHANNELS_TYPE', 'RESPONSE_MAXEIRP', 'REQUESTED_CHANNEL', 'REQUESTED_MAXEIRP', 'GRANT_ID', 'GRANT_EXPIRE_TIME', 'HEARTBEAT_INTERVAL', 'RESPONSE_CHANNEL', 'GRANT_RENEW', 'OPERATION_STATE', 'MEAS_REPORT_RETURNED', 'TRANSMIT_EXPIRE_TIME', 'TRANSMIT_EXPIRE_TIME_VALUE', 'CHANNEL_TYPE', 'CHANNEL', 'MAX_EIRP', 'FDN', 'BANDWIDTH', 'EARFCN', 'ADDITIONAL_CBSD_IDS', 'ADDITIONAL_CBSD_SERIAL_NUMBERS', 'CAUSE_CODE', 'CONFIGURED_MAX_TX_POWER', 'MAX_ALLOWED_EIRP_PSD', 'ARFCN_UL', 'ARFCN_DL', 'BSCHANNEL_BW_DL', 'BSCHANNEL_BW_UL', 'ERROR_CODE', 'ERROR_TYPE', 'ERROR_DESCRIPTION', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA']
        self.assertEqual(expected_result, headers)

    def test_tat_can_parse_csv_scema_2023_5_for_EV_SAS_REGISTRATION(self):
        headers = self.obj_under_test.get_headers_csv("2023,5", "EV_SAS_REGISTRATION")
        expected_result = ['EVENT_NAME', 'EVENT_OCCURRED_ON', 'EVENT_ID', 'EVENT_VERSION', 'EVENT_TIME', 'EVENT_SOURCE', 'CBSD_SERIAL_NUMBER', 'CBSD_ID', 'GROUP_ID', 'FCC_ID', 'USER_ID', 'CBSD_CATEGORY', 'RADIO_TECHNOLOGY', 'LATITUDE', 'LONGITUDE', 'HEIGHT', 'HEIGHT_TYPE', 'HORIZONTAL_ACCURACY', 'VERTICAL_ACCURACY', 'INDOOR_DEPLOYMENT', 'ANTENNA_AZIMUTH', 'ANTENNA_DOWNTILT', 'ANTENNA_GAIN', 'EIRP_CAPABILITY', 'ANTENNA_BEAMWIDTH', 'ANTENNA_MODEL', 'MEAS_CAPABILITY', 'GROUP_TYPE', 'MEAS_REPORT', 'RESPONSE_CODE', 'RESPONSE_MESSAGE', 'RESPONSE_DATA', 'REQUESTED_CHANNELS', 'CHANNEL_TYPE', 'MAXEIRP', 'REQUESTED_GRANT_CHANNEL', 'REQUESTED_MAXEIRP', 'GRANT_ID', 'GRANT_EXPIRE_TIME', 'HEARTBEAT_INTERVAL', 'MEASUREMENT_REPORT_REQUESTED', 'RESPONSE_MAXEIRP', 'RESPONSE_CHANNEL']
        self.assertEqual(expected_result, headers)

    def test_that_get_headers_return_error_message_when_wrong_version_found(self):
        result = self.obj_under_test.get_headers("2011,03", "EV_SC_RECONFIG")
        self.assertEqual(result, "Warning: Version 2011,03 not found in schemas.")

    def test_that_get_headers_return_error_message_when_none_version_found(self):
        result = self.obj_under_test.get_headers(None, "EV_SC_RECONFIG")
        self.assertEqual(result, "Warning: Version None not found in schemas.")

    def test_that_get_headers_return_error_message_when_wrong_event(self):
        result = self.obj_under_test.get_headers("2023,10", "WHAT")
        self.assertEqual(result, "Warning: Event WHAT not found in schema for version 2023,10")

    def test_that_get_headers_csv_return_error_message_when_wrong_version_found(self):
        result = self.obj_under_test.get_headers_csv("2011,03", "EV_SAS_REGISTRATION")
        self.assertEqual(result, "Warning: Version 2011,03 not found in schemas.")

    def test_that_get_headers_csv_return_error_message_when_event_is_not_in_schema_16(self):
        result = self.obj_under_test.get_headers_csv("2023,16", "WHAT")
        self.assertEqual(result, "Warning: Event WHAT not found in schema for version 2023,16")

    def test_that_get_headers_csv_return_error_message_when_event_is_not_in_schema(self):
        result = self.obj_under_test.get_headers_csv("2023,10", "WHAT")
        self.assertEqual(result, "Warning: Event WHAT not found in schema for version 2023,10")

    def test_that_can_get_csv_parser_dic(self):
        dic = self.obj_under_test.get_csv_parser_dic("2023,10")

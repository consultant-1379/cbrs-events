import os
import sys
from datetime import datetime

path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "python")
if path not in sys.path:
    sys.path.append(path)
from unittest import TestCase
from cbrs_events import EventDecompressor
from freezegun import freeze_time


class TestEventDecompressor(TestCase):
    def setUp(self):
        self.objUnderTest = EventDecompressor()

    def test_decompress_radio_technology_E_ULTRA(self):
        radio_technology = self.objUnderTest._decompress_radio_technology("1")
        self.assertEqual("E_UTRA", radio_technology)

    def test_decompress_radio_technology_NR(self):
        radio_technology = self.objUnderTest._decompress_radio_technology("2")
        self.assertEqual("NR", radio_technology)

    def test_decompress_channel_type_GAA(self):
        channel_type = self.objUnderTest._decompress_channel_type("1")
        self.assertEqual("GAA", channel_type)

    def test_decompress_channel_type_PAL(self):
        channel_type = self.objUnderTest._decompress_channel_type("2")
        self.assertEqual("PAL", channel_type)

    def test_decompress_channel_type_NOT_PRESENT(self):
        channel_type = self.objUnderTest._decompress_channel_type("-1")
        self.assertEqual("NOT_PRESENT", channel_type)

    def test_decompress_channel_type_List(self):
        channel_type = self.objUnderTest._decompress_channel_type("[1,1,1,2,2,1,1,1,1,1,1,1,1,1,1]")
        self.assertEqual(
            "[GAA, GAA, GAA, PAL, PAL, GAA, GAA, GAA, GAA, GAA, GAA, GAA, GAA, GAA, GAA]",
            channel_type)

    def test_decompress_date_with_offset_plus_1(self):
        with freeze_time("2023-01-01 12:00:00", tz_offset=1):
            date = self.objUnderTest._format_date("1688757154768")
            print("q",date)
            self.assertEqual("2023-07-07T20:12:34+01:00", date)

    def test_decompress_date_with_offset_plus_0(self):
        with freeze_time("2023-01-01 12:00:00", tz_offset=0):
            date = self.objUnderTest._format_date("1699197433787")
            self.assertEqual("2023-11-05T15:17:13+00:00", date)

    @freeze_time("2023-01-01 12:00:00", tz_offset=1)
    def test_decompress_date_with_no_offset(self):
        date = self.objUnderTest._format_date("1688757154768")
        self.assertEqual("2023-07-07T20:12:34+01:00", date)

    def test_get_value_when_no_need_decompress_23_5(self):
        decompressed = self.objUnderTest.get_processed_value("CBSD_SERIAL_NUMBER", "D829153166", "2023,5")
        self.assertEqual("D829153166", decompressed)

    def test_get_value_when_no_need_decompress_23_10(self):
        decompressed = self.objUnderTest.get_processed_value("CBSD_SERIAL_NUMBER", "D829153166", "2023,10")
        self.assertEqual("D829153166", decompressed)

    @freeze_time("2023-01-01 12:00:00", tz_offset=1)
    def test_get_value_when_decompress_time_23_10(self):
        decompressed = self.objUnderTest.get_processed_value("EVENT_TIME", "1688749235277", "2023,10")
        self.assertEqual("2023-07-07T18:00:35+01:00", decompressed)

    def test_that_can_get_date_with_custom_offset(self):
        self.objUnderTest = EventDecompressor("2023-08-01T23:34:58.834-07:00")
        decompressed_date = self.objUnderTest.get_processed_value("EVENT_TIME", "1690958098813", "2023,10")
        self.assertEqual("2023-08-01T23:34:58-07:00", decompressed_date)

    def test_get_value_when_decompress_meas_23_10(self):
        decompressed = self.objUnderTest.get_processed_value("MEAS_REQUESTED", "[1,2]", "2023,10")
        self.assertEqual("[RECEIVED_POWER_WITHOUT_GRANT,RECEIVED_POWER_WITH_GRANT]", decompressed)

    def test_get_value_when_decompress_meas_23_10_when_not_present(self):
        decompressed = self.objUnderTest.get_processed_value("MEASUREMENT_REPORT_REQUESTED", "[0,0]", "2023,10")
        self.assertEqual("", decompressed)

    def test_get_value_when_processed_radio_technology_23_10(self):
        decompressed = self.objUnderTest.get_processed_value("RADIO_TECHNOLOGY", "2", "2023,10")
        self.assertEqual("NR", decompressed)

    def test_get_value_when_processed_meas_23_10(self):
        decompressed = self.objUnderTest.get_processed_value("MEAS_REQUESTED", "[1,0]", "2023,10")
        self.assertEqual("[RECEIVED_POWER_WITHOUT_GRANT]", decompressed)

    def test_get_value_when_processed_channel_type_23_10(self):
        decompressed = self.objUnderTest.get_processed_value("CHANNEL_TYPE", "2", "2023,10")
        self.assertEqual("PAL", decompressed)

    def test_get_value_when_processed_meas_not_present_23_10(self):
        decompressed = self.objUnderTest.get_processed_value("MEASUREMENT_REPORT_REQUESTED", "[0,0]", "2023,10")
        self.assertEqual("", decompressed)

    def test_get_value_when_processed_maxeirp_NOT_PRESENT_23_10(self):
        decompressed = self.objUnderTest.get_processed_value("RESPONSE_MAXEIRP", "-138", "2023,10")
        self.assertEqual("NOT_PRESENT", decompressed)

    def test_get_value_when_processed_maxeirp_38_23_10(self):
        decompressed = self.objUnderTest.get_processed_value("RESPONSE_MAXEIRP", "38", "2023,10")
        self.assertEqual("38", decompressed)

    def test_get_value_when_processed_maxeirp_NOT_PRESENT_23_5(self):
        decompressed = self.objUnderTest.get_processed_value("RESPONSE_MAXEIRP", "-138", "2023,5")
        self.assertEqual("NOT_PRESENT", decompressed)

    def test_get_value_when_processed_maxeirp_38_23_5(self):
        decompressed = self.objUnderTest.get_processed_value("RESPONSE_MAXEIRP", "38", "2023,5")
        self.assertEqual("38", decompressed)

    def test_get_value_when_processed_maxeirp_38_list_23_10(self):
        decompressed = self.objUnderTest.get_processed_value("RESPONSE_MAXEIRP", "[-138.0, -138.0, -138.0, -138.0, "
                                                                                 "-138.0, -138.0, -138.0, -138.0, -138.0, "
                                                                                 "-138.0, -138.0, -138.0, -138.0, -138.0, "
                                                                                 "-138]", "2023,10")
        self.assertEqual("[NOT_PRESENT, NOT_PRESENT, NOT_PRESENT, NOT_PRESENT, NOT_PRESENT, NOT_PRESENT, NOT_PRESENT,"
                         " NOT_PRESENT, NOT_PRESENT, NOT_PRESENT, NOT_PRESENT, NOT_PRESENT, NOT_PRESENT, NOT_PRESENT,"
                         " NOT_PRESENT]", decompressed)

    def test_get_value_when_processed_maxeirp_38_list_23_5(self):
        decompressed = self.objUnderTest.get_processed_value("RESPONSE_MAXEIRP", "[-138.0, -138.0, -138.0, -138.0, "
                                                                                 "-138.0, -138.0, -138.0, -138.0, -138.0, "
                                                                                 "-138.0, -138.0, -138.0, -138.0, -138.0, "
                                                                                 "-138]", "2023,5")
        self.assertEqual("[NOT_PRESENT, NOT_PRESENT, NOT_PRESENT, NOT_PRESENT, NOT_PRESENT, NOT_PRESENT, NOT_PRESENT,"
                         " NOT_PRESENT, NOT_PRESENT, NOT_PRESENT, NOT_PRESENT, NOT_PRESENT, NOT_PRESENT, NOT_PRESENT,"
                         " NOT_PRESENT]", decompressed)

    def test_get_value_when_processed_rank_23_16(self):
        decompressed = self.objUnderTest.get_processed_value("CELL_1_RANK", "-1", "2023,16")
        self.assertEqual("NOT_PRESENT", decompressed)

    def test_get_value_when_processed_preferred_bandwidth_23_16(self):
        decompressed = self.objUnderTest.get_processed_value("CELL_1_PREFERRED_BANDWIDTH", "0", "2023,16")
        self.assertEqual("NOT_PRESENT", decompressed)

    def test_get_value_when_processed_cellid_23_16(self):
        decompressed = self.objUnderTest.get_processed_value("CELL_2_ID", "", "2023,16")
        self.assertEqual("NOT_PRESENT", decompressed)

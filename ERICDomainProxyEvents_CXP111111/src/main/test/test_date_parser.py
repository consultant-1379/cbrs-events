import os
import sys
path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "python")
if path not in sys.path:
    sys.path.append(path)
from cbrs_events import DateParserHelper
import unittest


class TestDateParserHelper(unittest.TestCase):

    def test_get_seconds_with_milliseconds(self):
        self.assertEqual(DateParserHelper.generate_comparative_list("2023-07-31T12:34:56.789+00:00"),
                         [2023, 7, 31, 12, 34, 56])

    def test_splitdate_with_microseconds(self):
        date, offset_symbol, hours, minutes = DateParserHelper.split_date("2023-07-31T12:34:56.789+01:30")
        self.assertEqual("2023-07-31T12:34:56", date)
        self.assertEqual("+", offset_symbol)
        self.assertEqual("01", hours)
        self.assertEqual("30", minutes)

    def test_splitdate_without_microseconds(self):
        date, offset_symbol, hours, minutes = DateParserHelper.split_date("2023-07-31T12:34:56-01:00")
        self.assertEqual("2023-07-31T12:34:56", date)
        self.assertEqual("-", offset_symbol)
        self.assertEqual("01", hours)
        self.assertEqual("00", minutes)

    def test_get_generate_list_without_milliseconds(self):
        self.assertEqual(DateParserHelper.generate_comparative_list("2000-02-29T00:00:00+02:00"),
                         [2000, 2, 28, 22, 0, 0])
        self.assertEqual(DateParserHelper.generate_comparative_list("2023-07-31T12:34:56-01:00"),
                         [2023, 7, 31, 13, 34, 56])

    def test_that_get_seconds_return_smaller_number_for_smaller_date_with_milli_short_range(self):
        date_number_1 = DateParserHelper.generate_comparative_list("2023-07-23T00:26:01.801926+01:00")
        date_number_2 = DateParserHelper.generate_comparative_list("2023-07-23T00:26:02.931982+01:00")
        self.assertGreater(date_number_2, date_number_1)

    def test_that_get_seconds_return_smaller_number_for_smaller_date_with_milli_long_range(self):
        date_number_1 = DateParserHelper.generate_comparative_list("2023-07-23T00:26:02.818800+01:00")
        date_number_2 = DateParserHelper.generate_comparative_list("2023-07-23T23:25:37.739+01:00")
        self.assertGreater(date_number_2, date_number_1)

    def test_that_get_seconds_return_smaller_number_for_smaller_date_with_milli_long_range_1(self):
        date_number_1 = DateParserHelper.generate_comparative_list("2023-07-28T00:00:00.903-05:00")
        date_number_2 = DateParserHelper.generate_comparative_list("2023-07-28T00:00:05.120-05:00")
        self.assertGreater(date_number_2, date_number_1)

    def test_that_get_seconds_return_smaller_number_for_smaller_date(self):
        date_number_1 = DateParserHelper.generate_comparative_list("2023-07-23T00:26:01+01:00")
        date_number_2 = DateParserHelper.generate_comparative_list("2023-07-23T00:26:02+01:00")
        self.assertGreater(date_number_2, date_number_1)

    def test_that_can_parse_input_date(self):
        date_number_1 = DateParserHelper.generate_comparative_list("2023-07-27T07:23:43+01:00")
        date_number_2 = DateParserHelper.generate_comparative_list("2023-07-27T07:23:44+01:00")
        date_nubmer_log = DateParserHelper.generate_comparative_list("2023-07-27T07:07:09.996582+01:00")
        self.assertGreater(date_number_2, date_number_1)
        self.assertGreater(date_number_2, date_nubmer_log)

    def test_with_different_offset_equal(self):
        date_number_1 = DateParserHelper.generate_comparative_list("2023-08-03T10:45:00-07:00")
        date_number_2 = DateParserHelper.generate_comparative_list("2023-08-03T12:45:00-05:00")
        self.assertEqual(date_number_1, date_number_2)

    def test_with_different_offset_greater(self):
        date_number_1 = DateParserHelper.generate_comparative_list("2023-07-22T18:26:01-05:00")
        date_number_2 = DateParserHelper.generate_comparative_list("2023-07-22T16:26:02-07:00")
        self.assertGreater(date_number_2, date_number_1)

    def test_with_different_offset_equal_1(self):
        date_number_1 = DateParserHelper.generate_comparative_list("2023-08-09T05:46:00+01:00")
        date_number_2 = DateParserHelper.generate_comparative_list("2023-08-08T23:46:00-05:00")
        self.assertEqual(date_number_1, date_number_2)

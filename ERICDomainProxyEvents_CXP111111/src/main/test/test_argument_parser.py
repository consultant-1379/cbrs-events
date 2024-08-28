import os
import sys

path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "python")
if path not in sys.path:
    sys.path.append(path)
from unittest import TestCase
from cbrs_events import ArgumentParser


class TestArgumentParser(TestCase):

    def setUp(self):
        self.parser = ArgumentParser()

    def test_process_arguments_no_args(self):
        self.assertRaises(SystemExit, self.parser.process_arguments, [])

    def test_process_arguments_two_args(self):
        self.assertRaises(SystemExit, self.parser.process_arguments, ['group1', 'start1'])

    def test_process_arguments_one_arg(self):
        result = self.parser.process_arguments(['group1'])
        self.assertEqual(('group1', None, None), result)

    def test_process_arguments_three_args_with_wrong_date(self):
        self.assertRaises(SystemExit, self.parser.process_arguments, ['group1', 'start1', 'end1'])

    def test_process_arguments_three_args_with_date_plus(self):
        result = self.parser.process_arguments(['group1', "2015-08-05T12:00:00-04:00", "2015-08-05T14:00:00-04:00"])


import os
import sys

path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "python")
if path not in sys.path:
    sys.path.append(path)
from cbrs_events import ElasticSearchIterator
from cbrs_events import DDPIterator
import unittest
from unittest.mock import Mock

RESPONSE_2023 = '{"took":11,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":17,"max_score":null,"hits":[{"_index":"enm_logs-application-2023.07.07","_type":"events","_id":"Df9MMYkBqXW8qXYnQmXD","_score":null,"_source":{"timestamp":"2023-07-07T18:00:28.707+01:00","host":"svc-4-dpmediation","program":"JBOSS","severity":"info","severity_code":6,"facility":"local2","facility_code":18,"pri":150,"tag":"JBOSS[20587]","message":"[com.ericsson.oss.itpf.EVENT_DATA_LOGGER] EV_SAS_RELINQUISHMENT {\"event-information\":\"13;2023,5;1688749228659;sas-system;D829137636;sas1\/cbsd732;LTE40dg2ERBS00001:LTE40dg2ERBS00001-1,LTE40dg2ERBS00001-2,LTE40dg2ERBS00001-3,LTE40dg2ERBS00001-7,LTE40dg2ERBS00001-8;sas1\/grant29048;0;NOT_SET;[NOT_SET]\"}"},"sort":[1688749228707]},{"_index":"enm_logs-application-2023.07.07","_type":"events","_id":"Cf9MMYkBqXW8qXYnQmW6","_score":null,"_source":{"timestamp":"2023-07-07T18:00:28.706+01:00","host":"svc-4-dpmediation","program":"JBOSS","severity":"info","severity_code":6,"facility":"local2","facility_code":18,"pri":150,"tag":"JBOSS[20587]","message":"[com.ericsson.oss.itpf.EVENT_DATA_LOGGER] EV_SAS_RELINQUISHMENT {\"event-information\":\"13;2023,5;1688749228659;sas-system;D829137636;sas1\/cbsd732;LTE40dg2ERBS00001:LTE40dg2ERBS00001-1,LTE40dg2ERBS00001-2,LTE40dg2ERBS00001-3,LTE40dg2ERBS00001-7,LTE40dg2ERBS00001-8;sas1\/grant29040;0;NOT_SET;[NOT_SET]\"}"},"sort":[1688749228706]},{"_index":"enm_logs-application-2023.07.07","_type":"events","_id":"Cv9MMYkBqXW8qXYnQmXD","_score":null,"_source":{"timestamp":"2023-07-07T18:00:28.706+01:00","host":"svc-4-dpmediation","program":"JBOSS","severity":"info","severity_code":6,"facility":"local2","facility_code":18,"pri":150,"tag":"JBOSS[20587]","message":"[com.ericsson.oss.itpf.EVENT_DATA_LOGGER] EV_SAS_RELINQUISHMENT {\"event-information\":\"13;2023,5;1688749228659;sas-system;D829137636;sas1\/cbsd732;LTE40dg2ERBS00001:LTE40dg2ERBS00001-1,LTE40dg2ERBS00001-2,LTE40dg2ERBS00001-3,LTE40dg2ERBS00001-7,LTE40dg2ERBS00001-8;sas1\/grant29045;0;NOT_SET;[NOT_SET]\"}"},"sort":[1688749228706]},{"_index":"enm_logs-application-2023.07.07","_type":"events","_id":"C_9MMYkBqXW8qXYnQmXD","_score":null,"_source":{"timestamp":"2023-07-07T18:00:28.706+01:00","host":"svc-4-dpmediation","program":"JBOSS","severity":"info","severity_code":6,"facility":"local2","facility_code":18,"pri":150,"tag":"JBOSS[20587]","message":"[com.ericsson.oss.itpf.EVENT_DATA_LOGGER] EV_SAS_RELINQUISHMENT {\"event-information\":\"13;2023,5;1688749228659;sas-system;D829137636;sas1\/cbsd732;LTE40dg2ERBS00001:LTE40dg2ERBS00001-1,LTE40dg2ERBS00001-2,LTE40dg2ERBS00001-3,LTE40dg2ERBS00001-7,LTE40dg2ERBS00001-8;sas1\/grant29041;0;NOT_SET;[NOT_SET]\"}"},"sort":[1688749228706]},{"_index":"enm_logs-application-2023.07.07","_type":"events","_id":"DP9MMYkBqXW8qXYnQmXD","_score":null,"_source":{"timestamp":"2023-07-07T18:00:28.706+01:00","host":"svc-4-dpmediation","program":"JBOSS","severity":"info","severity_code":6,"facility":"local2","facility_code":18,"pri":150,"tag":"JBOSS[20587]","message":"[com.ericsson.oss.itpf.EVENT_DATA_LOGGER] EV_SAS_RELINQUISHMENT {\"event-information\":\"13;2023,5;1688749228659;sas-system;D829137636;sas1\/cbsd732;LTE40dg2ERBS00001:LTE40dg2ERBS00001-1,LTE40dg2ERBS00001-2,LTE40dg2ERBS00001-3,LTE40dg2ERBS00001-7,LTE40dg2ERBS00001-8;sas1\/grant29046;0;NOT_SET;[NOT_SET]\"}"},"sort":[1688749228706]},{"_index":"enm_logs-application-2023.07.07","_type":"events","_id":"_f9MMYkBqXW8qXYnQmS6","_score":null,"_source":{"timestamp":"2023-07-07T18:00:28.705+01:00","host":"svc-4-dpmediation","program":"JBOSS","severity":"info","severity_code":6,"facility":"local2","facility_code":18,"pri":150,"tag":"JBOSS[20587]","message":"[com.ericsson.oss.itpf.EVENT_DATA_LOGGER] EV_SAS_RELINQUISHMENT {\"event-information\":\"13;2023,5;1688749228659;sas-system;D829137636;sas1\/cbsd732;LTE40dg2ERBS00001:LTE40dg2ERBS00001-1,LTE40dg2ERBS00001-2,LTE40dg2ERBS00001-3,LTE40dg2ERBS00001-7,LTE40dg2ERBS00001-8;sas1\/grant29038;0;NOT_SET;[NOT_SET]\"}"},"sort":[1688749228705]},{"_index":"enm_logs-application-2023.07.07","_type":"events","_id":"CP9MMYkBqXW8qXYnQmW6","_score":null,"_source":{"timestamp":"2023-07-07T18:00:28.705+01:00","host":"svc-4-dpmediation","program":"JBOSS","severity":"info","severity_code":6,"facility":"local2","facility_code":18,"pri":150,"tag":"JBOSS[20587]","message":"[com.ericsson.oss.itpf.EVENT_DATA_LOGGER] EV_SAS_RELINQUISHMENT {\"event-information\":\"13;2023,5;1688749228659;sas-system;D829137636;sas1\/cbsd732;LTE40dg2ERBS00001:LTE40dg2ERBS00001-1,LTE40dg2ERBS00001-2,LTE40dg2ERBS00001-3,LTE40dg2ERBS00001-7,LTE40dg2ERBS00001-8;sas1\/grant29039;0;NOT_SET;[NOT_SET]\"}"},"sort":[1688749228705]},{"_index":"enm_logs-application-2023.07.07","_type":"events","_id":"-v9MMYkBqXW8qXYnQmSv","_score":null,"_source":{"timestamp":"2023-07-07T18:00:28.704+01:00","host":"svc-4-dpmediation","program":"JBOSS","severity":"info","severity_code":6,"facility":"local2","facility_code":18,"pri":150,"tag":"JBOSS[20587]","message":"[com.ericsson.oss.itpf.EVENT_DATA_LOGGER] EV_SAS_RELINQUISHMENT {\"event-information\":\"13;2023,5;1688749228659;sas-system;D829137636;sas1\/cbsd732;LTE40dg2ERBS00001:LTE40dg2ERBS00001-1,LTE40dg2ERBS00001-2,LTE40dg2ERBS00001-3,LTE40dg2ERBS00001-7,LTE40dg2ERBS00001-8;sas1\/grant29035;0;NOT_SET;[NOT_SET]\"}"},"sort":[1688749228704]},{"_index":"enm_logs-application-2023.07.07","_type":"events","_id":"-_9MMYkBqXW8qXYnQmSv","_score":null,"_source":{"timestamp":"2023-07-07T18:00:28.704+01:00","host":"svc-4-dpmediation","program":"JBOSS","severity":"info","severity_code":6,"facility":"local2","facility_code":18,"pri":150,"tag":"JBOSS[20587]","message":"[com.ericsson.oss.itpf.EVENT_DATA_LOGGER] EV_SAS_RELINQUISHMENT {\"event-information\":\"13;2023,5;1688749228659;sas-system;D829137636;sas1\/cbsd732;LTE40dg2ERBS00001:LTE40dg2ERBS00001-1,LTE40dg2ERBS00001-2,LTE40dg2ERBS00001-3,LTE40dg2ERBS00001-7,LTE40dg2ERBS00001-8;sas1\/grant29034;0;NOT_SET;[NOT_SET]\"}"},"sort":[1688749228704]},{"_index":"enm_logs-application-2023.07.07","_type":"events","_id":"_P9MMYkBqXW8qXYnQmS6","_score":null,"_source":{"timestamp":"2023-07-07T18:00:28.704+01:00","host":"svc-4-dpmediation","program":"JBOSS","severity":"info","severity_code":6,"facility":"local2","facility_code":18,"pri":150,"tag":"JBOSS[20587]","message":"[com.ericsson.oss.itpf.EVENT_DATA_LOGGER] EV_SAS_RELINQUISHMENT {\"event-information\":\"13;2023,5;1688749228659;sas-system;D829137636;sas1\/cbsd732;LTE40dg2ERBS00001:LTE40dg2ERBS00001-1,LTE40dg2ERBS00001-2,LTE40dg2ERBS00001-3,LTE40dg2ERBS00001-7,LTE40dg2ERBS00001-8;sas1\/grant29036;0;NOT_SET;[NOT_SET]\"}"},"sort":[1688749228704]}]}}'


class TestElasticSearchIterator(unittest.TestCase):
    def setUp(self):
        # Create a mock response with some test data
        self.response = Mock()
        self.response.json.return_value = {
            "hits": {
                "hits": [
                    {"_source": {"message": 'EV_SAS_test1 "event-information":"info1;info2;group_id;info3"',
                                 "host": "svc-4"
                                         "-dpmediation"}},
                    {"_source": {"message": 'EV_OTHER_test2 "event-information":"info4;info5;group_id;info6"', "host":
                        "svc-4-dpmediation"}},
                    {"_source": {"message": 'EV_OTHER1_test3 "event-information":"info7;info8;group_id;info9"', "host":
                        "svc-4-dpmediation"}}
                ]
            }
        }

    def test_iteration(self):
        iterator = ElasticSearchIterator(self.response, "group_id")
        event1 = next(iterator)
        self.assertEqual(event1, ('EV_OTHER1_test3', "svc-4-dpmediation", ['info7', 'info8', "group_id", 'info9']))
        event2 = next(iterator)
        self.assertEqual(event2, ('EV_OTHER_test2', "svc-4-dpmediation", ['info4', 'info5', "group_id", 'info6']))
        event3 = next(iterator)
        self.assertEqual(event3, ('EV_SAS_test1', "svc-4-dpmediation", ['info1', 'info2', "group_id", 'info3']))

    def test_stop_iteration(self):
        iterator = ElasticSearchIterator(self.response, "group_id")

        while True:
            try:
                next(iterator)
            except StopIteration:
                break
        with self.assertRaises(StopIteration):
            next(iterator)


class TestDDPIterator(unittest.TestCase):

    def __init__(self, method_name: str = ...):
        super().__init__(method_name)
        self.test_directory = "test_files"

    def test_filter_log_files(self):
        iterator = DDPIterator(self.test_directory)
        log_files = iterator.filter_log_files(self.test_directory)
        self.assertEqual(len(log_files), 2)
        self.assertTrue(all(file.endswith('.csv.gz') for file in log_files))

    def test_read_full_file_with_split(self):
        iterator = DDPIterator("test_logs", "*")
        count = 0;
        for event, event_from, event_data in iterator:
            count += 1
        self.assertEqual(27, count)

    def test_iteration(self):
        iterator = DDPIterator(self.test_directory,
                               "ONRM_ROOT_MO|MKT_191|491425_FAIRVIEW:491425_FAIRVIEW:491425_7_6,491425_8_6,491425_9_6")
        events = list(iterator)

        expected_events = [
            ('EV_SAS_SPECTRUMINQUIRY', """dpmediation_1""",
             ["""4""", """2023,5""", """1690520400901""", """sas-system""", """null""",
              """TA8AKRC161711-1/8da7457578e7e76d4bc348d8f3addcec33dd9b58""",
              """ONRM_ROOT_MO|MKT_191|491425_FAIRVIEW:491425_FAIRVIEW:491425_7_6,491425_8_6,491425_9_6""",
              """[1, 2, 3, -1, -1, 6, 7, 8, -1, -1, 11, 12, 13, 14, 15]""",
              """[1, 1, 1, -1, -1, 1, 1, 2, -1, -1, 1, 1, 1, 1, 1]""",
              """[36.98, 36.98, 36.98, -138.0, -138.0, 36.98, 36.98, 37.0, -138.0, -138.0, 37.0, 37.0, 37.0, 37.0, 37.0]""",
              """0""", """NOT_SET""", """[NOT_SET]"""]),
            ('EV_SAS_SPECTRUMINQUIRY', 'dpmediation_1',
             ["""4""", """2023,5""", """1690520400901""", """sas-system""", """null""",
              """TA8AKRC161711-1/8caab2cab56e073c81f0e59277b76c0f47dcdc1f""",
              """ONRM_ROOT_MO|MKT_191|491425_FAIRVIEW:491425_FAIRVIEW:491425_7_6,491425_8_6,491425_9_6""",
              """[1, 2, 3, -1, -1, 6, 7, 8, -1, -1, 11, 12, 13, 14, 15]""",
              """[1, 1, 1, -1, -1, 1, 1, 2, -1, -1, 1, 1, 1, 1, 1]""",
              """[36.98, 36.98, 36.98, -138.0, -138.0, 36.98, 36.98, 37.0, -138.0, -138.0, 37.0, 37.0, 37.0, 37.0, 37.0]""",
              """0""", """NOT_SET""", """[NOT_SET]"""])
        ]
        assert (len(events), 4)
        for event, expected_event in zip(events, expected_events):
            self.assertEqual(event, expected_event)

    def test_start_end_date_filtering(self):
        start_date = "2023-07-28T00:00:03.552-05:00"  # Should filter out the first log entry
        end_date = "2023-07-28T00:00:06.552-05:00"  # Should filter out the last log entry
        iterator = DDPIterator(self.test_directory,
                               "ONRM_ROOT_MO|MKT_191|491908_PILLSBURY_HUB_SC_4:491908_PILLSBURY_HUB_SC_4:491908_7_6,491908_8_6,491908_9_6",
                               start_date=start_date, end_date=end_date)
        events = list(iterator)

        expected_events = [
            ('EV_SAS_SPECTRUMINQUIRY', 'svc-9-dpmediation',
             ["""4""", """2023,5""", """1690520404540""", """sas-system""", """null""",
              """TA8AKRC161711-1/8dd64df1b16ff9bb0c3e54f59a7de9d0c20e20f5""",
              """ONRM_ROOT_MO|MKT_191|491908_PILLSBURY_HUB_SC_4:491908_PILLSBURY_HUB_SC_4:491908_7_6,491908_8_6,491908_9_6""",
              """[1, 2, 3, -1, -1, 6, 7, 8, -1, -1, 11, 12, 13, 14, 15]""",
              """[1, 1, 1, -1, -1, 1, 1, 2, -1, -1, 1, 1, 1, 1, 1]""",
              """[37.0, 37.0, 37.0, -138.0, -138.0, 37.0, 37.0, 37.0, -138.0, -138.0, 37.0, 37.0, 37.0, 37.0, 37.0]""",
              """0""", """NOT_SET""", """[NOT_SET]"""])
        ]

        self.assertEqual(len(events), 2 * len(expected_events))
        for event, expected_event in zip(events, expected_events):
            self.assertEqual(event, expected_event)

import os
import sys
path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "python")
if path not in sys.path:
    sys.path.append(path)
from unittest import TestCase
from cbrs_events import HttpRequester
from unittest.mock import patch

RESPONSE_2023 = '{"took":11,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":17,"max_score":null,"hits":[{"_index":"enm_logs-application-2023.07.07","_type":"events","_id":"Df9MMYkBqXW8qXYnQmXD","_score":null,"_source":{"timestamp":"2023-07-07T18:00:28.707+01:00","host":"svc-4-dpmediation","program":"JBOSS","severity":"info","severity_code":6,"facility":"local2","facility_code":18,"pri":150,"tag":"JBOSS[20587]","message":"[com.ericsson.oss.itpf.EVENT_DATA_LOGGER] EV_SAS_RELINQUISHMENT {\"event-information\":\"13;2023,5;1688749228659;sas-system;D829137636;sas1\/cbsd732;LTE40dg2ERBS00001:LTE40dg2ERBS00001-1,LTE40dg2ERBS00001-2,LTE40dg2ERBS00001-3,LTE40dg2ERBS00001-7,LTE40dg2ERBS00001-8;sas1\/grant29048;0;NOT_SET;[NOT_SET]\"}"},"sort":[1688749228707]},{"_index":"enm_logs-application-2023.07.07","_type":"events","_id":"Cf9MMYkBqXW8qXYnQmW6","_score":null,"_source":{"timestamp":"2023-07-07T18:00:28.706+01:00","host":"svc-4-dpmediation","program":"JBOSS","severity":"info","severity_code":6,"facility":"local2","facility_code":18,"pri":150,"tag":"JBOSS[20587]","message":"[com.ericsson.oss.itpf.EVENT_DATA_LOGGER] EV_SAS_RELINQUISHMENT {\"event-information\":\"13;2023,5;1688749228659;sas-system;D829137636;sas1\/cbsd732;LTE40dg2ERBS00001:LTE40dg2ERBS00001-1,LTE40dg2ERBS00001-2,LTE40dg2ERBS00001-3,LTE40dg2ERBS00001-7,LTE40dg2ERBS00001-8;sas1\/grant29040;0;NOT_SET;[NOT_SET]\"}"},"sort":[1688749228706]},{"_index":"enm_logs-application-2023.07.07","_type":"events","_id":"Cv9MMYkBqXW8qXYnQmXD","_score":null,"_source":{"timestamp":"2023-07-07T18:00:28.706+01:00","host":"svc-4-dpmediation","program":"JBOSS","severity":"info","severity_code":6,"facility":"local2","facility_code":18,"pri":150,"tag":"JBOSS[20587]","message":"[com.ericsson.oss.itpf.EVENT_DATA_LOGGER] EV_SAS_RELINQUISHMENT {\"event-information\":\"13;2023,5;1688749228659;sas-system;D829137636;sas1\/cbsd732;LTE40dg2ERBS00001:LTE40dg2ERBS00001-1,LTE40dg2ERBS00001-2,LTE40dg2ERBS00001-3,LTE40dg2ERBS00001-7,LTE40dg2ERBS00001-8;sas1\/grant29045;0;NOT_SET;[NOT_SET]\"}"},"sort":[1688749228706]},{"_index":"enm_logs-application-2023.07.07","_type":"events","_id":"C_9MMYkBqXW8qXYnQmXD","_score":null,"_source":{"timestamp":"2023-07-07T18:00:28.706+01:00","host":"svc-4-dpmediation","program":"JBOSS","severity":"info","severity_code":6,"facility":"local2","facility_code":18,"pri":150,"tag":"JBOSS[20587]","message":"[com.ericsson.oss.itpf.EVENT_DATA_LOGGER] EV_SAS_RELINQUISHMENT {\"event-information\":\"13;2023,5;1688749228659;sas-system;D829137636;sas1\/cbsd732;LTE40dg2ERBS00001:LTE40dg2ERBS00001-1,LTE40dg2ERBS00001-2,LTE40dg2ERBS00001-3,LTE40dg2ERBS00001-7,LTE40dg2ERBS00001-8;sas1\/grant29041;0;NOT_SET;[NOT_SET]\"}"},"sort":[1688749228706]},{"_index":"enm_logs-application-2023.07.07","_type":"events","_id":"DP9MMYkBqXW8qXYnQmXD","_score":null,"_source":{"timestamp":"2023-07-07T18:00:28.706+01:00","host":"svc-4-dpmediation","program":"JBOSS","severity":"info","severity_code":6,"facility":"local2","facility_code":18,"pri":150,"tag":"JBOSS[20587]","message":"[com.ericsson.oss.itpf.EVENT_DATA_LOGGER] EV_SAS_RELINQUISHMENT {\"event-information\":\"13;2023,5;1688749228659;sas-system;D829137636;sas1\/cbsd732;LTE40dg2ERBS00001:LTE40dg2ERBS00001-1,LTE40dg2ERBS00001-2,LTE40dg2ERBS00001-3,LTE40dg2ERBS00001-7,LTE40dg2ERBS00001-8;sas1\/grant29046;0;NOT_SET;[NOT_SET]\"}"},"sort":[1688749228706]},{"_index":"enm_logs-application-2023.07.07","_type":"events","_id":"_f9MMYkBqXW8qXYnQmS6","_score":null,"_source":{"timestamp":"2023-07-07T18:00:28.705+01:00","host":"svc-4-dpmediation","program":"JBOSS","severity":"info","severity_code":6,"facility":"local2","facility_code":18,"pri":150,"tag":"JBOSS[20587]","message":"[com.ericsson.oss.itpf.EVENT_DATA_LOGGER] EV_SAS_RELINQUISHMENT {\"event-information\":\"13;2023,5;1688749228659;sas-system;D829137636;sas1\/cbsd732;LTE40dg2ERBS00001:LTE40dg2ERBS00001-1,LTE40dg2ERBS00001-2,LTE40dg2ERBS00001-3,LTE40dg2ERBS00001-7,LTE40dg2ERBS00001-8;sas1\/grant29038;0;NOT_SET;[NOT_SET]\"}"},"sort":[1688749228705]},{"_index":"enm_logs-application-2023.07.07","_type":"events","_id":"CP9MMYkBqXW8qXYnQmW6","_score":null,"_source":{"timestamp":"2023-07-07T18:00:28.705+01:00","host":"svc-4-dpmediation","program":"JBOSS","severity":"info","severity_code":6,"facility":"local2","facility_code":18,"pri":150,"tag":"JBOSS[20587]","message":"[com.ericsson.oss.itpf.EVENT_DATA_LOGGER] EV_SAS_RELINQUISHMENT {\"event-information\":\"13;2023,5;1688749228659;sas-system;D829137636;sas1\/cbsd732;LTE40dg2ERBS00001:LTE40dg2ERBS00001-1,LTE40dg2ERBS00001-2,LTE40dg2ERBS00001-3,LTE40dg2ERBS00001-7,LTE40dg2ERBS00001-8;sas1\/grant29039;0;NOT_SET;[NOT_SET]\"}"},"sort":[1688749228705]},{"_index":"enm_logs-application-2023.07.07","_type":"events","_id":"-v9MMYkBqXW8qXYnQmSv","_score":null,"_source":{"timestamp":"2023-07-07T18:00:28.704+01:00","host":"svc-4-dpmediation","program":"JBOSS","severity":"info","severity_code":6,"facility":"local2","facility_code":18,"pri":150,"tag":"JBOSS[20587]","message":"[com.ericsson.oss.itpf.EVENT_DATA_LOGGER] EV_SAS_RELINQUISHMENT {\"event-information\":\"13;2023,5;1688749228659;sas-system;D829137636;sas1\/cbsd732;LTE40dg2ERBS00001:LTE40dg2ERBS00001-1,LTE40dg2ERBS00001-2,LTE40dg2ERBS00001-3,LTE40dg2ERBS00001-7,LTE40dg2ERBS00001-8;sas1\/grant29035;0;NOT_SET;[NOT_SET]\"}"},"sort":[1688749228704]},{"_index":"enm_logs-application-2023.07.07","_type":"events","_id":"-_9MMYkBqXW8qXYnQmSv","_score":null,"_source":{"timestamp":"2023-07-07T18:00:28.704+01:00","host":"svc-4-dpmediation","program":"JBOSS","severity":"info","severity_code":6,"facility":"local2","facility_code":18,"pri":150,"tag":"JBOSS[20587]","message":"[com.ericsson.oss.itpf.EVENT_DATA_LOGGER] EV_SAS_RELINQUISHMENT {\"event-information\":\"13;2023,5;1688749228659;sas-system;D829137636;sas1\/cbsd732;LTE40dg2ERBS00001:LTE40dg2ERBS00001-1,LTE40dg2ERBS00001-2,LTE40dg2ERBS00001-3,LTE40dg2ERBS00001-7,LTE40dg2ERBS00001-8;sas1\/grant29034;0;NOT_SET;[NOT_SET]\"}"},"sort":[1688749228704]},{"_index":"enm_logs-application-2023.07.07","_type":"events","_id":"_P9MMYkBqXW8qXYnQmS6","_score":null,"_source":{"timestamp":"2023-07-07T18:00:28.704+01:00","host":"svc-4-dpmediation","program":"JBOSS","severity":"info","severity_code":6,"facility":"local2","facility_code":18,"pri":150,"tag":"JBOSS[20587]","message":"[com.ericsson.oss.itpf.EVENT_DATA_LOGGER] EV_SAS_RELINQUISHMENT {\"event-information\":\"13;2023,5;1688749228659;sas-system;D829137636;sas1\/cbsd732;LTE40dg2ERBS00001:LTE40dg2ERBS00001-1,LTE40dg2ERBS00001-2,LTE40dg2ERBS00001-3,LTE40dg2ERBS00001-7,LTE40dg2ERBS00001-8;sas1\/grant29036;0;NOT_SET;[NOT_SET]\"}"},"sort":[1688749228704]}]}}'
group_id = "DOM40dg2ERBS00276:DOM40dg2ERBS00276-1,DOM40dg2ERBS00276-2,DOM40dg2ERBS00276-3,DOM40dg2ERBS00276-7,DOM40dg2ERBS00276-8"
cell_id = "DOM40dg2ERBS00276"


class TestHttpRequester(TestCase):

    def setUp(self):
        self.requester = HttpRequester()
        self.cell_id = cell_id
        self.starting_date = '2023-07-01'
        self.end_date = '2023-07-31'

    @patch('requests.get')
    def test_get_events_without_starting_date(self, mock_get):
        mock_get.return_value = RESPONSE_2023

        response = self.requester.get_events(self.cell_id, None, None)

        self.assertEqual(response, RESPONSE_2023)
        mock_get.assert_called_once_with(HttpRequester.BASE_URL, params={
            'size': '10000',
            'sort': 'timestamp:desc',
            'q': HttpRequester.BASE_QUERY_NO_DATE.format(self.cell_id)
        })

    @patch('requests.get')
    def test_get_events_with_starting_date(self, mock_get):
        mock_get.return_value = RESPONSE_2023

        response = self.requester.get_events(self.cell_id, self.starting_date, self.end_date)

        self.assertEqual(response, RESPONSE_2023)
        mock_get.assert_called_once_with(HttpRequester.BASE_URL, params={
            'size': '10000',
            'sort': 'timestamp:desc',
            'q': HttpRequester.BASE_QUERY_WITH_DATE.format(self.cell_id, self.starting_date, self.end_date)
        })

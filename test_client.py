import sys
import os
import unittest
from unittest.mock import MagicMock, patch
import httpx


sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from client import ClusterClient


class TestClusterClient(unittest.TestCase):

    def setUp(self):
        self.hosts = ["http://node1.example.com", "http://node2.example.com","http://node3.example.com"]
        self.client = ClusterClient(hosts=self.hosts, timeout=2.0, max_retries=3, retry_backoff=0.01)

    def tearDown(self):
        self.client.close()

    # -------------------------------------------------------------------------
    # 1. Tests for check_all_nodes_ok (TCC Health Check)
    # -------------------------------------------------------------------------

    @patch.object(httpx.Client, 'get')
    def test_check_all_nodes_ok_success(self, mock_get):
        """TCC succeeds when all nodes return status code < 500."""
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        result = self.client.check_all_nodes_ok("group1")

        self.assertTrue(result)
        self.assertEqual(mock_get.call_count, len(self.hosts))

    @patch.object(httpx.Client, 'get')
    def test_check_all_nodes_ok_server_error(self, mock_get):
        """TCC fails immediately if any node returns status code >= 500."""
        mock_response_500 = MagicMock(spec=httpx.Response)
        mock_response_500.status_code = 500
        mock_get.return_value = mock_response_500

        result = self.client.check_all_nodes_ok("group1")

        self.assertFalse(result)

    @patch.object(httpx.Client, 'get')
    def test_check_all_nodes_ok_timeout(self, mock_get):
        """TCC fails if a network timeout occurs."""
        mock_get.side_effect = httpx.TimeoutException("Connection timed out")

        result = self.client.check_all_nodes_ok("group1")

        self.assertFalse(result)

    # -------------------------------------------------------------------------
    # 2. Tests for create_group_on_node (Single Node Helper)
    # -------------------------------------------------------------------------

    @patch.object(httpx.Client, 'post')
    def test_create_group_on_node_success(self, mock_post):
        """Single node create returns HTTP 201 response."""
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 201
        mock_post.return_value = mock_response

        res = self.client.create_group_on_node(self.hosts[0], "group1")

        self.assertIsNotNone(res)
        self.assertEqual(res.status_code, 201)
        mock_post.assert_called_once_with("http://node1.example.com/v1/group/", json={"groupId": "group1"})

    @patch.object(httpx.Client, 'post')
    def test_create_group_on_node_network_failure(self, mock_post):
        """Single node create returns None on network exception."""
        mock_post.side_effect = httpx.RequestError("Host unreachable")

        res = self.client.create_group_on_node(self.hosts[0], "group1")

        self.assertIsNone(res)

    # -------------------------------------------------------------------------
    # 3. Tests for delete_group_on_node_with_retry (Retry Mechanism)
    # -------------------------------------------------------------------------

    @patch('time.sleep', return_value=None)
    @patch.object(httpx.Client, 'send')
    @patch.object(httpx.Client, 'build_request')
    def test_delete_group_on_node_with_retry_success(self, mock_build, mock_send, mock_sleep):
        """Delete succeeds on first attempt."""
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200
        mock_send.return_value = mock_response

        res = self.client.delete_group_on_node_with_retry(self.hosts[0], "group1")

        self.assertIsNotNone(res)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(mock_send.call_count, 1)

    @patch('time.sleep', return_value=None)
    @patch.object(httpx.Client, 'send')
    @patch.object(httpx.Client, 'build_request')
    def test_delete_group_on_node_with_retry_client_error_no_retry(self, mock_build, mock_send, mock_sleep):
        """Client errors like 404 shouldn't be retried."""
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 404
        mock_send.return_value = mock_response

        res = self.client.delete_group_on_node_with_retry(self.hosts[0], "group1")

        self.assertIsNotNone(res)
        self.assertEqual(res.status_code, 404)
        self.assertEqual(mock_send.call_count, 1)

    @patch('time.sleep', return_value=None)
    @patch.object(httpx.Client, 'send')
    @patch.object(httpx.Client, 'build_request')
    def test_delete_group_on_node_with_retry_success_after_retries(self, mock_build, mock_send, mock_sleep):
        """Delete fails with 500 on attempt 1 & 2, succeeds on attempt 3."""
        resp_500 = MagicMock(spec=httpx.Response)
        resp_500.status_code = 500
        resp_200 = MagicMock(spec=httpx.Response)
        resp_200.status_code = 200

        mock_send.side_effect = [resp_500, resp_500, resp_200]

        res = self.client.delete_group_on_node_with_retry(self.hosts[0], "group1")

        self.assertIsNotNone(res)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(mock_send.call_count, 3)

    @patch('time.sleep', return_value=None)
    @patch.object(httpx.Client, 'send')
    @patch.object(httpx.Client, 'build_request')
    def test_delete_group_on_node_with_retry_exhausted(self, mock_build, mock_send, mock_sleep):
        """Delete fails all max_retries attempts, returning None."""
        mock_send.side_effect = httpx.TimeoutException("Timeout")

        res = self.client.delete_group_on_node_with_retry(self.hosts[0], "group1")

        self.assertIsNone(res)
        self.assertEqual(mock_send.call_count, self.client.max_retries)

    # -------------------------------------------------------------------------
    # 4. Tests for create_group (Saga Pattern & Rollback)
    # -------------------------------------------------------------------------

    @patch.object(ClusterClient, 'check_all_nodes_ok', return_value=False)
    def test_create_group_tcc_failed(self, mock_tcc):
        """create_group fails if TCC health check fails."""
        res = self.client.create_group("group1")

        self.assertEqual(res["status"], "TCC failed")


    @patch.object(ClusterClient, 'create_group_on_node')
    @patch.object(ClusterClient, 'check_all_nodes_ok', return_value=True)
    def test_create_group_success_all_nodes(self, mock_tcc, mock_create_node):
        """create_group succeeds across all nodes."""
        resp_201 = MagicMock(spec=httpx.Response)
        resp_201.status_code = 201
        resp_400 = MagicMock(spec=httpx.Response)
        resp_400.status_code = 400
        mock_create_node.side_effect = [resp_201,resp_400,resp_400]

        res = self.client.create_group("group1")

        self.assertEqual(res["status"], "success")
        self.assertEqual(mock_create_node.call_count, len(self.hosts))

    @patch.object(ClusterClient, 'delete_group_on_node_with_retry')
    @patch.object(ClusterClient, 'create_group_on_node')
    @patch.object(ClusterClient, 'check_all_nodes_ok', return_value=True)
    def test_create_group_saga_rollback_success(self, mock_tcc, mock_create_node, mock_del_node):
        """Creation fails on node 2; Saga rollback successfully deletes from node 1."""
        resp_201 = MagicMock(spec=httpx.Response)
        resp_201.status_code = 201

        resp_500 = MagicMock(spec=httpx.Response)
        resp_500.status_code = 500

        mock_create_node.side_effect = [resp_201, resp_500,resp_201]

        resp_del_200 = MagicMock(spec=httpx.Response)
        resp_del_200.status_code = 200
        mock_del_node.return_value = resp_del_200

        res = self.client.create_group("group1")

        self.assertEqual(res["status"], "rolled_back")
        self.assertEqual(res["failed_node"], "http://node2.example.com")
        mock_del_node.assert_called_once_with("http://node1.example.com", "group1")

    @patch.object(ClusterClient, 'delete_group_on_node_with_retry')
    @patch.object(ClusterClient, 'create_group_on_node')
    @patch.object(ClusterClient, 'check_all_nodes_ok', return_value=True)
    def test_create_group_saga_rollback_unstable(self, mock_tcc, mock_create_node, mock_del_node):
        """Creation fails on node 2; Saga rollback fails on node 1, leaving system UNSTABLE."""
        resp_201 = MagicMock(spec=httpx.Response)
        resp_201.status_code = 201

        resp_500 = MagicMock(spec=httpx.Response)
        resp_500.status_code = 500

        mock_create_node.side_effect = [resp_201,resp_201,resp_500]
        mock_del_node.return_value = None  # Delete failed after retries

        res = self.client.create_group("group1")
        self.assertEqual(res["status"], "unstable")
        self.assertEqual(["http://node1.example.com","http://node2.example.com"], res["remaining_nodes_after_rollback"])

    # -------------------------------------------------------------------------
    # 5. Tests for delete_group & get_group
    # -------------------------------------------------------------------------

    
    @patch.object(ClusterClient,'check_all_nodes_ok',return_value=False)
    def test_delete_group_tcc_failed(self,mock_tcc):
        res = self.client.delete_group("group1")
        self.assertEqual(res["status"], "TCC failed")

    @patch.object(ClusterClient, 'delete_group_on_node_with_retry')
    @patch.object(ClusterClient, 'check_all_nodes_ok', return_value=True)
    def test_delete_group_success(self, mock_tcc, mock_del_node):
        """delete_group succeeds on all nodes."""
        resp_200 = MagicMock(spec=httpx.Response)
        resp_200.status_code = 200
        mock_del_node.return_value = resp_200

        res = self.client.delete_group("group1")

        self.assertEqual(res["status"], "success")

    @patch.object(ClusterClient, 'delete_group_on_node_with_retry')
    @patch.object(ClusterClient, 'check_all_nodes_ok', return_value=True)
    def test_delete_group_unstable(self, mock_tcc, mock_del_node):
        """delete_group returns unstable if one node fails to delete after retries."""
        resp_200 = MagicMock(spec=httpx.Response)
        resp_200.status_code = 200
        resp_404 = MagicMock(spec=httpx.Response)
        resp_404.status_code = 404
        mock_del_node.side_effect = [resp_404, None,resp_200]

        res = self.client.delete_group("group1")

        self.assertEqual(res["status"], "unstable")
        self.assertEqual(res["failed_nodes"], ["http://node2.example.com"])

    @patch.object(httpx.Client, 'get')
    def test_get_group_success(self, mock_get):
        """get_group returns data from responding node."""
        resp_200 = MagicMock(spec=httpx.Response)
        resp_200.status_code = 200
        resp_200.json.return_value = {"groupId": "group1"}
        mock_get.return_value = resp_200

        res = self.client.get_group("group1")

        self.assertEqual(res["status"], "success")
        self.assertEqual(res["data"], {"groupId": "group1"})

    @patch.object(httpx.Client, 'get')
    def test_get_group_not_found(self, mock_get):
        """get_group returns not_found if group is missing on all nodes."""
        resp_404 = MagicMock(spec=httpx.Response)
        resp_404.status_code = 404
        mock_get.return_value = resp_404

        res = self.client.get_group("group1")

        self.assertEqual(res["status"], "not_found")


if __name__ == '__main__':
    unittest.main()

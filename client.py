import logging
import time
from typing import List, Dict, Any, Optional
import httpx

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("ClusterClient")

class ClusterClient:
    """
    Reliable Multi-Node Cluster API Client.
    Implements TCC health checks, Saga transaction rollbacks, and automatic retries
    to maintain cluster consistency across unstable API nodes.
    """

    def __init__(self, hosts: List[str], timeout: float = 5.0, max_retries: int = 3, retry_backoff: float = 1.0):
        """
        :param hosts: List of node base URLs (e.g., ['http://node1.example.com', 'http://node2.example.com'])
        :param timeout: Request timeout in seconds
        :param max_retries: Maximum number of retry attempts for critical operations (e.g., delete)
        :param retry_backoff: Backoff delay in seconds between retries
        """
        self.hosts = [h.rstrip('/') for h in hosts]
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.client = httpx.Client(timeout=self.timeout)

    # -------------------------------------------------------------------------
    # 1. Helper Functions (TCC Check, Single Node Create, Single Node Delete)
    # -------------------------------------------------------------------------

    def check_all_nodes_ok(self,group_id) -> bool:
        """
        TCC (Try-Confirm-Cancel) Phase 1 / Health Check.
        Verifies that all nodes in the cluster are reachable and operational.
        Returns True if all nodes respond with a non-server-error HTTP status, False otherwise.
        """
        logger.info("Starting TCC check: Verifying health of all cluster nodes...")
        for node in self.hosts:
            try:
                # We perform a lightweight GET request to test node responsiveness
                response = self.client.get(f"{node}/v1/group/{group_id}")
                # If node returns 5xx server error, consider the node unhealthy
                if response.status_code >= 500:
                    logger.warning(f"Health check failed for node {node}: Status {response.status_code}")
                    return False
            except (httpx.RequestError, httpx.TimeoutException) as e:
                logger.warning(f"Health check failed for node {node}: {str(e)}")
                return False
        logger.info("TCC check passed: All nodes are healthy and responding.")
        return True

    def create_group_on_node(self, node_url: str, group_id: str) -> Optional[httpx.Response]:
        """
        Creates a group on a specific single node.
        Returns the raw httpx.Response regardless of the status code (e.g. 201, 400, 500).
        Returns None in case of connection timeout/network failure.
        """
        url = f"{node_url}/v1/group/"
        payload = {"groupId": group_id}
        try:
            logger.info(f"Sending POST create group '{group_id}' to {node_url}")
            response = self.client.post(url, json=payload)
            return response
        except (httpx.RequestError, httpx.TimeoutException) as e:
            logger.error(f"Network failure while creating group '{group_id}' on {node_url}: {str(e)}")
            return None

    def delete_group_on_node_with_retry(self, node_url: str, group_id: str) -> Optional[httpx.Response]:
        """
        Deletes a group from a specific node with built-in retry mechanism.
        Retries up to max_retries on network errors or 5xx server errors.
        Returns the final httpx.Response or None if all retries fail.
        """
        url = f"{node_url}/v1/group/"
        payload = {"groupId": group_id}

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f"Attempt {attempt}/{self.max_retries}: Sending DELETE group '{group_id}' to {node_url}")
                # httpx DELETE with body request as specified in API doc
                request = self.client.build_request("DELETE", url, json=payload)
                response = self.client.send(request)

                if response.status_code == 200:
                    logger.info(f"Successfully deleted group '{group_id}' from {node_url}")
                    return response
                elif response.status_code < 500:
                    # Client errors (e.g. 404 or 400) shouldn't be retried
                    logger.warning(f"DELETE returned non-retryable status (maybe Already been Deleted or there is a problem in Format) {response.status_code} on {node_url}")
                    return response
                else:
                    logger.warning(f"DELETE failed with status {response.status_code} on {node_url} (Attempt {attempt})")
            except (httpx.RequestError, httpx.TimeoutException) as e:
                logger.warning(f"DELETE request exception on {node_url} (Attempt {attempt}): {str(e)}")

            if attempt < self.max_retries:
                time.sleep(self.retry_backoff)

        logger.error(f"Failed to delete group '{group_id}' from {node_url} after {self.max_retries} attempts.")
        return None

    # -------------------------------------------------------------------------
    # 2. Main Cluster APIs (Create Group, Delete Group, Get Group)
    # -------------------------------------------------------------------------

    def create_group(self, group_id: str) -> Dict[str, Any]:
        """
        Cluster API for Creating a Group across all nodes.
        1. Performs TCC check to verify all nodes are OK.
        2. Executes Saga pattern: calls create_group_on_node for each node, recording successful nodes.
        3. On error (500, timeout, non-201 response), triggers Rollback by deleting group from recorded nodes.
        4. If Rollback fails for any node, logs system as UNSTABLE.
        """
        # Step 1: TCC check
        if not self.check_all_nodes_ok(group_id):
            logger.error("Create operation aborted: TCC check failed (one or more nodes are down/unresponsive).")
            return {
                "status": "TCC failed",
                "message": "TCC health check failed. Not all cluster nodes are ready."
            }

        created_nodes: List[str] = []

        # Step 2: Saga pattern execution
        for node in self.hosts:
            response = self.create_group_on_node(node, group_id)

            if response is not None and response.status_code in (201,400):
                created_nodes.append(node)
                logger.info(f"Group '{group_id}' successfully created on node {node}")
            else:
                # Creation failed on this node (500, timeout, or bad request)
                error_info = f"Status {response.status_code}" if response else "Timeout/Connection Error"
                logger.error(f"Creation failed on node {node} ({error_info}). Initiating Saga Rollback...")

                # Step 3: Saga Rollback execution
                remaining_nodes = []
                rollback_success = True
                for rollback_node in created_nodes:
                    del_response = self.delete_group_on_node_with_retry(rollback_node, group_id)
                    if del_response is None or del_response.status_code not in (200,404):
                        rollback_success = False
                        remaining_nodes.append(rollback_node)
                        logger.critical(
                            f"CRITICAL: Rollback failed to delete group '{group_id}' on node {rollback_node}!"
                        )
                    

                if not rollback_success:
                    logger.critical(
                        f"SYSTEM STATUS: UNSTABLE! Group '{group_id}' was partially created and rollback failed."
                    )
                    return {
                        "status": "unstable",
                        "message": "System is in an UNSTABLE state! Group creation failed and rollback was incomplete.",
                        "created_nodes": created_nodes,
                        "remaining_nodes_after_rollback" : remaining_nodes,
                        "failed_node": node
                    }
                else:
                    logger.info(f"Saga Rollback completed successfully. Group '{group_id}' removed from affected nodes.")
                    return {
                        "status": "rolled_back",
                        "message": f"Creation failed on node {node}. Rollback successfully executed on all previously created nodes.",
                        "failed_node": node
                    }

        return {
            "status": "success",
            "message": f"Group '{group_id}' successfully created on all {len(self.hosts)} nodes.",
            "groupId": group_id
        }

    def delete_group(self, group_id: str) -> Dict[str, Any]:
        """
        Cluster API for Deleting a Group from all nodes.
        1. Performs TCC check to verify all nodes are OK.
        2. Calls delete_group_on_node_with_retry for each node.
        3. If delete fails on any node after retries, returns UNSTABLE system status.
        """
        # Step 1: TCC check
        if not self.check_all_nodes_ok(group_id):
            logger.error("Delete operation aborted: TCC check failed (one or more nodes are down/unresponsive).")
            return {
                "status": "TCC failed",
                "message": "TCC health check failed. Not all cluster nodes are ready."
            }

        # Step 2: Delete from each node
        failed_nodes: List[str] = []

        for node in self.hosts:
            del_response = self.delete_group_on_node_with_retry(node, group_id)
            if del_response is None or del_response.status_code not in (200,404) :
                failed_nodes.append(node)
                logger.error(f"Failed to delete group '{group_id}' from node {node}")

        if failed_nodes:
            logger.critical(
                f"SYSTEM STATUS: UNSTABLE! Failed to delete group '{group_id}' from nodes: {failed_nodes}"
            )
            return {
                "status": "unstable",
                "message": f"System is in an UNSTABLE state! Failed to delete group '{group_id}' from nodes: {failed_nodes}",
                "failed_nodes": failed_nodes
            }

        logger.info(f"Group '{group_id}' successfully deleted from all nodes.")
        return {
            "status": "success",
            "message": f"Group '{group_id}' successfully deleted from all nodes.",
            "groupId": group_id
        }

    def get_group(self, group_id: str, node_url: Optional[str] = None) -> Dict[str, Any]:
        """
        Cluster API for Fetching Group Info.
        Queries a specified node (or iterates nodes until found) GET /v1/group/{groupId}/.
        No rollback required.
        """
        target_nodes = [node_url.rstrip('/')] if node_url else self.hosts

        for node in target_nodes:
            url = f"{node}/v1/group/{group_id}/"
            try:
                logger.info(f"Fetching group info for '{group_id}' from {node}")
                response = self.client.get(url)
                if response.status_code == 200:
                    return {
                        "status": "success",
                        "node": node,
                        "data": response.json()
                    }
                elif response.status_code == 404:
                    logger.info(f"Group '{group_id}' not found on {node}")
            except (httpx.RequestError, httpx.TimeoutException) as e:
                logger.warning(f"Error fetching group '{group_id}' from {node}: {str(e)}")

        return {
            "status": "not_found",
            "message": f"Group '{group_id}' not found on cluster.",
            "groupId": group_id
        }

    def close(self):
        """Close the underlying HTTP client."""
        self.client.close()

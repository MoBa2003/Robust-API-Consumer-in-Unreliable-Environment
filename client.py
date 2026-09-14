import logging
import time
from typing import List, Dict, Any, Optional
import httpx


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("ClusterClient")

class ClusterClient:

    def __init__(self, hosts: List[str], timeout: float = 5.0, max_retries: int = 3, retry_backoff: float = 1.0):
     
        self.hosts = [h.rstrip('/') for h in hosts]
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.client = httpx.Client(timeout=self.timeout)
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


    def check_all_nodes_ok(self,group_id) -> bool:
      
        logger.info("Starting TCC check: Verifying health of all cluster nodes...")
        for node in self.hosts:
            try:
               
                response = self.client.get(f"{node}/v1/group/{group_id}")
                
                if response.status_code >= 500:
                    logger.warning(f"Health check failed for node {node}: Status {response.status_code}")
                    return False
            except (httpx.RequestError, httpx.TimeoutException) as e:
                logger.warning(f"Health check failed for node {node}: {str(e)}")
                return False
        logger.info("TCC check passed: All nodes are healthy and responding.")
        return True

    def create_group_on_node(self, node_url: str, group_id: str) -> Optional[httpx.Response]:
      
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
       
        url = f"{node_url}/v1/group/"
        payload = {"groupId": group_id}

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f"Attempt {attempt}/{self.max_retries}: Sending DELETE group '{group_id}' to {node_url}")
                request = self.client.build_request("DELETE", url, json=payload)
                response = self.client.send(request)

                if response.status_code == 200:
                    logger.info(f"Successfully deleted group '{group_id}' from {node_url}")
                    return response
                elif response.status_code < 500:
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


    def create_group(self, group_id: str) -> Dict[str, Any]:
      
        if not self.check_all_nodes_ok(group_id):
            logger.error("Create operation aborted: TCC check failed (one or more nodes are down/unresponsive).")
            return {
                "status": "TCC failed",
                "message": "TCC health check failed. Not all cluster nodes are ready."
            }

        created_nodes: List[str] = []

        for node in self.hosts:
            response = self.create_group_on_node(node, group_id)

            if response is not None and response.status_code in (201,400):
                created_nodes.append(node)
                logger.info(f"Group '{group_id}' successfully created on node {node}")
            else:
                error_info = f"Status {response.status_code}" if response else "Timeout/Connection Error"
                logger.error(f"Creation failed on node {node} ({error_info}). Initiating Saga Rollback...")

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
        if not self.check_all_nodes_ok(group_id):
            logger.error("Delete operation aborted: TCC check failed (one or more nodes are down/unresponsive).")
            return {
                "status": "TCC failed",
                "message": "TCC health check failed. Not all cluster nodes are ready."
            }

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
        self.client.close()

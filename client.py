import asyncio
import logging
from typing import List, Dict, Optional
import httpx

logger = logging.getLogger(__name__)

class ClusterStateError(Exception):
    """Raised when the cluster enters an unstable state or operations fail."""
    pass

class ClusterClient:
    def __init__(self, hosts: List[str], timeout: float = 5.0, max_retries: int = 3):
        self.hosts = [host.rstrip('/') for host in hosts]
        self.timeout = timeout
        self.max_retries = max_retries

    async def _check_nodes_health(self, client: httpx.AsyncClient) -> bool:
        """
        TCC Try Phase: Checks if all nodes in the cluster are responsive.
        Since there is no explicit /health endpoint, it sends a GET request for a dummy ID.
        Receiving a response (even 404 Not Found) means the node is up.
        """
        for host in self.hosts:
            try:
                response = await client.get(f"{host}/v1/group/health_check_dummy/")
                if response.status_code not in (200, 404):
                    return False
            except httpx.RequestError:
                return False
        return True

    async def _create_on_node(self, client: httpx.AsyncClient, host: str, group_id: str) -> Optional[httpx.Response]:
        """Creates a group on a single node and returns the raw response regardless of status."""
        try:
            return await client.post(f"{host}/v1/group/", json={"groupId": group_id})
        except httpx.RequestError:
            return None

    async def _delete_on_node_with_retry(self, client: httpx.AsyncClient, host: str, group_id: str) -> Optional[httpx.Response]:
        """Deletes a group from a single node with built-in exponential backoff retries."""
        url = f"{host}/v1/group/"
        payload = {"groupId": group_id}
        last_response = None
        
        for attempt in range(1, self.max_retries + 1):
            try:
                response = await client.request("DELETE", url, json=payload)
                last_response = response
                
                if response.status_code in (200, 404):
                    return response
            except httpx.RequestError:
                last_response = None
            
            if attempt < self.max_retries:
                await asyncio.sleep(2 ** (attempt - 1))
                
        return last_response

    async def create_group(self, group_id: str) -> None:
        """
        Creates a group using TCC for validation and Saga for rollback.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            # 1. TCC Try Phase
            if not await self._check_nodes_health(client):
                raise ClusterStateError("TCC Try Phase Failed: Not all nodes are responsive.")

            created_nodes = []
            
            # 2. Confirm Phase & Saga Execution
            for host in self.hosts:
                response = await self._create_on_node(client, host, group_id)
                
                if response and response.status_code in (201, 400):
                    if response.status_code == 400:
                        logger.info(f"Group {group_id} already exists on {host} (400). Treated as success.")
                    else:
                        logger.info(f"Group {group_id} successfully created on {host} (201).")
                        
                    created_nodes.append(host)
                else:
                    logger.error(f"Error creating group on {host}. Initiating Saga Rollback.")
                    
                    unstable = False
                    # 3. Rollback
                    for rollback_host in created_nodes:
                        del_response = await self._delete_on_node_with_retry(client, rollback_host, group_id)
                        
                        if not del_response or del_response.status_code not in (200, 404):
                            logger.critical(f"System is in an unstable state! Rollback failed on {rollback_host}.")
                            unstable = True
                            
                    if not unstable and created_nodes:
                        logger.info("Rollback executed successfully. System is consistent.")
                        
                    raise ClusterStateError(f"Group creation aborted and rolled back due to failure on {host}.")

    async def delete_group(self, group_id: str) -> None:
        """
        Deletes a group using TCC for validation and Retry-to-Target for resiliency.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            # 1. TCC Try Phase
            if not await self._check_nodes_health(client):
                raise ClusterStateError("TCC Try Phase Failed: Not all nodes are responsive.")

            # 2. Confirm Phase with Retries
            for host in self.hosts:
                response = await self._delete_on_node_with_retry(client, host, group_id)
                
                if not response or response.status_code not in (200, 404):
                    logger.critical(f"System is in an unstable state! Failed to delete on {host} after all retries.")
                    raise ClusterStateError("System is in an unstable state during deletion.")
            
            logger.info(f"Group {group_id} successfully deleted from all nodes.")

    async def get_group(self, group_id: str) -> Optional[Dict]:
        """
        Retrieves the group status from the first available node.
        No rollback mechanism required.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            for host in self.hosts:
                try:
                    response = await client.get(f"{host}/v1/group/{group_id}/")
                    if response.status_code == 200:
                        return response.json()
                    elif response.status_code == 404:
                        return None
                except httpx.RequestError:
                    continue
            return None
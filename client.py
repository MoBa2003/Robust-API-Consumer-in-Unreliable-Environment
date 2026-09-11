import asyncio
import logging
from typing import List, Dict, Optional
import httpx

# Configure module logger for tracing cluster operations and errors
logger = logging.getLogger(__name__)


# =====================================================================
# Custom Exceptions
# =====================================================================
class ClusterClientError(Exception):
    """Base exception class for all cluster client errors."""
    pass


class GroupCreationError(ClusterClientError):
    """Raised when group creation fails on any node and triggers a rollback."""
    pass


class GroupDeletionError(ClusterClientError):
    """Raised when group deletion fails on one or more cluster nodes."""
    pass


class RollbackFailedError(ClusterClientError):
    """Critical error raised when rollback operation fails on one or more nodes."""
    pass


class ClusterClient:
  

    def __init__(self, hosts: List[str], timeout: float = 5.0, max_retries: int = 3):
        self.hosts = [host.rstrip('/') for host in hosts]
        self.timeout = timeout
        self.max_retries = max_retries

    async def create_group(self, group_id: str) -> None:
        created_nodes: List[str] = []
        payload = {"groupId": group_id}

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            for host in self.hosts:
                url = f"{host}/v1/group/"
                try:
                    response = await client.post(url, json=payload)
                    
                    # 201 CREATED indicates successful creation on this node
                    if response.status_code == 201:
                        logger.info(f"Successfully created group '{group_id}' on node: {host}")
                        created_nodes.append(host)
                    else:
                        error_msg = f"Node '{host}' returned status code {response.status_code}: {response.text}"
                        logger.error(f"Failed to create group '{group_id}' on {host}. {error_msg}")
                        
                        # Trigger rollback on all nodes created so far
                        await self._rollback_creation(client, group_id, created_nodes)
                        raise GroupCreationError(f"Creation failed on '{host}'. Rollback executed. Details: {error_msg}")

                except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                    logger.error(f"Network or server error on {host} while creating group '{group_id}': {exc}")
                    
                    # Trigger rollback on all nodes created so far
                    await self._rollback_creation(client, group_id, created_nodes)
                    raise GroupCreationError(f"Creation failed on '{host}' due to error: {exc}. Rollback executed.")
    async def _rollback_creation(self, client: httpx.AsyncClient, group_id: str, nodes_to_rollback: List[str]) -> None:
        
        if not nodes_to_rollback:
            logger.info("No nodes require rollback.")
            return

        logger.warning(f"Initiating rollback for group '{group_id}' on nodes: {nodes_to_rollback}")
        failed_rollback_nodes: List[str] = []

        for host in nodes_to_rollback:
            success = await self._delete_with_retry(client, host, group_id)
            if not success:
                failed_rollback_nodes.append(host)

        if failed_rollback_nodes:
            critical_msg = f"Critical Error: Failed to roll back group '{group_id}' on nodes: {failed_rollback_nodes}"
            logger.critical(critical_msg)
            raise RollbackFailedError(critical_msg)

    async def _delete_with_retry(self, client: httpx.AsyncClient, host: str, group_id: str) -> bool:
        
        url = f"{host}/v1/group/"
        payload = {"groupId": group_id}

        for attempt in range(1, self.max_retries + 1):
            try:
                response = await client.request("DELETE", url, json=payload)
                
                # 200 OK means deleted; 404 Not Found is treated as successful rollback idempotency
                if response.status_code in (200, 404):
                    logger.info(f"Rollback succeeded on {host} for group '{group_id}' (Attempt {attempt})")
                    return True
                
                logger.warning(f"Rollback attempt {attempt} on {host} returned status: {response.status_code}")
            except httpx.RequestError as exc:
                logger.warning(f"Rollback attempt {attempt} on {host} failed with network error: {exc}")

            # Exponential backoff (1s, 2s, 4s...) before next retry
            if attempt < self.max_retries:
                backoff_time = 2 ** (attempt - 1)
                await asyncio.sleep(backoff_time)

        return False

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

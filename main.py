import os
import logging
import time
from client import ClusterClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MainRunner")

def get_env_hosts():
    raw_hosts = os.getenv("CLUSTER_HOSTS", "http://node1.example.com,http://node2.example.com,http://node3.example.com")
    return [h.strip() for h in raw_hosts.split(",") if h.strip()]

def main():
    hosts = get_env_hosts()
    timeout = float(os.getenv("CLIENT_TIMEOUT", "5.0"))
    max_retries = int(os.getenv("MAX_RETRIES", "3"))

    logger.info(f"Starting ClusterClient with hosts: {hosts}, timeout: {timeout}, max_retries: {max_retries}")

    with ClusterClient(hosts=hosts, timeout=timeout, max_retries=max_retries) as client:
        
        logger.info("Performing initial TCC health check...")
        is_ok = client.check_all_nodes_ok(group_id="system-check")
        logger.info(f"TCC Health Check Status: {is_ok}")

       
        while True:
            time.sleep(60)

if __name__ == "__main__":
    main()

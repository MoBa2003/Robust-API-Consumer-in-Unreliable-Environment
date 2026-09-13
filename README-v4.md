# ClusterClient: Distributed Systems Resilient API Client

`ClusterClient` is a resilient, fault-tolerant Python client designed to manage group objects (`Group`) across a distributed cluster of unstable REST API nodes. The project ensures distributed consistency using architectural patterns such as **Try-Confirm-Cancel (TCC)**, **Saga Pattern (with compensating rollbacks)**, and **Idempotent Retry-to-Target-State**.

In addition to core Python logic, this repository provides a complete DevOps lifecycle including containerization with **Docker (Multi-stage Build)**, configuration management and orchestration with **Kubernetes (Kustomize & Job/Deployment)**, network isolation with **NetworkPolicy**, and automated local deployment using **Kind** and **Skaffold**.

---

## 📑 Table of Contents
1. [Architectural Strategies](#1-architectural-strategies)
2. [Project Architecture & Repository Structure](#2-project-architecture--repository-structure)
3. [Unit Testing Strategy](#3-unit-testing-strategy)
4. [Docker Containerization](#4-docker-containerization)
5. [Kubernetes Manifests & Configuration](#5-kubernetes-manifests--configuration)
6. [Local Kind Cluster & Skaffold Automation](#6-local-kind-cluster--skaffold-automation)
7. [Step-by-Step Execution Guide](#7-step-by-step-execution-guide)
8. [Teardown & Cleanup Guide (Stopping Docker and Kubernetes)](#8-teardown--cleanup-guide-stopping-docker-and-kubernetes)

---

## 1. Architectural Strategies

In an inherently unstable distributed network, HTTP 5xx errors, connection drops, and network timeouts are expected failure modes. To maintain state consistency across nodes, `client.py` implements the following resilience patterns:

### 1.1. Try-Confirm-Cancel (TCC) Pattern / Cluster Health Check
* **Concept**: Prior to performing any state-modifying action (create or delete) across the cluster, the client executes a lightweight health verification phase across all configured nodes.
* **Implementation in `check_all_nodes_ok`**: The client dispatches GET requests to all nodes. If any node responds with an HTTP status code $\ge 500$ or experiences a timeout, the operation is immediately aborted and returns `TCC failed`.

### 1.2. Saga Transaction Pattern (Compensating Rollbacks)
* **Concept**: When creating a group across $N$ cluster nodes fails at node $k$, the client executes compensating transactions (DELETE requests) for previously modified nodes $1 \dots k-1$ to prevent a fragmented state.
* **Implementation in `create_group`**:
  1. Executes the initial TCC cluster health check.
  2. Dispatches sequential POST requests to create the group on each node.
  3. If node $k$ fails (5xx error or timeout), the **Saga Rollback** process is triggered immediately, issuing compensating DELETE requests (with retry logic) to nodes $1 \dots k-1$.
  4. If rollback succeeds, status `rolled_back` is returned. If cleanup fails on any prior node, the system flags the cluster state as critically `unstable`.

### 1.3. Idempotent Retry-to-Target-State Pattern
* **Concept**: DELETE operations are inherently idempotent (safely repeatable without side effects). The client persistently retries deletion until the desired target state (absence of resource) is confirmed.
* **Implementation in `delete_group_on_node_with_retry`**:
  * Deletion is retried up to `max_retries` with a configurable `retry_backoff` delay.
  * Receiving an HTTP 4xx response (such as 404 Not Found) is treated as achieving the target state, stopping the retry loop immediately without error.

---

## 2. Project Architecture & Repository Structure

The directory structure of the repository is organized as follows:

```text
API_CONSUMER/
├── client.py                 # Core ClusterClient class with TCC, Saga, and Retry logic
├── test_client.py            # Comprehensive 18-unit-test suite with full mocking
├── main.py                   # Kubernetes entrypoint reading environment variables from ConfigMap
├── requirements.txt          # Python dependencies (httpx, pytest)
├── Dockerfile                # Secure Multi-stage Docker build file
├── .dockerignore             # Docker build exclusions to minimize image footprint
├── kind-config.yaml          # 3-node Kind cluster configuration file
├── skaffold.yaml             # Local CI/CD pipeline configuration for Skaffold
└── manifests/                # Kubernetes manifests managed via Kustomize
    └── base/
        ├── configmap.yaml    # Environment configuration (CLUSTER_HOSTS, TIMEOUT, MAX_RETRIES)
        ├── deployment.yaml   # Long-running client Deployment with security contexts
        ├── job-test.yaml     # Standardized Kubernetes Job for automated unit tests
        ├── networkpolicy.yaml # Restricted egress network isolation policy
        └── kustomization.yaml # Kustomize entrypoint bundling all manifests
```

---

## 3. Unit Testing Strategy

The unit test suite in `test_client.py` is written using Python's standard `unittest` and `unittest.mock` modules. It comprises 18 test cases simulating distributed failure modes without external network dependencies:

1. **TCC Health Verification (`check_all_nodes_ok`)**:
   * All nodes healthy $\rightarrow$ Returns `True`
   * Single node returning HTTP 500 $\rightarrow$ Returns `False`
   * Network timeout on single node $\rightarrow$ Returns `False`
2. **Single-Node Operations & Retry Mechanism (`delete_group_on_node_with_retry`)**:
   * Successful deletion on attempt 1 $\rightarrow$ Status 200
   * Immediate termination upon receiving HTTP 404 Not Found
   * Transient HTTP 500 errors on attempts 1 & 2, succeeding on attempt 3 $\rightarrow$ Status 200
   * Exhaustion of `max_retries` due to persistent timeouts $\rightarrow$ Returns `None`
3. **Saga Transaction & Rollback Execution (`create_group`)**:
   * TCC check failure $\rightarrow$ Immediate abort (`TCC failed`)
   * Successful creation across all nodes $\rightarrow$ Returns `success`
   * Failure on node 2 with successful rollback on node 1 $\rightarrow$ Returns `rolled_back`
   * Failure on node 2 with failed rollback on node 1 $\rightarrow$ Returns `unstable`
4. **Cluster-Wide Deletion & Query Operations (`delete_group` & `get_group`)**:
   * Successful deletion across all nodes $\rightarrow$ Returns `success`
   * Unresolved node deletion failure after max retries $\rightarrow$ Returns `unstable`
   * Successful data retrieval via `GET` or graceful 404 handling

---

## 4. Docker Containerization

The application is containerized using a **Multi-Stage Build** pattern to maximize security and minimize image footprint:

* **Stage 1 (Builder)**: Installs dependencies from `requirements.txt` into an isolated virtual environment (`/opt/venv`).
* **Stage 2 (Runner)**: Copies only the runtime virtual environment onto a lightweight base image (`python:3.12-slim`).
* **Security Hardening**:
  * Creates a non-root system user and group (`appuser:appgroup` with UID/GID 10001).
  * Enforces non-root execution (`USER 10001:10001`).
  * Uses `.dockerignore` to exclude source control, documentation, and cached build artifacts.

---

## 5. Kubernetes Manifests & Configuration

Application manifests are located in `manifests/base/` and orchestrated via Kustomize:

1. **`configmap.yaml`**: Stores cluster configuration parameters including `CLUSTER_HOSTS`, `CLIENT_TIMEOUT`, and `MAX_RETRIES`.
2. **`deployment.yaml`**: Manages the long-running client application (`replicas: 2`) with:
   * **Security Context**: Privilege escalation disabled (`allowPrivilegeEscalation: false`) and read-only root filesystem enforced (`readOnlyRootFilesystem: true`).
   * **Volume Mount**: Mounts an `emptyDir` volume at `/tmp` to allow temporary file writes without compromising root filesystem immutability.
   * **Resource Governance**: Sets CPU (100m–200m) and Memory (128Mi–256Mi) requests and limits.
3. **`job-test.yaml`**: Implements a dedicated **Kubernetes Job** for automated unit test execution. The Pod executes all 18 unit tests, reports `OK`, transitions to `Completed` status, and releases cluster resources.
4. **`networkpolicy.yaml`**: Establishes egress traffic filtering, restricting outbound pod connections strictly to DNS (UDP 53) and HTTP/HTTPS (TCP 80/443).
5. **`kustomization.yaml`**: Bundles and manages all Kubernetes manifests as a single deployment unit.

---

## 6. Local Kind Cluster & Skaffold Automation

* **Kind Cluster (`kind-config.yaml`)**: Simulates a production multi-node Kubernetes cluster (1 Control-Plane node and 2 Worker nodes) locally.
* **Skaffold (`skaffold.yaml`)**: Automates local continuous development by tracking code changes, rebuilding Docker images, loading images into the Kind cluster, and applying Kustomize manifests dynamically.

---

## 7. Step-by-Step Execution Guide

### Option 1: Direct Execution in Python Environment

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run unit tests (18 tests)
python -m unittest test_client.py
```

### Usage Example

```python
from client import ClusterClient

hosts = [
    "http://node1.example.com",
    "http://node2.example.com",
    "http://node3.example.com"
]

# Using Context Manager protocol
with ClusterClient(hosts=hosts, timeout=5.0, max_retries=3) as client:
    # 1. Create group across all nodes (TCC + Saga Rollback)
    res_create = client.create_group("engineering-team")
    print("Create Status:", res_create["status"])

    # 2. Retrieve group details
    res_get = client.get_group("engineering-team")
    print("Group Details:", res_get)

    # 3. Delete group across cluster with Retry logic
    res_delete = client.delete_group("engineering-team")
    print("Delete Status:", res_delete["status"])
```

### Option 2: Deployment on Local Kubernetes Cluster (Kind + Skaffold)

1. **Create 3-Node Kind Cluster**:
   ```powershell
   kind create cluster --config kind-config.yaml
   ```

2. **Verify Node Status**:
   ```powershell
   kubectl get nodes
   ```

3. **Deploy with Skaffold**:
   ```powershell
   skaffold dev --status-check=false
   ```

4. **Verify Pod Status and Logs**:
   ```powershell
   # Check Pods (Unit tests pod Completed, Client deployment pods Running)
   kubectl get pods

   # View unit tests execution logs
   kubectl logs job/cluster-client-unit-tests

   # View client runner logs
   kubectl logs -l app.kubernetes.io/name=cluster-client --tail=50
   ```

---

## 8. Teardown & Cleanup Guide (Stopping Docker and Kubernetes)

To stop the running application, tear down the local cluster, and free up system resources (CPU and Memory), follow these steps:

### 8.1. Stop Skaffold Development Loop
If `skaffold dev` is actively running in your terminal:
* Press **`Ctrl + C`** in the terminal window.
* Skaffold will automatically clean up the deployed Kubernetes manifests, services, and pods before exiting.

### 8.2. Delete the Kind Kubernetes Cluster
To remove the 3-node Kind cluster (`cluster-client-cluster`) and free up allocated system RAM:
```powershell
kind delete cluster --name cluster-client-cluster
```

### 8.3. Docker Resource Cleanup (Optional)
To purge temporary build caches and dangling container resources from Docker:
```powershell
docker system prune -f
```

### 8.4. Stop Docker Desktop
If you no longer require the Docker daemon:
1. Right-click the **Docker Desktop whale icon** in the Windows System Tray (near the clock).
2. Select **Quit Docker Desktop**.

# Distributed Systems Reliability API Challenge: ClusterClient

This repository contains a resilient Python client (`ClusterClient`) designed to manage group objects across a multi-node cluster with unreliable APIs. The client guarantees distributed consistency by utilizing architectural patterns such as **Try-Confirm-Cancel (TCC)** for cluster health checks, the **Saga Pattern** for compensating transaction rollbacks, and **Idempotent Retry-to-Target-State** for deletion resilience.

---

## 1. Distributed Systems Architectural Strategies

Managing state across multiple nodes in an unreliable network presents challenges like partial failures, network timeouts, and server errors (`5xx`). To solve these challenges, `ClusterClient` implements the following distributed transaction patterns:

### 1.1. Try-Confirm-Cancel (TCC) / Cluster Health Check
* **Concept**: Before executing any state-changing operation across the cluster, the client performs a lightweight "Try" phase to check node reachability and responsiveness.
* **Implementation**: The `check_all_nodes_ok()` method sends a `GET` query to every node in the cluster. If any node returns a server error (`>= 500`) or experiences a network timeout/exception, the operation is aborted immediately without modifying cluster state.

### 1.2. Saga Transaction Pattern (Compensating Transactions)
* **Concept**: When a multi-step distributed transaction (creating a group on $N$ nodes) fails halfway, the system executes compensating actions to reverse the completed steps.
* **Implementation in `create_group()`**:
  1. Executes a TCC health check.
  2. Sequentially sends `POST /v1/group/` to each node.
  3. If node $k$ fails (5xx, timeout, or bad response), creation halts immediately.
  4. The client initiates a **Saga Rollback**, issuing compensating `DELETE` requests with retries to all nodes $1 \dots k-1$ where the group was successfully created.
  5. Returns `status: "rolled_back"` if all compensations succeed, or `status: "unstable"` if any rollback deletion fails.

### 1.3. Idempotent Retry-to-Target-State
* **Concept**: HTTP `DELETE` operations are inherently idempotent. Rather than rolling back a deletion failure, the client continuously retries the operation until the target state (absence of the resource) is reached.
* **Implementation in `delete_group()` & Rollback**:
  * `delete_group_on_node_with_retry()` retries up to `max_retries` with a configurable backoff (`retry_backoff`).
  * Non-retryable client statuses (e.g., `404 Not Found` or `400 Bad Request`) immediately complete the retry loop, as `404` indicates the group is already gone.

---

## 2. Client Architecture (`client.py`)

The `ClusterClient` class is built on top of `httpx.Client` for fast, reliable HTTP execution.

### Class Configuration & Parameters
```python
client = ClusterClient(
    hosts=["http://node1.example.com", "http://node2.example.com"],
    timeout=5.0,        # Request timeout in seconds
    max_retries=3,      # Max retries for critical idempotent actions
    retry_backoff=1.0   # Backoff delay (seconds) between retries
)
```

### Core API Methods

| Method | Description | Return Statuses |
| :--- | :--- | :--- |
| `check_all_nodes_ok(group_id)` | TCC Health Check across all cluster nodes via GET. | `True` / `False` |
| `create_group(group_id)` | Cluster-wide creation using TCC + Saga Rollback. | `"success"`, `"rolled_back"`, `"unstable"`, `"failed"` |
| `delete_group(group_id)` | Cluster-wide deletion using TCC + Retry-to-target-state. | `"success"`, `"unstable"`, `"failed"` |
| `get_group(group_id, node_url=None)` | Fetches group info from a target node (or fallback iterator). | `"success"`, `"not_found"` |
| `close()` | Safely closes the underlying HTTP client session. | N/A |

---

## 3. Unit Testing Strategy (`test_client.py`)

A comprehensive test suite of **17 unit tests** was developed using `pytest` and `unittest` with mock objects (`unittest.mock.patch`). The tests simulate all failure modes without requiring external network connections.

### Test Coverage Highlights

1. **TCC Checks (`check_all_nodes_ok`)**:
   * All nodes healthy $\rightarrow$ returns `True`.
   * Single node returns 500 $\rightarrow$ returns `False`.
   * Network timeout on single node $\rightarrow$ returns `False`.

2. **Single-Node Operations & Retry Mechanism (`delete_group_on_node_with_retry`)**:
   * Immediate deletion success on attempt 1.
   * Non-retryable `404 Not Found` $\rightarrow$ stops retry immediately.
   * Transient `500 Server Error` on attempts 1 & 2 $\rightarrow$ succeeds on attempt 3.
   * Network timeouts on all attempts $\rightarrow$ exhausts `max_retries` and returns `None`.

3. **Saga Transaction & Rollback (`create_group`)**:
   * TCC failure $\rightarrow$ aborts creation immediately (`failed`).
   * Successful creation across all nodes $\rightarrow$ (`success`).
   * Partial creation failure $\rightarrow$ successfully executes compensating DELETEs on completed nodes (`rolled_back`).
   * Partial creation failure + Rollback failure $\rightarrow$ flags system state as critical (`unstable`).

4. **Cluster Deletion & Information Retrieval**:
   * Deletion succeeds on all nodes $\rightarrow$ (`success`).
   * Deletion fails after retries on one node $\rightarrow$ returns (`unstable`).
   * Successful `GET` request retrieval and `404 Not Found` handling.

---

## 4. How to Run the Project & Tests

### Prerequisites
* Python 3.9+
* Required packages: `httpx`, `pytest`

```bash
pip install httpx pytest
```

### Running Unit Tests

Execute the complete test suite using `pytest`:
```bash
pytest test_client.py -v
```

Alternatively, run using the standard Python `unittest` runner:
```bash
python -m unittest test_client.py
```

### Usage Example

```python
from client import ClusterClient

hosts = ["http://localhost:8081", "http://localhost:8082"]

with ClusterClient(hosts=hosts, timeout=3.0, max_retries=3) as client:
    # 1. Create a group across all nodes
    res_create = client.create_group("dev-team")
    print("Create Status:", res_create["status"])

    # 2. Query group details
    res_get = client.get_group("dev-team")
    print("Group Info:", res_get)

    # 3. Delete group from cluster
    res_delete = client.delete_group("dev-team")
    print("Delete Status:", res_delete["status"])
```

---

## 5. Future Containerization & Deployment Setup

* **Docker**: A lightweight `Dockerfile` based on `python:3.12-slim` can containerize the client application.
* **Kubernetes Manifests**: Deployment & CronJob manifests in the `manifests/` directory allow orchestrated execution within Kubernetes or Minikube clusters.

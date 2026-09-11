Rolling back strategies in Distributed Systems (According to my Research in Websites and get help from LLMs) : 

Saga Pattern (Compensating Transactions): Breaks a distributed transaction into independent local steps. If a failure occurs mid-process, it issues compensating actions (e.g., executing a DELETE after a failed POST) to undo previous successful steps and restore consistency.

Idempotent Retry-to-Target-State: Leverages the idempotency of HTTP methods and eventual consistency. Instead of rolling back, it uses exponential backoff to continuously retry the failed operation until every node reaches the desired target state.

Try-Confirm-Cancel (TCC): Ensures cluster-wide readiness before applying changes by splitting the request. It first verifies node responsiveness (Try), then either executes the operation globally (Confirm) or aborts safely without altering state (Cancel).

Implementation Strategy(the strategy used for rolling back in this project for each method)

Create Group (POST)
For creating a new group, the client first utilizes the TCC (Try-Confirm-Cancel) pattern to verify that all nodes are responsive and ready. If the initial check passes but an unexpected error occurs during the actual creation phase, the client falls back to the Saga pattern. It will issue compensating DELETE requests to remove the newly created group from any nodes that successfully processed the initial POST, ensuring the cluster rolls back to a consistent state.

Delete Group (DELETE)
For deleting a group, the client also begins with the TCC pattern to validate cluster readiness. Since the DELETE operation is inherently idempotent, if a network or server failure occurs mid-execution, the client employs the Retry-to-Target-State strategy. It will continuously retry the DELETE request with exponential backoff on the failing nodes until the group is successfully removed across the entire cluster.
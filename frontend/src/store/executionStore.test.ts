import { describe, it, expect, beforeEach } from "vitest";
import { useExecutionStore, isTerminalStatus } from "./executionStore";

beforeEach(() => {
  useExecutionStore.getState().reset();
});

describe("isTerminalStatus", () => {
  it.each(["done", "error", "skipped", "cached"] as const)(
    "returns true for terminal status '%s'",
    (status) => {
      expect(isTerminalStatus(status)).toBe(true);
    },
  );

  it.each(["running", "pending", "idle", "dirty"] as const)(
    "returns false for non-terminal status '%s'",
    (status) => {
      expect(isTerminalStatus(status)).toBe(false);
    },
  );
});

describe("markDirty", () => {
  it("transitions a 'done' node to 'dirty'", () => {
    const store = useExecutionStore.getState();
    store.setNodeStatus("n1", "done");
    store.markDirty("n1");
    expect(useExecutionStore.getState().nodeStatuses["n1"]).toBe("dirty");
  });

  it("transitions a 'cached' node to 'dirty'", () => {
    const store = useExecutionStore.getState();
    store.setNodeStatus("n1", "cached");
    store.markDirty("n1");
    expect(useExecutionStore.getState().nodeStatuses["n1"]).toBe("dirty");
  });

  it("does not change a 'running' node", () => {
    const store = useExecutionStore.getState();
    store.setNodeStatus("n1", "running");
    store.markDirty("n1");
    expect(useExecutionStore.getState().nodeStatuses["n1"]).toBe("running");
  });

  it("does not change a 'pending' node", () => {
    const store = useExecutionStore.getState();
    store.setNodeStatus("n1", "pending");
    store.markDirty("n1");
    expect(useExecutionStore.getState().nodeStatuses["n1"]).toBe("pending");
  });

  it("does not change an 'error' node", () => {
    const store = useExecutionStore.getState();
    store.setNodeStatus("n1", "error");
    store.markDirty("n1");
    expect(useExecutionStore.getState().nodeStatuses["n1"]).toBe("error");
  });

  it("does not add a status for a node not in the map (idle)", () => {
    useExecutionStore.getState().markDirty("unknown");
    expect(useExecutionStore.getState().nodeStatuses["unknown"]).toBeUndefined();
  });

  it("does not re-set a 'dirty' node (dirty is not in the guard)", () => {
    const store = useExecutionStore.getState();
    store.setNodeStatus("n1", "done");
    store.markDirty("n1");
    expect(useExecutionStore.getState().nodeStatuses["n1"]).toBe("dirty");
    // markDirty again — 'dirty' is NOT in the guard (done|cached), so it's a no-op
    useExecutionStore.getState().markDirty("n1");
    expect(useExecutionStore.getState().nodeStatuses["n1"]).toBe("dirty");
  });
});

describe("setNodeStatus", () => {
  it("sets the status for a node", () => {
    useExecutionStore.getState().setNodeStatus("n1", "running");
    expect(useExecutionStore.getState().nodeStatuses["n1"]).toBe("running");
  });

  it("keeps a 'running' node in pendingNodeIds", () => {
    useExecutionStore.getState().setAllPending(["n1", "n2"]);
    useExecutionStore.getState().setNodeStatus("n1", "running");
    expect(useExecutionStore.getState().pendingNodeIds).toContain("n1");
  });

  it("removes node from pendingNodeIds on 'done'", () => {
    useExecutionStore.getState().setAllPending(["n1", "n2"]);
    useExecutionStore.getState().setNodeStatus("n1", "done");
    expect(useExecutionStore.getState().pendingNodeIds).not.toContain("n1");
    expect(useExecutionStore.getState().pendingNodeIds).toContain("n2");
  });

  it("removes node from pendingNodeIds on 'error'", () => {
    useExecutionStore.getState().setAllPending(["n1"]);
    useExecutionStore.getState().setNodeStatus("n1", "error");
    expect(useExecutionStore.getState().pendingNodeIds).not.toContain("n1");
  });

  it("removes node from pendingNodeIds on 'skipped'", () => {
    useExecutionStore.getState().setAllPending(["n1"]);
    useExecutionStore.getState().setNodeStatus("n1", "skipped");
    expect(useExecutionStore.getState().pendingNodeIds).not.toContain("n1");
  });

  it("removes node from pendingNodeIds on 'cached'", () => {
    useExecutionStore.getState().setAllPending(["n1"]);
    useExecutionStore.getState().setNodeStatus("n1", "cached");
    expect(useExecutionStore.getState().pendingNodeIds).not.toContain("n1");
  });

  it("does not crash when node is not in pendingNodeIds", () => {
    useExecutionStore.getState().setNodeStatus("n99", "done");
    expect(useExecutionStore.getState().nodeStatuses["n99"]).toBe("done");
    expect(useExecutionStore.getState().pendingNodeIds).toEqual([]);
  });
});

describe("setAllPending", () => {
  it("sets all provided node IDs to 'pending'", () => {
    useExecutionStore.getState().setAllPending(["a", "b", "c"]);
    const { nodeStatuses } = useExecutionStore.getState();
    expect(nodeStatuses["a"]).toBe("pending");
    expect(nodeStatuses["b"]).toBe("pending");
    expect(nodeStatuses["c"]).toBe("pending");
  });

  it("replaces any existing nodeStatuses (clean slate)", () => {
    useExecutionStore.getState().setNodeStatus("old", "done");
    useExecutionStore.getState().setAllPending(["new1"]);
    const { nodeStatuses } = useExecutionStore.getState();
    expect(nodeStatuses["old"]).toBeUndefined();
    expect(nodeStatuses["new1"]).toBe("pending");
  });

  it("sets pendingNodeIds to the full list", () => {
    useExecutionStore.getState().setAllPending(["x", "y"]);
    expect(useExecutionStore.getState().pendingNodeIds).toEqual(["x", "y"]);
  });

  it("sets initialPendingCount to the length", () => {
    useExecutionStore.getState().setAllPending(["a", "b", "c"]);
    expect(useExecutionStore.getState().initialPendingCount).toBe(3);
  });

  it("handles an empty array", () => {
    useExecutionStore.getState().setAllPending([]);
    expect(useExecutionStore.getState().nodeStatuses).toEqual({});
    expect(useExecutionStore.getState().pendingNodeIds).toEqual([]);
    expect(useExecutionStore.getState().initialPendingCount).toBe(0);
  });
});

describe("reset", () => {
  it("clears all state back to initial values", () => {
    // Populate the store with non-default values
    const store = useExecutionStore.getState();
    store.setNodeStatus("n1", "done");
    store.setNodeTraceback("n1", "some error");
    store.setRunning(true);
    store.setCurrentNodeId("n1");
    store.setPendingNodeIds(["n1", "n2"]);
    store.setRunId("run-123");
    store.setWsStatus("connected");
    store.setDraggingPortType("TABLE");
    store.setRunResult("success");
    store.setLastDoneNodeId("n1");

    useExecutionStore.getState().reset();

    const state = useExecutionStore.getState();
    expect(state.nodeStatuses).toEqual({});
    expect(state.nodeTracebacks).toEqual({});
    expect(state.isRunning).toBe(false);
    expect(state.currentNodeId).toBeNull();
    expect(state.pendingNodeIds).toEqual([]);
    expect(state.initialPendingCount).toBe(0);
    expect(state.runId).toBeNull();
    expect(state.wsStatus).toBe("disconnected");
    expect(state.draggingPortType).toBeNull();
    expect(state.runResult).toBeNull();
    expect(state.lastDoneNodeId).toBeNull();
  });
});

import { create } from "zustand";
import type { NodeStatus } from "../lib/types";

interface ExecutionState {
  nodeStatuses: Record<string, NodeStatus>;
  isRunning: boolean;
  currentNodeId: string | null;
  lastRunId: string | null;
  pendingNodeIds: Set<string>;

  setNodeStatus: (nodeId: string, status: NodeStatus) => void;
  setAllPending: (nodeIds: string[]) => void;
  setRunning: (running: boolean) => void;
  setCurrentNodeId: (nodeId: string | null) => void;
  setLastRunId: (runId: string | null) => void;
  reset: () => void;
}

const TERMINAL: ReadonlySet<NodeStatus> = new Set(["done", "error", "skipped"]);

export const useExecutionStore = create<ExecutionState>((set) => ({
  nodeStatuses: {},
  isRunning: false,
  currentNodeId: null,
  lastRunId: null,
  pendingNodeIds: new Set(),

  setNodeStatus: (nodeId, status) =>
    set((state) => {
      const nodeStatuses = { ...state.nodeStatuses, [nodeId]: status };
      const pendingNodeIds = new Set(state.pendingNodeIds);

      if (TERMINAL.has(status)) {
        pendingNodeIds.delete(nodeId);
      }

      return { nodeStatuses, pendingNodeIds };
    }),

  setAllPending: (nodeIds) =>
    set(() => {
      const nodeStatuses: Record<string, NodeStatus> = {};
      for (const id of nodeIds) {
        nodeStatuses[id] = "pending";
      }
      return { nodeStatuses, pendingNodeIds: new Set(nodeIds) };
    }),

  setRunning: (running) => set({ isRunning: running }),

  setCurrentNodeId: (nodeId) => set({ currentNodeId: nodeId }),

  setLastRunId: (runId) => set({ lastRunId: runId }),

  reset: () =>
    set({
      nodeStatuses: {},
      isRunning: false,
      currentNodeId: null,
      lastRunId: null,
      pendingNodeIds: new Set(),
    }),
}));

import { create } from "zustand";
import type { NodeStatus } from "../lib/types";

const TERMINAL_STATUSES: ReadonlySet<NodeStatus> = new Set([
  "done",
  "error",
  "skipped",
  "cached",
]);

export function isTerminalStatus(status: NodeStatus): boolean {
  return TERMINAL_STATUSES.has(status);
}

interface ExecutionState {
  nodeStatuses: Record<string, NodeStatus>;
  nodeTracebacks: Record<string, string>;
  isRunning: boolean;
  currentNodeId: string | null;
  pendingNodeIds: string[];
  initialPendingCount: number;
  runId: string | null;

  setNodeStatus: (nodeId: string, status: NodeStatus) => void;
  setNodeTraceback: (nodeId: string, traceback: string) => void;
  setAllPending: (nodeIds: string[]) => void;
  setRunning: (running: boolean) => void;
  setCurrentNodeId: (nodeId: string | null) => void;
  setPendingNodeIds: (ids: string[]) => void;
  setRunId: (id: string | null) => void;
  reset: () => void;
}

export const useExecutionStore = create<ExecutionState>((set) => ({
  nodeStatuses: {},
  nodeTracebacks: {},
  isRunning: false,
  currentNodeId: null,
  pendingNodeIds: [],
  initialPendingCount: 0,
  runId: null,

  setNodeStatus: (nodeId, status) =>
    set((state) => {
      const nodeStatuses = { ...state.nodeStatuses, [nodeId]: status };
      const pending = new Set(state.pendingNodeIds);

      if (TERMINAL_STATUSES.has(status)) {
        pending.delete(nodeId);
      }

      return { nodeStatuses, pendingNodeIds: [...pending] };
    }),

  setNodeTraceback: (nodeId, traceback) =>
    set((state) => ({
      nodeTracebacks: { ...state.nodeTracebacks, [nodeId]: traceback },
    })),

  setAllPending: (nodeIds) =>
    set(() => {
      const nodeStatuses: Record<string, NodeStatus> = {};
      for (const id of nodeIds) {
        nodeStatuses[id] = "pending";
      }
      return {
        nodeStatuses,
        pendingNodeIds: [...nodeIds],
        initialPendingCount: nodeIds.length,
      };
    }),

  setRunning: (running) => set({ isRunning: running }),

  setCurrentNodeId: (nodeId) => set({ currentNodeId: nodeId }),

  setPendingNodeIds: (ids) => set({ pendingNodeIds: ids }),

  setRunId: (id) => set({ runId: id }),

  reset: () =>
    set({
      nodeStatuses: {},
      nodeTracebacks: {},
      isRunning: false,
      currentNodeId: null,
      pendingNodeIds: [],
      initialPendingCount: 0,
      runId: null,
    }),
}));

import { create } from "zustand";
import type { NodeStatus, PortType } from "../lib/types";

export type WsStatus = "connected" | "disconnected" | "reconnecting" | "failed";

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
  wsStatus: WsStatus;
  draggingPortType: PortType | null;
  runResult: "success" | "error" | null;
  lastDoneNodeId: string | null;

  setNodeStatus: (nodeId: string, status: NodeStatus) => void;
  setNodeTraceback: (nodeId: string, traceback: string) => void;
  setAllPending: (nodeIds: string[]) => void;
  setRunning: (running: boolean) => void;
  setCurrentNodeId: (nodeId: string | null) => void;
  setPendingNodeIds: (ids: string[]) => void;
  setRunId: (id: string | null) => void;
  setWsStatus: (status: WsStatus) => void;
  setDraggingPortType: (type: PortType | null) => void;
  setRunResult: (result: "success" | "error" | null) => void;
  setLastDoneNodeId: (nodeId: string | null) => void;
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
  wsStatus: "disconnected" as WsStatus,
  draggingPortType: null,
  runResult: null,
  lastDoneNodeId: null,

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

  setWsStatus: (status) => set({ wsStatus: status }),

  setDraggingPortType: (type) => set({ draggingPortType: type }),

  setRunResult: (result) => set({ runResult: result }),

  setLastDoneNodeId: (nodeId) => set({ lastDoneNodeId: nodeId }),

  reset: () =>
    set({
      nodeStatuses: {},
      nodeTracebacks: {},
      isRunning: false,
      currentNodeId: null,
      pendingNodeIds: [],
      initialPendingCount: 0,
      runId: null,
      wsStatus: "disconnected",
      draggingPortType: null,
      runResult: null,
      lastDoneNodeId: null,
    }),
}));

// Expose store on window in dev/test so Playwright can drive state changes.
if (import.meta.env.DEV) {
  (window as unknown as Record<string, unknown>).__EXECUTION_STORE__ = useExecutionStore;
}

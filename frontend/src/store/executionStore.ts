import { create } from "zustand";
import type { NodeStatus } from "../lib/types";

export type { NodeStatus };

interface ExecutionState {
  nodeStatuses: Record<string, NodeStatus>;
  isRunning: boolean;
  currentNodeId: string | null;

  setNodeStatus: (nodeId: string, status: NodeStatus) => void;
  setRunning: (running: boolean) => void;
  setCurrentNodeId: (nodeId: string | null) => void;
  reset: () => void;
}

export const useExecutionStore = create<ExecutionState>((set) => ({
  nodeStatuses: {},
  isRunning: false,
  currentNodeId: null,

  setNodeStatus: (nodeId, status) =>
    set((state) => ({
      nodeStatuses: { ...state.nodeStatuses, [nodeId]: status },
    })),

  setRunning: (running) => set({ isRunning: running }),

  setCurrentNodeId: (nodeId) => set({ currentNodeId: nodeId }),

  reset: () =>
    set({
      nodeStatuses: {},
      isRunning: false,
      currentNodeId: null,
    }),
}));

import { useEffect, useRef } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import type { WsMessage } from "../lib/types";
import { useExecutionStore } from "../store/executionStore";
import { getPipelineStatus } from "../lib/api";

const MAX_BACKOFF = 30_000;

function getWsUrl(pipelineId: string): string {
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  const host = window.location.host;

  const basePath = import.meta.env.BASE_URL.replace(/\/$/, "");
  return `${proto}//${host}${basePath}/ws/pipelines/${pipelineId}`;
}

export function useExecutionSocket(pipelineId: string | undefined) {
  const qc = useQueryClient();
  const wsRef = useRef<WebSocket | null>(null);
  const backoffRef = useRef(1000);
  const mountedRef = useRef(true);

  useEffect(() => {
    mountedRef.current = true;
    if (!pipelineId) return;

    function connect() {
      if (!mountedRef.current || !pipelineId) return;

      const ws = new WebSocket(getWsUrl(pipelineId));
      wsRef.current = ws;

      ws.onopen = () => {
        backoffRef.current = 1000; // reset on successful connect
        useExecutionStore.getState().setWsStatus("connected");

        // On reconnect, check if a run finished while we were disconnected
        const store = useExecutionStore.getState();
        if (store.isRunning && pipelineId) {
          const runIdAtDisconnect = store.runId;
          const checkStatus = (attempt: number) => {
            if (!mountedRef.current) return;
            getPipelineStatus(pipelineId).then((status) => {
              if (!mountedRef.current) return;
              const current = useExecutionStore.getState();
              // Only reset if the same run is still tracked and server says not running
              if (current.isRunning && current.runId === runIdAtDisconnect && !status.is_running) {
                current.setRunning(false);
                current.setCurrentNodeId(null);
                // Don't clear runResult — let query invalidation show the actual outcome
                qc.invalidateQueries({ queryKey: ["pipeline", pipelineId] });
                qc.invalidateQueries({ queryKey: ["runs", pipelineId] });
              }
            }).catch(() => {
              if (attempt < 2 && mountedRef.current) {
                setTimeout(() => checkStatus(attempt + 1), 1000);
              }
            });
          };
          checkStatus(0);
        }
      };

      ws.onmessage = (event) => {
        const msg: WsMessage = JSON.parse(event.data as string);
        const store = useExecutionStore.getState();

        if (!msg.node_id) return;

        switch (msg.status) {
          case "running":
            store.setNodeStatus(msg.node_id, "running");
            store.setCurrentNodeId(msg.node_id);
            break;
          case "done":
            store.setNodeStatus(msg.node_id, msg.cached ? "cached" : "done");
            store.setLastDoneNodeId(msg.node_id);
            qc.invalidateQueries({
              queryKey: ["output", pipelineId, msg.node_id],
            });
            break;
          case "error":
            store.setNodeStatus(msg.node_id, "error");
            store.setLastDoneNodeId(msg.node_id);
            if (msg.traceback) {
              store.setNodeTraceback(msg.node_id, msg.traceback);
            }
            break;
          case "skipped":
            store.setNodeStatus(msg.node_id, "skipped");
            break;
        }

        // Check pipeline completion: all pending nodes resolved
        const updated = useExecutionStore.getState();
        if (updated.isRunning && updated.pendingNodeIds.length === 0) {
          const hasError = Object.values(updated.nodeStatuses).some(
            (s) => s === "error",
          );
          const result = hasError ? "error" : "success";
          store.setRunResult(result);
          if (result === "success") {
            toast.success("Pipeline completed");
          } else {
            toast.error("Pipeline failed");
          }
          store.setRunning(false);
          store.setCurrentNodeId(null);
          qc.invalidateQueries({ queryKey: ["runs", pipelineId] });
          qc.invalidateQueries({ queryKey: ["pipelineStatus", pipelineId] });
        }
      };

      ws.onclose = () => {
        wsRef.current = null;
        if (!mountedRef.current) {
          useExecutionStore.getState().setWsStatus("disconnected");
          return;
        }
        useExecutionStore.getState().setWsStatus("reconnecting");
        const delay = Math.min(backoffRef.current, MAX_BACKOFF);
        backoffRef.current = delay * 2;
        setTimeout(connect, delay);
      };

      ws.onerror = () => {
        ws.close();
      };
    }

    connect();

    return () => {
      mountedRef.current = false;
      wsRef.current?.close();
      wsRef.current = null;
    };
  }, [pipelineId, qc]);
}

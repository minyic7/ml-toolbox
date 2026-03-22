import { useEffect, useRef } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import type { WsMessage } from "../lib/types";
import { useExecutionStore } from "../store/executionStore";
import { getPipelineStatus } from "../lib/api";

const MAX_BACKOFF = 30_000;
const MAX_RETRIES = 10;

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
  const retryCountRef = useRef(0);
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
        retryCountRef.current = 0;
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
        const msg = JSON.parse(event.data as string) as Record<string, unknown>;

        // Handle metadata_updated events (sent with "type" instead of "status")
        if (msg.type === "metadata_updated" && typeof msg.node_id === "string") {
          qc.invalidateQueries({
            queryKey: ["metadata", pipelineId, msg.node_id],
          });
          return;
        }

        // Handle analysis_updated events (CC subprocess output analysis)
        if (msg.type === "analysis_updated" && typeof msg.node_id === "string") {
          qc.invalidateQueries({
            queryKey: ["analysis", pipelineId, msg.node_id],
          });
          return;
        }

        // Handle metadata_propagated events (schema change propagated downstream)
        if (msg.type === "metadata_propagated") {
          qc.invalidateQueries({ queryKey: ["pipeline", pipelineId] });
          qc.invalidateQueries({ queryKey: ["metadata"] });
          const count = Array.isArray(msg.updated_nodes) ? msg.updated_nodes.length : 0;
          toast.success(`Schema updated — ${count} downstream node${count === 1 ? "" : "s"} reconfigured`);
          return;
        }

        // Handle pipeline_updated events (e.g. CC patched node params)
        if (msg.type === "pipeline_updated") {
          qc.invalidateQueries({ queryKey: ["pipeline", pipelineId] });
          if (typeof msg.node_id === "string") {
            toast.info("Node params auto-configured based on upstream data");
          }
          return;
        }

        const wsMsg = msg as unknown as WsMessage;
        const store = useExecutionStore.getState();

        if (!wsMsg.node_id) return;

        switch (wsMsg.status) {
          case "running":
            store.setNodeStatus(wsMsg.node_id!, "running");
            store.setCurrentNodeId(wsMsg.node_id!);
            break;
          case "done":
            store.setNodeStatus(wsMsg.node_id!, wsMsg.cached ? "cached" : "done");
            store.setLastDoneNodeId(wsMsg.node_id!);
            qc.invalidateQueries({
              queryKey: ["output", pipelineId, wsMsg.node_id],
            });
            break;
          case "error":
            store.setNodeStatus(wsMsg.node_id!, "error");
            store.setLastDoneNodeId(wsMsg.node_id!);
            if (wsMsg.traceback) {
              store.setNodeTraceback(wsMsg.node_id!, wsMsg.traceback);
            }
            break;
          case "skipped":
            store.setNodeStatus(wsMsg.node_id!, "skipped");
            break;
        }

        // Check pipeline completion: all pending nodes resolved
        const updated = useExecutionStore.getState();
        if (updated.isRunning && updated.pendingNodeIds.length === 0) {
          const statuses = Object.values(updated.nodeStatuses);
          const hasError = statuses.some((s) => s === "error");
          const result = hasError ? "error" : "success";
          store.setRunResult(result);

          // Build summary
          const total = statuses.length;
          const done = statuses.filter((s) => s === "done").length;
          const cached = statuses.filter((s) => s === "cached").length;
          const errors = statuses.filter((s) => s === "error").length;
          const skipped = statuses.filter((s) => s === "skipped").length;

          if (result === "success") {
            const parts = [`${total} nodes`];
            if (cached > 0) parts.push(`${cached} cached`);
            toast.success(`Pipeline completed: ${parts.join(", ")}`);
          } else {
            toast.error(
              `Pipeline failed: ${errors} error(s), ${done} succeeded, ${skipped} skipped`,
            );
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
        retryCountRef.current += 1;
        if (retryCountRef.current > MAX_RETRIES) {
          useExecutionStore.getState().setWsStatus("failed");
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

import { useEffect, useRef } from "react";
import { useQueryClient } from "@tanstack/react-query";
import type { WsMessage } from "../lib/types";
import { useExecutionStore } from "../store/executionStore";

const MAX_BACKOFF = 30_000;

function getWsUrl(pipelineId: string): string {
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  const host = window.location.host;

  // In production behind /ml-toolbox/ base path, the backend WS is at /ws/...
  // Vite dev proxy handles /ws/* → ws://localhost:8000
  const basePath = import.meta.env.PROD ? "/ml-toolbox" : "";
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
            qc.invalidateQueries({
              queryKey: ["output", pipelineId, msg.node_id],
            });
            break;
          case "error":
            store.setNodeStatus(msg.node_id, "error");
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
          store.setRunning(false);
          store.setCurrentNodeId(null);
          qc.invalidateQueries({ queryKey: ["runs", pipelineId] });
          qc.invalidateQueries({ queryKey: ["pipelineStatus", pipelineId] });
        }
      };

      ws.onclose = () => {
        wsRef.current = null;
        if (!mountedRef.current) return;
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

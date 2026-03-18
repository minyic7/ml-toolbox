import { useEffect, useRef, useCallback } from "react";
import type { WsMessage } from "@/lib/types";

export interface UseWebSocketOptions {
  /** Pipeline ID to subscribe to. Pass undefined to disconnect. */
  pipelineId: string | undefined;
  /** Called for every status message received from the server. */
  onMessage: (msg: WsMessage) => void;
}

/**
 * Connects to `WS /ws/pipelines/{id}` when a pipeline is loaded.
 * Reconnects automatically on disconnect with exponential backoff.
 * Disconnects when pipelineId becomes undefined or component unmounts.
 */
export function useWebSocket({ pipelineId, onMessage }: UseWebSocketOptions) {
  const onMessageRef = useRef(onMessage);
  onMessageRef.current = onMessage;

  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const backoff = useRef(1000);

  const cleanup = useCallback(() => {
    if (reconnectTimer.current) {
      clearTimeout(reconnectTimer.current);
      reconnectTimer.current = null;
    }
    if (wsRef.current) {
      wsRef.current.close();
      wsRef.current = null;
    }
  }, []);

  useEffect(() => {
    if (!pipelineId) {
      cleanup();
      return;
    }

    function connect() {
      const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
      const base = import.meta.env.VITE_API_BASE
        ? import.meta.env.VITE_API_BASE.replace(/^http/, "ws")
        : `${proto}//${window.location.host}`;
      const url = `${base}/ws/pipelines/${pipelineId}`;

      const ws = new WebSocket(url);
      wsRef.current = ws;

      ws.onopen = () => {
        backoff.current = 1000;
      };

      ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data as string) as WsMessage;
          onMessageRef.current(msg);
        } catch {
          // ignore malformed messages
        }
      };

      ws.onclose = () => {
        wsRef.current = null;
        // Reconnect with exponential backoff (max 30s)
        reconnectTimer.current = setTimeout(() => {
          connect();
        }, backoff.current);
        backoff.current = Math.min(backoff.current * 2, 30_000);
      };

      ws.onerror = () => {
        // onclose will fire after this, triggering reconnect
      };
    }

    connect();

    return cleanup;
  }, [pipelineId, cleanup]);
}

import { useEffect, useRef, useState } from "react";
import { useExecutionStore } from "../../store/executionStore";
import type { WsStatus } from "../../store/executionStore";
import { Button } from "@/components/ui/button";

export default function DisconnectionBanner() {
  const wsStatus = useExecutionStore((s) => s.wsStatus);
  const isRunning = useExecutionStore((s) => s.isRunning);

  const [visible, setVisible] = useState(false);
  const [midRunDisconnect, setMidRunDisconnect] = useState(false);
  const prevStatusRef = useRef<WsStatus>(wsStatus);
  const dismissTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  // Track whether we've ever been disconnected so "connected" after disconnect shows the green banner
  const wasDisconnectedRef = useRef(false);

  useEffect(() => {
    prevStatusRef.current = wsStatus;

    if (dismissTimerRef.current) {
      clearTimeout(dismissTimerRef.current);
      dismissTimerRef.current = null;
    }

    if (wsStatus === "reconnecting") {
      wasDisconnectedRef.current = true;
      setVisible(true);
      // Check if a run was in progress at the moment of disconnect
      if (useExecutionStore.getState().isRunning) {
        setMidRunDisconnect(true);
      }
    } else if (wsStatus === "connected" && wasDisconnectedRef.current) {
      // Show green "Reconnected" banner, then auto-dismiss after 2s (unless mid-run)
      setVisible(true);
      if (!midRunDisconnect) {
        dismissTimerRef.current = setTimeout(() => {
          setVisible(false);
          wasDisconnectedRef.current = false;
        }, 2000);
      }
    } else if (wsStatus === "disconnected") {
      wasDisconnectedRef.current = true;
      setVisible(true);
    }

    return () => {
      if (dismissTimerRef.current) clearTimeout(dismissTimerRef.current);
    };
  }, [wsStatus, midRunDisconnect]);

  // Reset midRunDisconnect when run completes and we're connected
  useEffect(() => {
    if (!isRunning && wsStatus === "connected" && midRunDisconnect) {
      setMidRunDisconnect(false);
      dismissTimerRef.current = setTimeout(() => {
        setVisible(false);
        wasDisconnectedRef.current = false;
      }, 2000);
    }
  }, [isRunning, wsStatus, midRunDisconnect]);

  const handleDismiss = () => {
    setVisible(false);
    setMidRunDisconnect(false);
    wasDisconnectedRef.current = false;
  };

  if (!visible) return null;

  const isReconnecting = wsStatus === "reconnecting" || wsStatus === "disconnected";

  let text: string;
  if (midRunDisconnect && isReconnecting) {
    text = "Run may still be in progress. Refresh to check.";
  } else if (isReconnecting) {
    text = "Connection lost. Reconnecting\u2026";
  } else {
    text = "Reconnected";
  }

  const bgColor = isReconnecting
    ? "rgba(186, 117, 23, 0.15)" // --warning-amber at 15%
    : "rgba(99, 153, 34, 0.15)"; // --success-green at 15%

  const textColor = isReconnecting
    ? "var(--warning-amber)"
    : "var(--success-green)";

  return (
    <div
      style={{
        height: 32,
        backgroundColor: bgColor,
        color: textColor,
        fontSize: 12,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        gap: 8,
        animation: "banner-slide-down 0.2s ease-out",
      }}
    >
      {isReconnecting && (
        <span
          style={{
            display: "inline-block",
            width: 6,
            height: 6,
            borderRadius: "50%",
            backgroundColor: "var(--warning-amber)",
            animation: "banner-pulse 1.5s ease-in-out infinite",
          }}
        />
      )}
      <span>{text}</span>
      {midRunDisconnect && (
        <Button
          variant="link"
          className="h-auto p-0 text-xs underline"
          style={{ color: textColor }}
          onClick={handleDismiss}
        >
          Dismiss
        </Button>
      )}
      <style>{`
        @keyframes banner-slide-down {
          from { transform: translateY(-100%); opacity: 0; }
          to { transform: translateY(0); opacity: 1; }
        }
        @keyframes banner-pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.4; }
        }
      `}</style>
    </div>
  );
}

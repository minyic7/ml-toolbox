import { useEffect, useRef, useCallback, useMemo, useState } from "react";
import { Terminal } from "@xterm/xterm";
import { FitAddon } from "@xterm/addon-fit";
import { WebLinksAddon } from "@xterm/addon-web-links";
import "@xterm/xterm/css/xterm.css";
import { TerminalSquare, X, Maximize2, Minimize2 } from "lucide-react";

interface PipelineTerminalProps {
  pipelineId: string;
  onClose: () => void;
}

export default function PipelineTerminal({
  pipelineId,
  onClose,
}: PipelineTerminalProps) {
  const termRef = useRef<HTMLDivElement>(null);
  const terminalRef = useRef<Terminal | null>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const fitAddonRef = useRef<FitAddon | null>(null);

  // ── Fullscreen ─────────────────────────────────────────────
  const [isFullscreen, setIsFullscreen] = useState(false);

  useEffect(() => {
    if (!isFullscreen) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") setIsFullscreen(false);
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [isFullscreen]);

  // ── Resizable width ──────────────────────────────────────────
  const [width, setWidth] = useState(480);
  const [dragging, setDragging] = useState(false);
  const [handleHovered, setHandleHovered] = useState(false);
  const dragStartX = useRef(0);
  const dragStartWidth = useRef(480);

  const minWidth = 300;
  const maxWidth = useMemo(() => Math.floor(window.innerWidth * 0.7), []);

  useEffect(() => {
    if (!dragging) return;
    const handleMouseMove = (e: MouseEvent) => {
      e.preventDefault();
      const delta = dragStartX.current - e.clientX;
      const newWidth = Math.min(
        maxWidth,
        Math.max(minWidth, dragStartWidth.current + delta),
      );
      setWidth(newWidth);
    };
    const handleMouseUp = () => {
      setDragging(false);
    };
    document.addEventListener("mousemove", handleMouseMove);
    document.addEventListener("mouseup", handleMouseUp);
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";
    return () => {
      document.removeEventListener("mousemove", handleMouseMove);
      document.removeEventListener("mouseup", handleMouseUp);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
    };
  }, [dragging, maxWidth]);

  const handleDragStart = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      dragStartX.current = e.clientX;
      dragStartWidth.current = width;
      setDragging(true);
    },
    [width],
  );

  // ── Resize protocol (0x01 + JSON) ────────────────────────────
  const sendResize = useCallback((cols: number, rows: number) => {
    const ws = wsRef.current;
    if (ws && ws.readyState === WebSocket.OPEN) {
      const json = JSON.stringify({ cols, rows });
      const payload = new Uint8Array(1 + json.length);
      payload[0] = 0x01;
      for (let i = 0; i < json.length; i++) {
        payload[i + 1] = json.charCodeAt(i);
      }
      ws.send(payload.buffer);
    }
  }, []);

  // ── WebSocket connection ─────────────────────────────────────
  const connectWs = useCallback(
    (terminal: Terminal) => {
      const proto =
        window.location.protocol === "https:" ? "wss:" : "ws:";
      const wsUrl = `${proto}//${window.location.host}/ws/pipelines/${pipelineId}/terminal`;

      const ws = new WebSocket(wsUrl);
      ws.binaryType = "arraybuffer";

      ws.onopen = () => {
        terminal.writeln("\x1b[2m Connecting to Pipeline CC...\x1b[0m");
        sendResize(terminal.cols, terminal.rows);
      };

      ws.onmessage = (event) => {
        if (event.data instanceof ArrayBuffer) {
          terminal.write(new Uint8Array(event.data));
        } else {
          terminal.write(event.data);
        }
      };

      ws.onclose = () => {
        terminal.writeln(
          "\r\n\x1b[2m Connection closed.\x1b[0m",
        );
      };

      ws.onerror = () => {
        terminal.writeln(
          "\r\n\x1b[31m WebSocket error — check backend.\x1b[0m",
        );
      };

      terminal.onData((data) => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(new TextEncoder().encode(data));
        }
      });

      wsRef.current = ws;
    },
    [pipelineId, sendResize],
  );

  // ── Terminal initialization ──────────────────────────────────
  useEffect(() => {
    if (!termRef.current) return;

    const terminal = new Terminal({
      cursorBlink: true,
      fontSize: 13,
      fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
      theme: {
        background: "#1a1b26",
        foreground: "#a9b1d6",
        cursor: "#c0caf5",
        selectionBackground: "#33467c",
        black: "#414868",
        red: "#f7768e",
        green: "#9ece6a",
        yellow: "#e0af68",
        blue: "#7aa2f7",
        magenta: "#bb9af7",
        cyan: "#7dcfff",
        white: "#c0caf5",
      },
    });

    const fitAddon = new FitAddon();
    terminal.loadAddon(fitAddon);
    terminal.loadAddon(new WebLinksAddon());
    terminal.open(termRef.current);
    fitAddon.fit();

    terminalRef.current = terminal;
    fitAddonRef.current = fitAddon;

    connectWs(terminal);

    // Resize observer
    const ro = new ResizeObserver(() => {
      fitAddon.fit();
      sendResize(terminal.cols, terminal.rows);
    });
    ro.observe(termRef.current);

    return () => {
      terminal.dispose();
      ro.disconnect();
      wsRef.current?.close();
    };
  }, [pipelineId, connectWs, sendResize]);

  // Re-fit when width or fullscreen changes
  useEffect(() => {
    fitAddonRef.current?.fit();
  }, [width, isFullscreen]);

  return (
    <div
      className="flex flex-col h-full"
      style={
        isFullscreen
          ? {
              position: "fixed" as const,
              top: 0,
              left: 0,
              right: 0,
              bottom: 0,
              width: "100vw",
              zIndex: 50,
              background: "#1a1b26",
            }
          : {
              width,
              minWidth: width,
              background: "#1a1b26",
              borderLeft: "1px solid var(--border-default)",
              position: "relative" as const,
            }
      }
    >
      {/* Drag handle for resizing */}
      {!isFullscreen && (
        <div
          onMouseDown={handleDragStart}
          onMouseEnter={() => setHandleHovered(true)}
          onMouseLeave={() => setHandleHovered(false)}
          style={{
            position: "absolute",
            top: 0,
            left: 0,
            bottom: 0,
            width: 4,
            cursor: "col-resize",
            zIndex: 10,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
          }}
        >
          <div
            style={{
              width: 2,
              height: 32,
              borderRadius: 1,
              background:
                dragging || handleHovered
                  ? "var(--text-muted)"
                  : "var(--border-default)",
              transition: "background 150ms",
            }}
          />
        </div>
      )}

      {/* Header */}
      <div
        className="flex items-center gap-2 px-3 shrink-0"
        style={{
          height: 38,
          background: "#1a1b26",
          borderBottom: "1px solid #292e42",
        }}
      >
        <TerminalSquare
          size={14}
          style={{ color: "#7aa2f7", flexShrink: 0 }}
        />
        <span
          style={{
            fontFamily: "'Inter', sans-serif",
            fontWeight: 600,
            fontSize: 11,
            color: "#a9b1d6",
            whiteSpace: "nowrap",
            overflow: "hidden",
            textOverflow: "ellipsis",
            textTransform: "uppercase",
            letterSpacing: "0.04em",
          }}
        >
          Pipeline CC
        </span>

        <div style={{ flex: 1 }} />

        {/* Fullscreen toggle */}
        <button
          onClick={() => setIsFullscreen(!isFullscreen)}
          aria-label={isFullscreen ? "Exit fullscreen" : "Fullscreen"}
          title={isFullscreen ? "Exit fullscreen" : "Fullscreen"}
          style={{
            width: 24,
            height: 24,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            border: "1px solid #292e42",
            borderRadius: 4,
            background: "transparent",
            cursor: "pointer",
            color: "#565f89",
            flexShrink: 0,
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.background = "#292e42";
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.background = "transparent";
          }}
        >
          {isFullscreen ? <Minimize2 size={13} /> : <Maximize2 size={13} />}
        </button>

        {/* Close button */}
        <button
          onClick={onClose}
          title="Close (Esc)"
          style={{
            width: 24,
            height: 24,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            border: "1px solid #292e42",
            borderRadius: 4,
            background: "transparent",
            cursor: "pointer",
            color: "#565f89",
            flexShrink: 0,
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.background = "#292e42";
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.background = "transparent";
          }}
        >
          <X size={13} />
        </button>
      </div>

      {/* Terminal */}
      <div ref={termRef} style={{ flex: 1, padding: "4px 0 0 4px" }} />
    </div>
  );
}

import { useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { getNodeDefinitions } from "../../lib/api";
import type { NodeDefinition } from "../../lib/types";
import NodeLibraryGroup from "./NodeLibraryGroup";

const STORAGE_KEY = "ml-toolbox-sidebar-open";
const EXPANDED_WIDTH = 220;
const COLLAPSED_WIDTH = 40;

function readStoredOpen(): boolean {
  try {
    const v = localStorage.getItem(STORAGE_KEY);
    return v === null ? true : v === "true";
  } catch {
    return true;
  }
}

interface SidebarProps {
  onAddNode?: (nodeType: string) => void;
}

export default function Sidebar({ onAddNode }: SidebarProps) {
  const [open, setOpen] = useState(readStoredOpen);
  const [search, setSearch] = useState("");
  const searchRef = useRef<HTMLInputElement>(null);

  // Persist collapse state
  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, String(open));
    } catch {
      // ignore
    }
  }, [open]);

  // Global "/" shortcut to focus search
  useEffect(() => {
    function handleKey(e: KeyboardEvent) {
      if (
        e.key === "/" &&
        !e.ctrlKey &&
        !e.metaKey &&
        !(e.target instanceof HTMLInputElement) &&
        !(e.target instanceof HTMLTextAreaElement)
      ) {
        e.preventDefault();
        if (!open) setOpen(true);
        // Small delay to let sidebar expand before focusing
        requestAnimationFrame(() => searchRef.current?.focus());
      }
    }
    window.addEventListener("keydown", handleKey);
    return () => window.removeEventListener("keydown", handleKey);
  }, [open]);

  // Fetch node definitions
  const { data: nodes = [], isLoading } = useQuery<NodeDefinition[]>({
    queryKey: ["nodeDefinitions"],
    queryFn: getNodeDefinitions,
    staleTime: 5 * 60 * 1000,
  });

  // Group by category, filtered by search
  const grouped = useMemo(() => {
    const q = search.toLowerCase().trim();
    const filtered = q
      ? nodes.filter(
          (n) =>
            n.label.toLowerCase().includes(q) ||
            n.category.toLowerCase().includes(q) ||
            n.description.toLowerCase().includes(q),
        )
      : nodes;

    const groups: Record<string, NodeDefinition[]> = {};
    for (const node of filtered) {
      const cat = node.category;
      if (!groups[cat]) groups[cat] = [];
      groups[cat].push(node);
    }

    // Sort categories in a stable order
    const order = ["ingest", "transform", "train", "evaluate", "export", "utility"];
    return Object.entries(groups).sort(
      ([a], [b]) => (order.indexOf(a) === -1 ? 99 : order.indexOf(a)) - (order.indexOf(b) === -1 ? 99 : order.indexOf(b)),
    );
  }, [nodes, search]);

  function handleAdd(nodeType: string) {
    onAddNode?.(nodeType);
  }

  return (
    <aside
      className="flex h-full flex-col border-r transition-[width] duration-200"
      style={{
        width: open ? EXPANDED_WIDTH : COLLAPSED_WIDTH,
        minWidth: open ? EXPANDED_WIDTH : COLLAPSED_WIDTH,
        borderColor: "var(--border-default)",
        backgroundColor: "var(--node-bg)",
      }}
    >
      {/* Toggle button */}
      <div className="flex items-center justify-between border-b px-2 py-2" style={{ borderColor: "var(--border-default)" }}>
        {open && (
          <span className="text-xs font-semibold uppercase tracking-wide" style={{ color: "var(--text-secondary)" }}>
            Nodes
          </span>
        )}
        <button
          onClick={() => setOpen((v) => !v)}
          className="flex h-6 w-6 items-center justify-center rounded transition-colors hover:bg-[var(--canvas-bg)]"
          title={open ? "Collapse sidebar" : "Expand sidebar"}
          style={{ color: "var(--text-secondary)" }}
        >
          {open ? "«" : "»"}
        </button>
      </div>

      {open && (
        <>
          {/* Search */}
          <div className="border-b px-2 py-2" style={{ borderColor: "var(--border-default)" }}>
            <input
              ref={searchRef}
              type="text"
              placeholder="Search nodes…  /"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="w-full rounded border px-2 py-1 text-sm outline-none transition-colors focus:border-[var(--accent-blue)]"
              style={{
                borderColor: "var(--border-default)",
                color: "var(--text-primary)",
                backgroundColor: "var(--node-bg)",
              }}
            />
          </div>

          {/* Node list */}
          <div className="flex-1 overflow-y-auto px-1 py-1">
            {isLoading && (
              <p className="px-2 py-4 text-center text-xs" style={{ color: "var(--text-muted)" }}>
                Loading…
              </p>
            )}
            {!isLoading && grouped.length === 0 && (
              <p className="px-2 py-4 text-center text-xs" style={{ color: "var(--text-muted)" }}>
                {search ? "No matching nodes" : "No nodes available"}
              </p>
            )}
            {grouped.map(([category, categoryNodes]) => (
              <NodeLibraryGroup
                key={category}
                category={category}
                nodes={categoryNodes}
                onAddNode={handleAdd}
              />
            ))}
          </div>
        </>
      )}
    </aside>
  );
}

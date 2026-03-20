import { useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  Download,
  Shuffle,
  Brain,
  BarChart3,
  Upload,
  Wrench,
  Box,
} from "lucide-react";
import type { LucideIcon } from "lucide-react";
import { getNodeDefinitions } from "../../lib/api";
import type { NodeDefinition } from "../../lib/types";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Separator } from "@/components/ui/separator";
import NodeLibraryGroup from "./NodeLibraryGroup";
import SidebarRailItem from "./SidebarRailItem";

const CATEGORY_ICONS: Record<string, LucideIcon> = {
  ingest: Download,
  transform: Shuffle,
  train: Brain,
  evaluate: BarChart3,
  export: Upload,
  utility: Wrench,
  demo: Box,
};

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
      className="flex h-full flex-col border-r border-border bg-background transition-[width] duration-200"
      style={{
        width: open ? EXPANDED_WIDTH : COLLAPSED_WIDTH,
        minWidth: open ? EXPANDED_WIDTH : COLLAPSED_WIDTH,
      }}
    >
      {/* Toggle button */}
      <div className="flex items-center justify-between px-2 py-2">
        {open && (
          <span className="text-xs font-semibold uppercase tracking-wide" style={{ color: "var(--text-secondary)" }}>
            Nodes
          </span>
        )}
        <Button
          variant="ghost"
          size="icon"
          className="h-6 w-6 text-[var(--text-secondary)]"
          onClick={() => setOpen((v) => !v)}
          title={open ? "Collapse sidebar" : "Expand sidebar"}
          aria-label={open ? "Collapse sidebar" : "Expand sidebar"}
        >
          {open ? "\u00AB" : "\u00BB"}
        </Button>
      </div>

      <Separator />

      {!open && (
        <div className="flex flex-1 flex-col items-center gap-1 overflow-y-auto py-2">
          {grouped.map(([category, categoryNodes]) => (
            <SidebarRailItem
              key={category}
              category={category}
              icon={CATEGORY_ICONS[category] ?? Box}
              nodes={categoryNodes}
              onAddNode={handleAdd}
            />
          ))}
        </div>
      )}

      {open && (
        <>
          {/* Search */}
          <div className="px-2 py-2">
            <Input
              ref={searchRef}
              type="text"
              placeholder="Search nodes...  /"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="h-8 text-sm"
            />
          </div>

          <Separator />

          {/* Node list */}
          <div className="flex-1 overflow-y-auto px-1 py-1">
            {isLoading && (
              <p className="px-2 py-4 text-center text-xs" style={{ color: "var(--text-muted)" }}>
                Loading...
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

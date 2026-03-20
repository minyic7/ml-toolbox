import { useState, useRef, useEffect, useCallback } from "react";
import { Search, ChevronDown } from "lucide-react";
import { STATUS_BADGE_COLORS, STATUS_LABELS } from "../../lib/runConstants";

interface FilterRowProps {
  pipelines: { id: string; name: string }[];
  selectedPipelineId: string | null;
  onPipelineChange: (id: string | null) => void;
  selectedStatuses: string[];
  onStatusToggle: (status: string) => void;
  searchQuery: string;
  onSearchChange: (q: string) => void;
  runCount: number;
}

/* ── Status pill definitions ─────────────────────────────────────── */

const STATUS_PREFIX: Record<string, string> = { done: "\u2713 ", error: "\u2717 ", cancelled: "" };
const STATUS_KEYS = ["done", "error", "cancelled"] as const;
const MUTED_COLOR = "var(--text-muted)";

/* ── Component ───────────────────────────────────────────────────── */

export default function FilterRow({
  pipelines,
  selectedPipelineId,
  onPipelineChange,
  selectedStatuses,
  onStatusToggle,
  searchQuery,
  onSearchChange,
  runCount,
}: FilterRowProps) {
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const [dropdownSearch, setDropdownSearch] = useState("");
  const dropdownRef = useRef<HTMLDivElement>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);

  const selectedPipeline = pipelines.find((p) => p.id === selectedPipelineId);
  const displayName = selectedPipeline?.name ?? "All pipelines";

  /* Close dropdown on outside click */
  useEffect(() => {
    if (!dropdownOpen) return;
    function handleClick(e: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setDropdownOpen(false);
        setDropdownSearch("");
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [dropdownOpen]);

  /* Focus dropdown search when opened */
  useEffect(() => {
    if (dropdownOpen) {
      requestAnimationFrame(() => {
        dropdownRef.current?.querySelector<HTMLInputElement>("input")?.focus();
      });
    }
  }, [dropdownOpen]);

  const filteredPipelines = pipelines.filter((p) =>
    p.name.toLowerCase().includes(dropdownSearch.toLowerCase()),
  );

  const handleStatusToggle = useCallback(
    (key: string) => {
      // Prevent deselecting the last active status
      if (selectedStatuses.includes(key) && selectedStatuses.length === 1) return;
      onStatusToggle(key);
    },
    [selectedStatuses, onStatusToggle],
  );

  return (
    <div style={styles.row}>
      {/* Pipeline selector */}
      <div style={styles.section} ref={dropdownRef}>
        <button
          style={styles.pipelineBtn}
          onClick={() => setDropdownOpen(!dropdownOpen)}
        >
          <span style={styles.pipelineDot} />
          <span style={styles.pipelineLabel}>{displayName}</span>
          <ChevronDown size={12} color="var(--text-muted)" />
        </button>

        {dropdownOpen && (
          <div style={styles.dropdown}>
            <div style={styles.dropdownSearchWrap}>
              <input
                type="text"
                placeholder="Search…"
                value={dropdownSearch}
                onChange={(e) => setDropdownSearch(e.target.value)}
                style={styles.dropdownSearchInput}
              />
            </div>
            <div style={styles.dropdownList}>
              <button
                style={{
                  ...styles.dropdownItem,
                  fontWeight: selectedPipelineId === null ? 600 : 400,
                }}
                onClick={() => {
                  onPipelineChange(null);
                  setDropdownOpen(false);
                  setDropdownSearch("");
                }}
              >
                <span style={styles.pipelineDot} />
                <span style={{ flex: 1 }}>All pipelines</span>
                <span style={styles.dropdownCount}>{pipelines.length}</span>
              </button>
              {filteredPipelines.map((p) => (
                <button
                  key={p.id}
                  style={{
                    ...styles.dropdownItem,
                    fontWeight: selectedPipelineId === p.id ? 600 : 400,
                  }}
                  onClick={() => {
                    onPipelineChange(p.id);
                    setDropdownOpen(false);
                    setDropdownSearch("");
                  }}
                >
                  <span style={styles.pipelineDot} />
                  <span
                    style={{
                      flex: 1,
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                    }}
                  >
                    {p.name}
                  </span>
                </button>
              ))}
            </div>
          </div>
        )}
      </div>

      <div style={styles.hairline} />

      {/* Status pills */}
      <div style={styles.section}>
        <div style={styles.pillGroup}>
          {STATUS_KEYS.map((key) => {
            const active = selectedStatuses.includes(key);
            const badge = STATUS_BADGE_COLORS[key];
            return (
              <button
                key={key}
                style={{
                  ...styles.pill,
                  backgroundColor: active ? badge.bg : "transparent",
                  color: active ? badge.color : MUTED_COLOR,
                }}
                onClick={() => handleStatusToggle(key)}
              >
                {`${STATUS_PREFIX[key] ?? ""}${STATUS_LABELS[key]}`}
              </button>
            );
          })}
        </div>
      </div>

      <div style={styles.hairline} />

      {/* Search input */}
      <div style={{ ...styles.section, flex: 1 }}>
        <div style={styles.searchWrap}>
          <Search size={13} color="var(--text-muted)" style={{ flexShrink: 0 }} />
          <input
            ref={searchInputRef}
            type="text"
            placeholder="Search run ID…"
            value={searchQuery}
            onChange={(e) => onSearchChange(e.target.value)}
            style={styles.searchInput}
          />
        </div>
      </div>

      {/* Run count */}
      <span style={styles.runCount}>
        {runCount} {runCount === 1 ? "run" : "runs"}
      </span>
    </div>
  );
}

/* ── Styles ───────────────────────────────────────────────────────── */

const styles: Record<string, React.CSSProperties> = {
  row: {
    height: 32,
    display: "flex",
    alignItems: "center",
    gap: 0,
    padding: "0 12px",
    borderBottom: "1px solid var(--border-default)",
    backgroundColor: "var(--node-bg)",
    fontFamily: "'Inter', sans-serif",
    fontSize: 9,
  },
  section: {
    display: "flex",
    alignItems: "center",
    position: "relative",
  },
  hairline: {
    width: 1,
    height: 16,
    backgroundColor: "var(--border-default)",
    margin: "0 8px",
    opacity: 0.5,
  },

  /* Pipeline selector */
  pipelineBtn: {
    display: "flex",
    alignItems: "center",
    gap: 5,
    background: "none",
    border: "none",
    cursor: "pointer",
    padding: "4px 6px",
    borderRadius: 4,
    fontFamily: "'Inter', sans-serif",
    fontSize: 9,
    color: "var(--text-primary)",
    whiteSpace: "nowrap",
  },
  pipelineDot: {
    display: "inline-block",
    width: 6,
    height: 6,
    borderRadius: "50%",
    backgroundColor: "var(--accent-primary)",
    flexShrink: 0,
  },
  pipelineLabel: {
    maxWidth: 140,
    overflow: "hidden",
    textOverflow: "ellipsis",
    whiteSpace: "nowrap",
  },

  /* Dropdown */
  dropdown: {
    position: "absolute",
    top: "100%",
    left: 0,
    marginTop: 4,
    width: 220,
    backgroundColor: "var(--node-bg)",
    border: "1px solid var(--border-default)",
    borderRadius: 8,
    boxShadow: "0 4px 16px rgba(0,0,0,.08)",
    zIndex: 50,
    overflow: "hidden",
  },
  dropdownSearchWrap: {
    padding: "6px 8px",
    borderBottom: "1px solid var(--border-default)",
  },
  dropdownSearchInput: {
    width: "100%",
    border: "none",
    outline: "none",
    fontFamily: "'Inter', sans-serif",
    fontSize: 10,
    color: "var(--text-primary)",
    backgroundColor: "transparent",
    padding: "2px 0",
  },
  dropdownList: {
    maxHeight: 200,
    overflowY: "auto",
    padding: "4px 0",
  },
  dropdownItem: {
    display: "flex",
    alignItems: "center",
    gap: 6,
    width: "100%",
    padding: "5px 10px",
    background: "none",
    border: "none",
    cursor: "pointer",
    fontFamily: "'Inter', sans-serif",
    fontSize: 10,
    color: "var(--text-primary)",
    textAlign: "left",
  },
  dropdownCount: {
    fontSize: 9,
    color: "var(--text-muted)",
  },

  /* Status pills */
  pillGroup: {
    display: "flex",
    alignItems: "center",
    gap: 4,
  },
  pill: {
    display: "inline-flex",
    alignItems: "center",
    padding: "2px 8px",
    borderRadius: 9999,
    border: "none",
    cursor: "pointer",
    fontFamily: "'Inter', sans-serif",
    fontSize: 9,
    fontWeight: 500,
    lineHeight: "16px",
    whiteSpace: "nowrap",
    transition: "background-color 0.1s, color 0.1s",
  },

  /* Search */
  searchWrap: {
    display: "flex",
    alignItems: "center",
    gap: 5,
    flex: 1,
  },
  searchInput: {
    flex: 1,
    border: "none",
    outline: "none",
    fontFamily: "'Inter', sans-serif",
    fontSize: 9,
    color: "var(--text-primary)",
    backgroundColor: "transparent",
    padding: 0,
  },

  /* Run count */
  runCount: {
    fontFamily: "'Inter', sans-serif",
    fontSize: 9,
    color: "var(--text-muted)",
    whiteSpace: "nowrap",
    marginLeft: 8,
  },
};

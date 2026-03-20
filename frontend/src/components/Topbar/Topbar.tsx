import { useState, useCallback, useEffect, useMemo, useRef } from "react";
import { useNavigate } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  getPipeline,
  updatePipeline,
  listPipelines,
  createPipeline,
  listRuns,
  deleteRun,
  patchPipelineSettings,
} from "../../lib/api";
import type { PipelineSettings } from "../../lib/types";
import { useExecutionStore } from "../../store/executionStore";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { Menu, Clock, Settings } from "lucide-react";
import { toast } from "sonner";
import PipelineNameInput from "./PipelineNameInput";
import RunButton from "./RunButton";
import CancelButton from "./CancelButton";
import AutoSaveIndicator from "./AutoSaveIndicator";
import type { SaveStatus } from "./AutoSaveIndicator";
import SettingsModal from "./SettingsModal";
import HistoryDrawer from "./HistoryDrawer";
import NavDrawer from "./NavDrawer";

interface TopbarProps {
  pipelineId: string;
  onViewRun?: (runId: string) => void;
}

export default function Topbar({ pipelineId, onViewRun }: TopbarProps) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const [saveStatus, setSaveStatus] = useState<SaveStatus>("saved");
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [historyOpen, setHistoryOpen] = useState(false);
  const [navOpen, setNavOpen] = useState(false);

  const isRunning = useExecutionStore((s) => s.isRunning);
  const currentNodeId = useExecutionStore((s) => s.currentNodeId);
  const runResult = useExecutionStore((s) => s.runResult);
  const setRunResult = useExecutionStore((s) => s.setRunResult);

  // ── Queries ───────────────────────────────────────────────────────
  const { data: pipeline } = useQuery({
    queryKey: ["pipeline", pipelineId],
    queryFn: () => getPipeline(pipelineId),
  });

  const { data: pipelines = [] } = useQuery({
    queryKey: ["pipelines"],
    queryFn: listPipelines,
    enabled: navOpen,
  });

  const { data: runs = [] } = useQuery({
    queryKey: ["runs", pipelineId],
    queryFn: () => listRuns(pipelineId),
    enabled: historyOpen,
  });

  // ── Mutations ─────────────────────────────────────────────────────
  const renameMutation = useMutation({
    mutationFn: (name: string) => {
      if (!pipeline) throw new Error("Pipeline not loaded");
      return updatePipeline(pipelineId, { ...pipeline, name });
    },
    onMutate: () => setSaveStatus("saving"),
    onSuccess: () => {
      setSaveStatus("saved");
      queryClient.invalidateQueries({ queryKey: ["pipeline", pipelineId] });
      queryClient.invalidateQueries({ queryKey: ["pipelines"] });
    },
    onError: () => setSaveStatus("error"),
  });

  const settingsMutation = useMutation({
    mutationFn: (patch: Partial<PipelineSettings>) =>
      patchPipelineSettings(pipelineId, patch),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["pipeline", pipelineId] });
    },
  });

  const deleteRunMutation = useMutation({
    mutationFn: (runId: string) => deleteRun(pipelineId, runId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["runs", pipelineId] });
    },
  });

  const createMutation = useMutation({
    mutationFn: () => createPipeline({ name: "Untitled Pipeline" }),
    onSuccess: (data) => {
      setNavOpen(false);
      navigate(`/pipeline/${data.id}`);
    },
  });

  const lastRenameRef = useRef<string | null>(null);

  const handleRename = useCallback(
    (name: string) => {
      lastRenameRef.current = name;
      renameMutation.mutate(name, {
        onError: () => toast.error("Failed to rename pipeline"),
      });
    },
    [renameMutation],
  );

  const handleRetry = useCallback(() => {
    if (lastRenameRef.current !== null) {
      renameMutation.mutate(lastRenameRef.current, {
        onError: () => toast.error("Failed to rename pipeline"),
      });
    }
  }, [renameMutation]);

  const nodeIds = useMemo(
    () => pipeline?.nodes.map((n) => n.id) ?? [],
    [pipeline],
  );

  // Clear runResult after 3s (bar fades)
  useEffect(() => {
    if (!runResult) return;
    const timer = setTimeout(() => setRunResult(null), 3000);
    return () => clearTimeout(timer);
  }, [runResult, setRunResult]);

  // Current running node label
  const currentNodeLabel = useMemo(() => {
    if (!isRunning || !currentNodeId || !pipeline) return null;
    const node = pipeline.nodes.find((n) => n.id === currentNodeId);
    return node?.name ?? node?.type ?? null;
  }, [isRunning, currentNodeId, pipeline]);

  return (
    <header
      className="relative flex items-center shrink-0 px-3 gap-3 border-b select-none"
      style={{
        height: 46,
        backgroundColor: "var(--node-bg)",
        borderColor: "var(--border-default)",
      }}
    >
      {/* ── Progress bar (bottom edge, 2px full-width) ── */}
      <div className="absolute bottom-0 left-0 right-0" style={{ height: 2 }}>
        {isRunning && !runResult && (
          <div className="topbar-progress-indeterminate" />
        )}
        {runResult && (
          <div
            className="topbar-progress-fill"
            style={{
              backgroundColor:
                runResult === "error"
                  ? "var(--error-red)"
                  : "var(--success-green)",
            }}
          />
        )}
      </div>

      {/* ── Left section: nav menu ── */}
      <Tooltip>
        <TooltipTrigger asChild>
          <button
            type="button"
            className="shrink-0 flex items-center justify-center"
            style={{
              width: 28,
              height: 28,
              borderRadius: 6,
              border: "none",
              background: "transparent",
              color: "var(--text-secondary)",
              cursor: "pointer",
            }}
            onClick={() => setNavOpen(true)}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = "var(--ghost-hover-bg)";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = "transparent";
            }}
          >
            <Menu style={{ width: 16, height: 16 }} />
          </button>
        </TooltipTrigger>
        <TooltipContent>Pipelines</TooltipContent>
      </Tooltip>

      {/* ── Logo ── */}
      <span
        style={{
          fontFamily: "'Manrope', sans-serif",
          fontWeight: 800,
          color: "var(--accent-primary)",
          letterSpacing: "-0.02em",
          fontSize: 15,
          whiteSpace: "nowrap",
          userSelect: "none",
        }}
      >
        ML-Toolbox
      </span>

      {/* ── Vertical divider ── */}
      <div
        style={{
          width: 1,
          height: 20,
          backgroundColor: "var(--border-default)",
          flexShrink: 0,
        }}
      />

      {/* ── Pipeline name ── */}
      {pipeline && (
        <PipelineNameInput
          name={pipeline.name}
          onRename={handleRename}
        />
      )}

      {/* ── Auto-save indicator ── */}
      <AutoSaveIndicator status={saveStatus} onRetry={handleRetry} retryDisabled={renameMutation.isPending} />

      {/* ── Spacer ── */}
      <div className="flex-1" />

      {/* ── Right section: Run + Cancel + ghost buttons ── */}
      <div className="flex items-center gap-1.5">
        <RunButton pipelineId={pipelineId} nodeIds={nodeIds} currentNodeLabel={currentNodeLabel} />

        {isRunning && <CancelButton pipelineId={pipelineId} />}

        {/* ⚙ Settings ghost button */}
        <Tooltip>
          <TooltipTrigger asChild>
            <button
              type="button"
              className="flex items-center justify-center shrink-0"
              style={{
                width: 28,
                height: 28,
                borderRadius: 6,
                border: "1px solid var(--border-default)",
                background: "transparent",
                color: "var(--text-secondary)",
                cursor: "pointer",
                transition: "background 0.15s",
              }}
              onClick={() => setSettingsOpen(true)}
              onMouseEnter={(e) => {
                e.currentTarget.style.background = "var(--ghost-hover-bg)";
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.background = "transparent";
              }}
            >
              <Settings style={{ width: 14, height: 14 }} />
            </button>
          </TooltipTrigger>
          <TooltipContent>Settings</TooltipContent>
        </Tooltip>

        {/* 🕐 History ghost button */}
        <Tooltip>
          <TooltipTrigger asChild>
            <button
              type="button"
              className="flex items-center justify-center shrink-0"
              style={{
                width: 28,
                height: 28,
                borderRadius: 6,
                border: "1px solid var(--border-default)",
                background: "transparent",
                color: "var(--text-secondary)",
                cursor: "pointer",
                transition: "background 0.15s",
              }}
              onClick={() => setHistoryOpen(true)}
              onMouseEnter={(e) => {
                e.currentTarget.style.background = "var(--ghost-hover-bg)";
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.background = "transparent";
              }}
            >
              <Clock style={{ width: 14, height: 14 }} />
            </button>
          </TooltipTrigger>
          <TooltipContent>Run history</TooltipContent>
        </Tooltip>
      </div>

      {/* ── Modals & Drawers ── */}
      {pipeline && (
        <SettingsModal
          open={settingsOpen}
          onClose={() => setSettingsOpen(false)}
          settings={pipeline.settings}
          onUpdate={(patch) => settingsMutation.mutate(patch, { onError: () => toast.error("Failed to save settings") })}
        />
      )}

      <HistoryDrawer
        open={historyOpen}
        onClose={() => setHistoryOpen(false)}
        runs={runs}
        onDeleteRun={(runId) => deleteRunMutation.mutate(runId, { onError: () => toast.error("Failed to delete run") })}
        onViewRun={onViewRun}
      />

      <NavDrawer
        open={navOpen}
        onClose={() => setNavOpen(false)}
        pipelines={pipelines}
        currentPipelineId={pipelineId}
        onSelect={(id) => navigate(`/pipeline/${id}`)}
        onCreate={() => createMutation.mutate(undefined, { onError: () => toast.error("Failed to create pipeline") })}
      />
    </header>
  );
}

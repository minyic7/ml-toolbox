import { useState, useCallback, useEffect, useMemo } from "react";
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
import { Button } from "@/components/ui/button";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { Menu, Clock, Settings } from "lucide-react";
import PipelineNameInput from "./PipelineNameInput";
import RunButton from "./RunButton";
import AutoSaveIndicator from "./AutoSaveIndicator";
import type { SaveStatus } from "./AutoSaveIndicator";
import SettingsModal from "./SettingsModal";
import HistoryDrawer from "./HistoryDrawer";
import NavDrawer from "./NavDrawer";

interface TopbarProps {
  pipelineId: string;
}

export default function Topbar({ pipelineId }: TopbarProps) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const [saveStatus, setSaveStatus] = useState<SaveStatus>("saved");
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [historyOpen, setHistoryOpen] = useState(false);
  const [navOpen, setNavOpen] = useState(false);

  const isRunning = useExecutionStore((s) => s.isRunning);
  const pendingNodeIds = useExecutionStore((s) => s.pendingNodeIds);
  const initialPendingCount = useExecutionStore((s) => s.initialPendingCount);
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

  const handleRename = useCallback(
    (name: string) => renameMutation.mutate(name),
    [renameMutation],
  );

  const nodeIds = useMemo(
    () => pipeline?.nodes.map((n) => n.id) ?? [],
    [pipeline],
  );

  // ── Progress bar ──────────────────────────────────────────────────
  const progress = useMemo(() => {
    if (!isRunning || initialPendingCount === 0) return 0;
    return (initialPendingCount - pendingNodeIds.length) / initialPendingCount;
  }, [isRunning, pendingNodeIds.length, initialPendingCount]);

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
      className="relative flex items-center shrink-0 px-3 gap-3 border-b border-border select-none"
      style={{
        height: 48,
        backgroundColor: "var(--node-bg)",
      }}
    >
      {/* Progress bar (overlays bottom edge) */}
      {(isRunning || runResult) && (
        <div
          className="absolute bottom-0 left-0 h-0.5 transition-all duration-300"
          style={{
            width: runResult ? "100%" : `${progress * 100}%`,
            backgroundColor: runResult === "error"
              ? "var(--error-red)"
              : runResult === "success"
                ? "var(--success-green)"
                : "var(--accent-blue)",
            opacity: runResult ? 1 : undefined,
          }}
        />
      )}

      {/* Left section: nav + pipeline name */}
      <div className="flex items-center gap-2 min-w-0">
        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              variant="ghost"
              size="icon"
              className="h-7 w-7 shrink-0 text-[var(--text-secondary)]"
              onClick={() => setNavOpen(true)}
            >
              <Menu className="h-4 w-4" />
            </Button>
          </TooltipTrigger>
          <TooltipContent>Pipelines</TooltipContent>
        </Tooltip>

        {pipeline && (
          <PipelineNameInput
            name={pipeline.name}
            onRename={handleRename}
          />
        )}

        <AutoSaveIndicator status={saveStatus} />

        {currentNodeLabel && (
          <span
            className="flex items-center gap-1.5 text-xs"
            style={{
              color: "var(--accent-blue)",
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
              maxWidth: "20ch",
            }}
            title={`Running: ${currentNodeLabel}`}
          >
            <span
              className="status-dot-pulse"
              style={{
                display: "inline-block",
                width: 6,
                height: 6,
                borderRadius: "50%",
                background: "var(--accent-blue)",
                flexShrink: 0,
              }}
            />
            Running… {currentNodeLabel}
          </span>
        )}
      </div>

      {/* Spacer */}
      <div className="flex-1" />

      {/* Right section: run button + toolbar */}
      <div className="flex items-center gap-1.5">
        <RunButton pipelineId={pipelineId} nodeIds={nodeIds} />

        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              variant="ghost"
              size="icon"
              className="h-7 w-7 text-[var(--text-secondary)]"
              onClick={() => setHistoryOpen(true)}
            >
              <Clock className="h-4 w-4" />
            </Button>
          </TooltipTrigger>
          <TooltipContent>Run history</TooltipContent>
        </Tooltip>

        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              variant="ghost"
              size="icon"
              className="h-7 w-7 text-[var(--text-secondary)]"
              onClick={() => setSettingsOpen(true)}
            >
              <Settings className="h-4 w-4" />
            </Button>
          </TooltipTrigger>
          <TooltipContent>Settings</TooltipContent>
        </Tooltip>
      </div>

      {/* Modals & Drawers */}
      {pipeline && (
        <SettingsModal
          open={settingsOpen}
          onClose={() => setSettingsOpen(false)}
          settings={pipeline.settings}
          onUpdate={(patch) => settingsMutation.mutate(patch)}
        />
      )}

      <HistoryDrawer
        open={historyOpen}
        onClose={() => setHistoryOpen(false)}
        runs={runs}
        onDeleteRun={(runId) => deleteRunMutation.mutate(runId)}
      />

      <NavDrawer
        open={navOpen}
        onClose={() => setNavOpen(false)}
        pipelines={pipelines}
        currentPipelineId={pipelineId}
        onSelect={(id) => navigate(`/pipeline/${id}`)}
        onCreate={() => createMutation.mutate()}
      />
    </header>
  );
}

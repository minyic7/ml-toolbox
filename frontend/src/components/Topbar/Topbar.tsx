import { useState, useCallback, useMemo } from "react";
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
  const nodeStatuses = useExecutionStore((s) => s.nodeStatuses);

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
    if (!isRunning || pendingNodeIds.length === 0) return 0;
    const done = pendingNodeIds.filter((id) => {
      const s = nodeStatuses[id];
      return s === "done" || s === "error" || s === "skipped" || s === "cached";
    }).length;
    return done / pendingNodeIds.length;
  }, [isRunning, pendingNodeIds, nodeStatuses]);

  return (
    <header
      className="relative flex items-center shrink-0 px-3 gap-3 border-b select-none"
      style={{
        height: 48,
        backgroundColor: "var(--node-bg)",
        borderColor: "var(--border-default)",
      }}
    >
      {/* Progress bar (overlays bottom edge) */}
      {isRunning && (
        <div
          className="absolute bottom-0 left-0 h-0.5 transition-all duration-300"
          style={{
            width: `${progress * 100}%`,
            backgroundColor: "var(--accent-blue)",
          }}
        />
      )}

      {/* Left section: nav + pipeline name */}
      <div className="flex items-center gap-2 min-w-0">
        <button
          type="button"
          onClick={() => setNavOpen(true)}
          className="p-1.5 rounded hover:bg-black/5 transition-colors shrink-0"
          style={{ color: "var(--text-secondary)" }}
          title="Pipelines"
        >
          <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
            <path d="M2 3.5a.5.5 0 01.5-.5h11a.5.5 0 010 1h-11a.5.5 0 01-.5-.5zm0 4a.5.5 0 01.5-.5h11a.5.5 0 010 1h-11a.5.5 0 01-.5-.5zm0 4a.5.5 0 01.5-.5h11a.5.5 0 010 1h-11a.5.5 0 01-.5-.5z" />
          </svg>
        </button>

        {pipeline && (
          <PipelineNameInput
            name={pipeline.name}
            onRename={handleRename}
          />
        )}

        <AutoSaveIndicator status={saveStatus} />
      </div>

      {/* Spacer */}
      <div className="flex-1" />

      {/* Right section: run button + toolbar */}
      <div className="flex items-center gap-1.5">
        <RunButton pipelineId={pipelineId} nodeIds={nodeIds} />

        <button
          type="button"
          onClick={() => setHistoryOpen(true)}
          className="p-1.5 rounded hover:bg-black/5 transition-colors"
          style={{ color: "var(--text-secondary)" }}
          title="Run history"
        >
          <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
            <path d="M8 3.5a.5.5 0 01.5.5v3.793l2.354 2.353a.5.5 0 01-.708.708l-2.5-2.5A.5.5 0 017.5 8V4a.5.5 0 01.5-.5z" />
            <path d="M8 1a7 7 0 100 14A7 7 0 008 1zM2 8a6 6 0 1112 0A6 6 0 012 8z" />
          </svg>
        </button>

        <button
          type="button"
          onClick={() => setSettingsOpen(true)}
          className="p-1.5 rounded hover:bg-black/5 transition-colors"
          style={{ color: "var(--text-secondary)" }}
          title="Settings"
        >
          <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
            <path d="M8 4.754a3.246 3.246 0 100 6.492 3.246 3.246 0 000-6.492zM5.754 8a2.246 2.246 0 114.492 0 2.246 2.246 0 01-4.492 0z" />
            <path d="M9.796 1.343c-.527-1.79-3.065-1.79-3.592 0l-.094.319a.873.873 0 01-1.255.52l-.292-.16c-1.64-.892-3.433.902-2.54 2.541l.159.292a.873.873 0 01-.52 1.255l-.319.094c-1.79.527-1.79 3.065 0 3.592l.319.094a.873.873 0 01.52 1.255l-.16.292c-.892 1.64.902 3.434 2.541 2.54l.292-.159a.873.873 0 011.255.52l.094.319c.527 1.79 3.065 1.79 3.592 0l.094-.319a.873.873 0 011.255-.52l.292.16c1.64.892 3.434-.902 2.54-2.541l-.159-.292a.873.873 0 01.52-1.255l.319-.094c1.79-.527 1.79-3.065 0-3.592l-.319-.094a.873.873 0 01-.52-1.255l.16-.292c.892-1.64-.902-3.433-2.541-2.54l-.292.159a.873.873 0 01-1.255-.52l-.094-.319zm-2.633.283c.246-.835 1.428-.835 1.674 0l.094.319a1.873 1.873 0 002.693 1.115l.291-.16c.764-.415 1.6.42 1.184 1.185l-.159.292a1.873 1.873 0 001.116 2.692l.318.094c.835.246.835 1.428 0 1.674l-.319.094a1.873 1.873 0 00-1.115 2.693l.16.291c.415.764-.42 1.6-1.185 1.184l-.292-.159a1.873 1.873 0 00-2.692 1.116l-.094.318c-.246.835-1.428.835-1.674 0l-.094-.319a1.873 1.873 0 00-2.693-1.115l-.291.16c-.764.415-1.6-.42-1.184-1.185l.159-.292A1.873 1.873 0 002.98 9.796l-.318-.094c-.835-.246-.835-1.428 0-1.674l.319-.094A1.873 1.873 0 004.096 5.24l-.16-.291c-.415-.764.42-1.6 1.185-1.184l.292.159A1.873 1.873 0 008.1 2.806l.094-.318z" />
          </svg>
        </button>
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

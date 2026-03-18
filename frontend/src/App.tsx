import { useState } from "react";
import { Canvas } from "@/components/Canvas";
import { Sidebar } from "@/components/Panel/Sidebar";
import { RightPanel } from "@/components/Panel/RightPanel";

export default function App() {
  const [rightPanelOpen, setRightPanelOpen] = useState(false);

  return (
    <div className="flex h-screen w-screen overflow-hidden bg-background text-foreground">
      {/* Sidebar - left */}
      <div className="w-[250px] shrink-0 border-r border-border">
        <Sidebar />
      </div>

      {/* Canvas - center */}
      <div className="flex-1">
        <Canvas onNodeSelect={() => setRightPanelOpen(true)} />
      </div>

      {/* Right panel - conditionally shown */}
      {rightPanelOpen && (
        <div className="w-[350px] shrink-0 border-l border-border">
          <RightPanel onClose={() => setRightPanelOpen(false)} />
        </div>
      )}
    </div>
  );
}

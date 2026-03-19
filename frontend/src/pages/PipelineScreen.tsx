import { useParams } from "react-router-dom";
import Topbar from "../components/Topbar/Topbar";

export default function PipelineScreen() {
  const { id } = useParams<{ id: string }>();

  if (!id) return null;

  return (
    <div className="flex flex-col h-screen">
      <Topbar pipelineId={id} />
      <main
        className="flex-1 overflow-hidden"
        style={{ backgroundColor: "var(--canvas-bg)" }}
      >
        {/* Canvas will render here */}
      </main>
    </div>
  );
}

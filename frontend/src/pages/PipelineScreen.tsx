import { useParams } from "react-router-dom";

export default function PipelineScreen() {
  const { id } = useParams<{ id: string }>();

  return (
    <div>
      <h1>Pipeline: {id}</h1>
      <p>Canvas will render here.</p>
    </div>
  );
}

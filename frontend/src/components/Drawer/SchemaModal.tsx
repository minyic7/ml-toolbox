import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import SchemaEditor from "./SchemaEditor";

interface SchemaModalProps {
  open: boolean;
  onClose: () => void;
  pipelineId: string;
  nodeId: string;
}

export default function SchemaModal({
  open,
  onClose,
  pipelineId,
  nodeId,
}: SchemaModalProps) {
  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent
        className="max-w-[80vw] max-h-[80vh] overflow-auto"
      >
        <DialogHeader>
          <DialogTitle>Column Schema</DialogTitle>
        </DialogHeader>
        <SchemaEditor pipelineId={pipelineId} nodeId={nodeId} />
      </DialogContent>
    </Dialog>
  );
}

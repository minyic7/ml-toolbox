import { NodeLibrarySidebar } from "@/components/Canvas/Sidebar";
import type { NodeDefinition } from "@/lib/types";

interface SidebarProps {
  definitions: NodeDefinition[];
}

export function Sidebar({ definitions }: SidebarProps) {
  return <NodeLibrarySidebar definitions={definitions} />;
}

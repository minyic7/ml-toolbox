import { BrowserRouter, Routes, Route } from "react-router-dom";
import { TooltipProvider } from "@/components/ui/tooltip";
import { Toaster } from "@/components/ui/sonner";
import HomeScreen from "./pages/HomeScreen";
import PipelineScreen from "./pages/PipelineScreen";

export default function App() {
  return (
    <BrowserRouter basename="/ml-toolbox">
      <TooltipProvider delayDuration={300}>
        <Routes>
          <Route path="/" element={<HomeScreen />} />
          <Route path="/pipeline/:id" element={<PipelineScreen />} />
        </Routes>
        <Toaster position="bottom-center" />
      </TooltipProvider>
    </BrowserRouter>
  );
}

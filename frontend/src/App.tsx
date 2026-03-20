import { BrowserRouter, Routes, Route } from "react-router-dom";
import { TooltipProvider } from "@/components/ui/tooltip";
import { Toaster } from "@/components/ui/sonner";
import ErrorBoundary from "@/components/ErrorBoundary";
import HomeScreen from "./pages/HomeScreen";
import PipelineScreen from "./pages/PipelineScreen";

export default function App() {
  return (
    <BrowserRouter basename="/ml-toolbox">
      <TooltipProvider delayDuration={300}>
        <Routes>
          <Route
            path="/"
            element={
              <ErrorBoundary>
                <HomeScreen />
              </ErrorBoundary>
            }
          />
          <Route
            path="/pipeline/:id"
            element={
              <ErrorBoundary>
                <PipelineScreen />
              </ErrorBoundary>
            }
          />
        </Routes>
        <Toaster position="bottom-center" />
      </TooltipProvider>
    </BrowserRouter>
  );
}

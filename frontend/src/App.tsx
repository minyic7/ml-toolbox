import { BrowserRouter, Routes, Route } from "react-router-dom";
import HomeScreen from "./pages/HomeScreen";
import PipelineScreen from "./pages/PipelineScreen";

export default function App() {
  return (
    <BrowserRouter basename="/ml-toolbox">
      <Routes>
        <Route path="/" element={<HomeScreen />} />
        <Route path="/pipeline/:id" element={<PipelineScreen />} />
      </Routes>
    </BrowserRouter>
  );
}

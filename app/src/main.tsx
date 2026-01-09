import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import "./index.css";
import { initializeStorage } from "./lib/storage";
import { logger } from "./lib/logger";


initializeStorage();


window.addEventListener('unhandledrejection', (event) => {
  logger.error('Unhandled promise rejection', event.reason);
  event.preventDefault(); 
});

window.addEventListener('error', (event) => {
  logger.error('Uncaught error', event.error || new Error(event.message));
});

createRoot(document.getElementById("root")!).render(<App />);

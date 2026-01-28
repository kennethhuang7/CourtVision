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

const originalWarn = console.warn;
console.warn = (...args) => {
  const message = args[0]?.toString() || '';
  if (message.includes('You are trying to animate opacity from "undefined"')) {
    return;
  }
  originalWarn.apply(console, args);
};

createRoot(document.getElementById("root")!).render(<App />);

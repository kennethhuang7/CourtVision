import { Toaster } from "@/components/ui/toaster";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { AuthProvider } from "@/contexts/AuthContext";
import { ThemeProvider } from "@/contexts/ThemeContext";
import { CacheProvider } from "@/contexts/CacheContext";
import { EnsembleProvider } from "@/contexts/EnsembleContext";
import { ChatWindowProvider } from "@/contexts/ChatWindowContext";
import { NotificationProvider } from "@/contexts/NotificationContext";
import { DoNotDisturbProvider } from "@/contexts/DoNotDisturbContext";
import { DashboardLayout } from "@/components/layout/DashboardLayout";
import { ErrorBoundary } from "@/components/ErrorBoundary";
import Index from "./pages/Index";
import Login from "./pages/Login";
import Register from "./pages/Register";
import VerifyEmail from "./pages/VerifyEmail";
import Predictions from "./pages/dashboard/Predictions";
import PlayerAnalysis from "./pages/dashboard/PlayerAnalysis";
import PickFinder from "./pages/dashboard/PickFinder";
import Trends from "./pages/dashboard/Trends";
import SavedPicks from "./pages/dashboard/SavedPicks";
import Community from "./pages/dashboard/Community";
import Messages from "./pages/dashboard/Messages";
import Friends from "./pages/dashboard/Friends";
import MyGroups from "./pages/dashboard/MyGroups";
import ModelPerformance from "./pages/dashboard/ModelPerformance";
import Analytics from "./pages/dashboard/Analytics";
import Settings from "./pages/dashboard/Settings";
import Notifications from "./pages/dashboard/Notifications";
import NotFound from "./pages/NotFound";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      staleTime: 30000,
    },
  },
});

const App = () => (
  <ErrorBoundary>
    <QueryClientProvider client={queryClient}>
      <AuthProvider>
        <ThemeProvider>
          <CacheProvider>
            <DoNotDisturbProvider>
              <NotificationProvider>
                <ChatWindowProvider>
                  <EnsembleProvider>
                    <TooltipProvider>
                      <Toaster />
                      <Sonner />
                      <BrowserRouter>
                <Routes>
                  <Route path="/" element={<Index />} />
                  <Route path="/login" element={
                    <ErrorBoundary>
                      <Login />
                    </ErrorBoundary>
                  } />
                  <Route path="/register" element={
                    <ErrorBoundary>
                      <Register />
                    </ErrorBoundary>
                  } />
                  <Route path="/verify-email" element={
                    <ErrorBoundary>
                      <VerifyEmail />
                    </ErrorBoundary>
                  } />
                  <Route path="/dashboard" element={
                    <ErrorBoundary>
                      <DashboardLayout />
                    </ErrorBoundary>
                  }>
                    <Route index element={
                      <ErrorBoundary>
                        <Predictions />
                      </ErrorBoundary>
                    } />
                    <Route path="player-analysis" element={
                      <ErrorBoundary>
                        <PlayerAnalysis />
                      </ErrorBoundary>
                    } />
                    <Route path="pick-finder" element={
                      <ErrorBoundary>
                        <PickFinder />
                      </ErrorBoundary>
                    } />
                    <Route path="trends" element={
                      <ErrorBoundary>
                        <Trends />
                      </ErrorBoundary>
                    } />
                    <Route path="saved-picks" element={
                      <ErrorBoundary>
                        <SavedPicks />
                      </ErrorBoundary>
                    } />
                    <Route path="community" element={
                      <ErrorBoundary>
                        <Community />
                      </ErrorBoundary>
                    } />
                    <Route path="messages" element={
                      <ErrorBoundary>
                        <Messages />
                      </ErrorBoundary>
                    } />
                    <Route path="friends" element={
                      <ErrorBoundary>
                        <Friends />
                      </ErrorBoundary>
                    } />
                    <Route path="groups" element={
                      <ErrorBoundary>
                        <MyGroups />
                      </ErrorBoundary>
                    } />
                    <Route path="model-performance" element={
                      <ErrorBoundary>
                        <ModelPerformance />
                      </ErrorBoundary>
                    } />
                    <Route path="analytics" element={
                      <ErrorBoundary>
                        <Analytics />
                      </ErrorBoundary>
                    } />
                    <Route path="notifications" element={
                      <ErrorBoundary>
                        <Notifications />
                      </ErrorBoundary>
                    } />
                  </Route>
                  <Route path="*" element={<NotFound />} />
                </Routes>
                  </BrowserRouter>
                </TooltipProvider>
                  </EnsembleProvider>
                </ChatWindowProvider>
              </NotificationProvider>
            </DoNotDisturbProvider>
        </CacheProvider>
      </ThemeProvider>
      </AuthProvider>
    </QueryClientProvider>
  </ErrorBoundary>
);

export default App;

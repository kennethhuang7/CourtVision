import { useState } from 'react';
import { Link, Navigate } from 'react-router-dom';
import { useAuth } from '@/contexts/AuthContext';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Checkbox } from '@/components/ui/checkbox';
import { Eye, EyeOff, ArrowRight } from 'lucide-react';
import { toast } from 'sonner';
import { AuthTitleBar } from '@/components/layout/AuthTitleBar';
import { SupabaseConnectionStatus } from '@/components/ui/supabase-connection-status';

export default function Login() {
  const { login, isAuthenticated } = useAuth();
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [rememberMe, setRememberMe] = useState(false);
  const [error, setError] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);

  if (isAuthenticated) {
    return <Navigate to="/dashboard" replace />;
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');

    if (!email || !password) {
      setError('Please fill in all fields');
      return;
    }

    try {
      setIsSubmitting(true);
      await login(email, password, rememberMe);
      toast.success('Welcome back!');
    } catch (err: any) {
      
      const errorMessage = err?.message || String(err);
      if (
        errorMessage.includes('timeout') ||
        errorMessage.includes('522') ||
        errorMessage.includes('Failed to fetch') ||
        errorMessage.includes('NetworkError') ||
        errorMessage.includes('Unexpected token')
      ) {
        setError('Unable to connect to the server. Please check your connection and try again.');
      } else {
        setError('Invalid credentials. Please try again.');
      }
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="min-h-screen flex bg-background">
      <AuthTitleBar />
      <div className="flex-1 flex items-center justify-center p-8">
        <div className="w-full max-w-md space-y-8 animate-slide-up">
          <div className="flex items-center gap-3">
            <div className="flex h-12 w-12 items-center justify-center rounded-xl overflow-hidden shrink-0">
              <img src="/courtvision.png" alt="CourtVision" className="h-full w-full object-contain" />
            </div>
            <div className="min-w-0 flex-1">
              <h1 className="text-2xl font-bold text-foreground leading-tight truncate">CourtVision</h1>
              <p className="text-sm text-muted-foreground leading-tight truncate">AI Performance Analytics</p>
            </div>
          </div>

          <div className="min-w-0">
            <h2 className="text-3xl font-bold text-foreground leading-tight truncate">Welcome back</h2>
            <p className="mt-2 text-muted-foreground leading-tight truncate">
              Sign in to access your predictions dashboard
            </p>
          </div>

          <SupabaseConnectionStatus />

          {error && (
            <div className="rounded-lg bg-destructive/10 border border-destructive/20 p-4 text-sm text-destructive animate-scale-in">
              {error}
            </div>
          )}

          <form onSubmit={handleSubmit} className="space-y-6">
            <div className="space-y-2">
              <Label htmlFor="email">Email</Label>
              <Input
                id="email"
                type="email"
                placeholder="name@example.com"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                autoComplete="email"
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="password">Password</Label>
              <div className="relative">
                <Input
                  id="password"
                  type={showPassword ? 'text' : 'password'}
                  placeholder="Enter your password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  autoComplete="current-password"
                />
                <button
                  type="button"
                  onClick={() => setShowPassword(!showPassword)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground transition-colors shrink-0"
                >
                  {showPassword ? <EyeOff className="h-4 w-4 shrink-0" /> : <Eye className="h-4 w-4 shrink-0" />}
                </button>
              </div>
            </div>

            <div className="flex items-center gap-2">
              <Checkbox
                id="remember"
                checked={rememberMe}
                onCheckedChange={(checked) => setRememberMe(checked as boolean)}
              />
              <Label htmlFor="remember" className="text-sm text-muted-foreground cursor-pointer">
                Remember me
              </Label>
            </div>

            <Button type="submit" variant="hero" size="lg" className="w-full" disabled={isSubmitting}>
              {isSubmitting ? (
                <div className="h-5 w-5 animate-spin rounded-full border-2 border-current border-t-transparent shrink-0" />
              ) : (
                <>
                  <span className="whitespace-nowrap">Sign In</span>
                  <ArrowRight className="h-4 w-4 shrink-0" />
                </>
              )}
            </Button>
          </form>

          <p className="text-center text-sm text-muted-foreground">
            Don't have an account?{' '}
            <Link to="/register" className="font-medium text-primary hover:underline">
              Create one
            </Link>
          </p>
        </div>
      </div>

      
      <div className="hidden lg:flex lg:flex-1 items-center justify-center bg-gradient-hero p-12">
        <div className="max-w-lg text-center space-y-6 animate-fade-in">
          <div className="mx-auto flex h-24 w-24 items-center justify-center rounded-3xl overflow-hidden shrink-0">
            <img src="/courtvision.png" alt="CourtVision" className="h-full w-full object-contain" />
          </div>
          <h2 className="text-4xl font-bold text-foreground leading-tight">
            AI-Powered NBA Predictions
          </h2>
          <p className="text-lg text-muted-foreground leading-tight">
            Get accurate player performance predictions backed by machine learning models trained on years of NBA data.
          </p>
          <div className="flex justify-center gap-8 pt-4">
            <div>
              <p className="text-3xl font-bold text-primary leading-tight whitespace-nowrap">94%</p>
              <p className="text-sm text-muted-foreground leading-tight whitespace-nowrap">Accuracy Rate</p>
            </div>
            <div>
              <p className="text-3xl font-bold text-accent leading-tight whitespace-nowrap">15K+</p>
              <p className="text-sm text-muted-foreground leading-tight whitespace-nowrap">Predictions</p>
            </div>
            <div>
              <p className="text-3xl font-bold text-success leading-tight whitespace-nowrap">4</p>
              <p className="text-sm text-muted-foreground leading-tight whitespace-nowrap">ML Models</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

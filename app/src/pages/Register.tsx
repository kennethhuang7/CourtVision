import { useState } from 'react';
import { Link, Navigate, useNavigate } from 'react-router-dom';
import { useAuth } from '@/contexts/AuthContext';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Eye, EyeOff, ArrowRight, Check, X } from 'lucide-react';
import { toast } from 'sonner';
import { cn } from '@/lib/utils';
import { AuthTitleBar } from '@/components/layout/AuthTitleBar';

export default function Register() {
  const { register, isAuthenticated } = useAuth();
  const navigate = useNavigate();
  const [email, setEmail] = useState('');
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [error, setError] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);

  if (isAuthenticated) {
    return <Navigate to="/dashboard" replace />;
  }

  const passwordStrength = {
    length: password.length >= 8,
    uppercase: /[A-Z]/.test(password),
    lowercase: /[a-z]/.test(password),
    number: /[0-9]/.test(password),
    special: /[!@#$%^&*(),.?":{}|<>]/.test(password),
  };

  const isStrongPassword = Object.values(passwordStrength).every(Boolean);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');

    if (!email || !username || !password || !confirmPassword) {
      setError('Please fill in all fields');
      return;
    }

    if (password !== confirmPassword) {
      setError('Passwords do not match');
      return;
    }

    if (!isStrongPassword) {
      setError('Please create a stronger password');
      return;
    }

    try {
      setIsSubmitting(true);
      await register(email, username, password);
      toast.success('Account created. Please verify your email before signing in.');
      navigate('/verify-email', { replace: true, state: { email } });
    } catch (err) {
      setError('Registration failed. Please try again.');
    } finally {
      setIsSubmitting(false);
    }
  };

  const PasswordCheck = ({ met, label }: { met: boolean; label: string }) => (
    <div className="flex items-center gap-2 text-sm">
      {met ? (
        <Check className="h-4 w-4 text-success shrink-0" />
      ) : (
        <X className="h-4 w-4 text-muted-foreground shrink-0" />
      )}
      <span className={cn(met ? 'text-success' : 'text-muted-foreground', 'leading-tight')}>{label}</span>
    </div>
  );

  return (
    <div className="min-h-screen flex bg-background">
      <AuthTitleBar />
      
      <div className="hidden lg:flex lg:flex-1 items-center justify-center bg-gradient-hero p-12">
        <div className="max-w-lg text-center space-y-6 animate-fade-in">
          <div className="mx-auto flex h-24 w-24 items-center justify-center rounded-3xl overflow-hidden shrink-0">
            <img src="/courtvision.png" alt="CourtVision" className="h-full w-full object-contain" />
          </div>
          <h2 className="text-4xl font-bold text-foreground leading-tight">
            Join the Future of Sports Analytics
          </h2>
          <p className="text-lg text-muted-foreground leading-tight">
            Create your account and start making data-driven decisions with our advanced AI prediction system.
          </p>
        </div>
      </div>

      
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
            <h2 className="text-3xl font-bold text-foreground leading-tight truncate">Create account</h2>
            <p className="mt-2 text-muted-foreground leading-tight truncate">
              Get started with your free account today
            </p>
          </div>

          
          {error && (
            <div className="rounded-lg bg-destructive/10 border border-destructive/20 p-4 text-sm text-destructive animate-scale-in">
              {error}
            </div>
          )}

          
          <form onSubmit={handleSubmit} className="space-y-5">
            <div className="space-y-2">
              <Label htmlFor="email">Email</Label>
              <Input
                id="email"
                type="email"
                placeholder="name@example.com"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="username">Username</Label>
              <Input
                id="username"
                type="text"
                placeholder="Choose a username"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="password">Password</Label>
              <div className="relative">
                <Input
                  id="password"
                  type={showPassword ? 'text' : 'password'}
                  placeholder="Create a password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                />
                <button
                  type="button"
                  onClick={() => setShowPassword(!showPassword)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground transition-colors shrink-0"
                >
                  {showPassword ? <EyeOff className="h-4 w-4 shrink-0" /> : <Eye className="h-4 w-4 shrink-0" />}
                </button>
              </div>
              {password && (
                <div className="mt-2 space-y-1 animate-fade-in">
                  <PasswordCheck met={passwordStrength.length} label="At least 8 characters" />
                  <PasswordCheck met={passwordStrength.uppercase} label="One uppercase letter" />
                  <PasswordCheck met={passwordStrength.lowercase} label="One lowercase letter" />
                  <PasswordCheck met={passwordStrength.number} label="One number" />
                  <PasswordCheck met={passwordStrength.special} label="One special character (!@#$%^&*)" />
                </div>
              )}
            </div>

            <div className="space-y-2">
              <Label htmlFor="confirmPassword">Confirm Password</Label>
              <Input
                id="confirmPassword"
                type="password"
                placeholder="Confirm your password"
                value={confirmPassword}
                onChange={(e) => setConfirmPassword(e.target.value)}
              />
            </div>

            <Button type="submit" variant="hero" size="lg" className="w-full" disabled={isSubmitting}>
              {isSubmitting ? (
                <div className="h-5 w-5 animate-spin rounded-full border-2 border-current border-t-transparent shrink-0" />
              ) : (
                <>
                  <span className="whitespace-nowrap">Create Account</span>
                  <ArrowRight className="h-4 w-4 shrink-0" />
                </>
              )}
            </Button>
          </form>

          
          <p className="text-center text-sm text-muted-foreground">
            Already have an account?{' '}
            <Link to="/login" className="font-medium text-primary hover:underline">
              Sign in
            </Link>
          </p>
        </div>
      </div>
    </div>
  );
}

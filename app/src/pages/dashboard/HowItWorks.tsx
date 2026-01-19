import { useState } from 'react';
import {
  Brain,
  Layers,
  Target,
  TrendingUp,
  Database,
  Cpu,
  BarChart3,
  Zap,
  Shield,
  ChevronDown,
  ChevronUp,
  Sparkles,
  Activity,
  Users,
  Clock,
  MapPin,
  Calendar,
  GitBranch,
  ExternalLink
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { Button } from '@/components/ui/button';

interface AccordionItemProps {
  title: string;
  icon: React.ReactNode;
  children: React.ReactNode;
  defaultOpen?: boolean;
}

function AccordionItem({ title, icon, children, defaultOpen = false }: AccordionItemProps) {
  const [isOpen, setIsOpen] = useState(defaultOpen);

  return (
    <div className="rounded-xl border border-border/60 bg-card/50 overflow-hidden transition-all duration-200 hover:border-border">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="w-full flex items-center justify-between p-4 text-left hover:bg-accent/5 transition-colors"
      >
        <div className="flex items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-primary/10 text-primary">
            {icon}
          </div>
          <span className="font-semibold text-foreground">{title}</span>
        </div>
        {isOpen ? (
          <ChevronUp className="h-5 w-5 text-muted-foreground" />
        ) : (
          <ChevronDown className="h-5 w-5 text-muted-foreground" />
        )}
      </button>
      {isOpen && (
        <div className="px-4 pb-4 animate-in fade-in slide-in-from-top-2 duration-200">
          <div className="pt-2 border-t border-border/40">
            {children}
          </div>
        </div>
      )}
    </div>
  );
}

interface FeatureCardProps {
  icon: React.ReactNode;
  title: string;
  description: string;
  delay?: number;
}

function FeatureCard({ icon, title, description, delay = 0 }: FeatureCardProps) {
  return (
    <div
      className="rounded-xl border border-border/60 bg-card/50 p-5 hover:border-border hover:bg-accent/5 transition-all duration-200 hover:-translate-y-0.5 animate-in fade-in slide-in-from-bottom-2"
      style={{ animationDelay: `${delay}ms` }}
    >
      <div className="flex h-11 w-11 items-center justify-center rounded-lg bg-primary/10 text-primary mb-3">
        {icon}
      </div>
      <h3 className="font-semibold text-foreground mb-1">{title}</h3>
      <p className="text-sm text-muted-foreground">{description}</p>
    </div>
  );
}

interface ModelCardProps {
  name: string;
  description: string;
  strengths: string[];
  color: string;
  delay?: number;
}

function ModelCard({ name, description, strengths, color, delay = 0 }: ModelCardProps) {
  return (
    <div
      className="rounded-xl border border-border/60 bg-card/50 p-5 hover:border-border transition-all duration-200 animate-in fade-in slide-in-from-bottom-2"
      style={{ animationDelay: `${delay}ms` }}
    >
      <div className="flex items-center gap-3 mb-3">
        <div
          className="h-3 w-3 rounded-full"
          style={{ backgroundColor: color }}
        />
        <h3 className="font-semibold text-foreground">{name}</h3>
      </div>
      <p className="text-sm text-muted-foreground mb-3">{description}</p>
      <div className="space-y-1">
        {strengths.map((strength, i) => (
          <div key={i} className="flex items-center gap-2 text-xs text-muted-foreground">
            <Zap className="h-3 w-3 text-primary" />
            <span>{strength}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

export default function HowItWorks() {
  return (
    <div className="space-y-8 animate-fade-in p-6">
      {/* Hero Section */}
      <div className="text-center space-y-4 pb-4">
        <div className="flex justify-center">
          <div className="flex h-16 w-16 items-center justify-center rounded-2xl bg-gradient-to-br from-primary/20 via-primary/10 to-accent/10 text-primary animate-in zoom-in duration-300">
            <Brain className="h-8 w-8" />
          </div>
        </div>
        <div className="space-y-2">
          <h1 className="text-3xl font-bold text-foreground">How CourtVision Works</h1>
          <p className="text-muted-foreground max-w-2xl mx-auto">
            An ensemble-based machine learning system that forecasts NBA player statistics
            using 150+ engineered features and four state-of-the-art algorithms.
          </p>
        </div>
        <div className="flex justify-center gap-2 pt-2">
          <a
            href="https://github.com/yourusername/NBA-Player-Performance-Prediction/blob/main/docs/TECHNICAL_OVERVIEW.md"
            target="_blank"
            rel="noopener noreferrer"
          >
            <Button variant="outline" size="sm" className="gap-2">
              <ExternalLink className="h-4 w-4" />
              Full Technical Documentation
            </Button>
          </a>
        </div>
      </div>

      {/* Quick Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="rounded-xl border border-border/60 bg-card/50 p-4 text-center animate-in fade-in slide-in-from-bottom-2 duration-300">
          <div className="text-3xl font-bold text-primary">4</div>
          <div className="text-sm text-muted-foreground">ML Models</div>
        </div>
        <div className="rounded-xl border border-border/60 bg-card/50 p-4 text-center animate-in fade-in slide-in-from-bottom-2 duration-300" style={{ animationDelay: '50ms' }}>
          <div className="text-3xl font-bold text-primary">150+</div>
          <div className="text-sm text-muted-foreground">Features</div>
        </div>
        <div className="rounded-xl border border-border/60 bg-card/50 p-4 text-center animate-in fade-in slide-in-from-bottom-2 duration-300" style={{ animationDelay: '100ms' }}>
          <div className="text-3xl font-bold text-primary">7</div>
          <div className="text-sm text-muted-foreground">Stat Types</div>
        </div>
        <div className="rounded-xl border border-border/60 bg-card/50 p-4 text-center animate-in fade-in slide-in-from-bottom-2 duration-300" style={{ animationDelay: '150ms' }}>
          <div className="text-3xl font-bold text-primary">100%</div>
          <div className="text-sm text-muted-foreground">Leakage-Free</div>
        </div>
      </div>

      {/* The Ensemble */}
      <div className="space-y-4">
        <div className="flex items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-primary/10 text-primary">
            <Layers className="h-5 w-5" />
          </div>
          <div>
            <h2 className="text-xl font-bold text-foreground">The Ensemble</h2>
            <p className="text-sm text-muted-foreground">Four algorithms working together for robust predictions</p>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          <ModelCard
            name="XGBoost"
            description="Gradient boosting with strong regularization"
            strengths={['Handles overfitting well', 'Fast training', 'Feature importance']}
            color="hsl(var(--primary))"
            delay={0}
          />
          <ModelCard
            name="LightGBM"
            description="Histogram-based gradient boosting"
            strengths={['Faster training', 'Lower memory', 'Complex patterns']}
            color="hsl(142, 76%, 36%)"
            delay={50}
          />
          <ModelCard
            name="CatBoost"
            description="Optimized for categorical features"
            strengths={['Handles categories', 'Less tuning needed', 'Robust defaults']}
            color="hsl(38, 92%, 50%)"
            delay={100}
          />
          <ModelCard
            name="Random Forest"
            description="Bagged ensemble of decision trees"
            strengths={['Outlier resistant', 'Interpretable', 'Stable predictions']}
            color="hsl(280, 67%, 55%)"
            delay={150}
          />
        </div>

        <div className="rounded-xl border border-primary/20 bg-primary/5 p-4">
          <p className="text-sm text-muted-foreground">
            <strong className="text-foreground">Why ensemble?</strong> Different models capture different patterns.
            By averaging their predictions, individual errors tend to cancel out, resulting in more reliable forecasts.
          </p>
        </div>
      </div>

      {/* Feature Engineering */}
      <div className="space-y-4">
        <div className="flex items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-primary/10 text-primary">
            <Database className="h-5 w-5" />
          </div>
          <div>
            <h2 className="text-xl font-bold text-foreground">Feature Engineering</h2>
            <p className="text-sm text-muted-foreground">150+ carefully crafted predictive features</p>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          <FeatureCard
            icon={<TrendingUp className="h-5 w-5" />}
            title="Rolling Averages"
            description="Performance over last 5, 10, and 20 games with exponential weighting for recent games"
            delay={0}
          />
          <FeatureCard
            icon={<Activity className="h-5 w-5" />}
            title="Per-36 Rates"
            description="Stats normalized to per-36-minute rates for fair comparison across different roles"
            delay={50}
          />
          <FeatureCard
            icon={<Shield className="h-5 w-5" />}
            title="Opponent Defense"
            description="Team and position-specific defensive stats including points allowed and pace"
            delay={100}
          />
          <FeatureCard
            icon={<Users className="h-5 w-5" />}
            title="Teammate Impact"
            description="Tracks when star teammates are out and historical performance adjustments"
            delay={150}
          />
          <FeatureCard
            icon={<Clock className="h-5 w-5" />}
            title="Rest & Schedule"
            description="Days rest, back-to-backs, schedule density, and fatigue indicators"
            delay={200}
          />
          <FeatureCard
            icon={<MapPin className="h-5 w-5" />}
            title="Travel & Context"
            description="Timezone changes, altitude effects (Denver/Utah), home/away splits"
            delay={250}
          />
        </div>
      </div>

      {/* Detailed Sections */}
      <div className="space-y-3">
        <h2 className="text-xl font-bold text-foreground flex items-center gap-2">
          <Sparkles className="h-5 w-5 text-primary" />
          Deep Dive
        </h2>

        <div className="space-y-3">
          <AccordionItem
            title="Predicted Statistics"
            icon={<BarChart3 className="h-5 w-5" />}
            defaultOpen
          >
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3 pt-3">
              {['Points', 'Rebounds', 'Assists', 'Steals', 'Blocks', 'Turnovers', '3-Pointers'].map((stat, i) => (
                <div
                  key={stat}
                  className="rounded-lg bg-secondary/50 px-3 py-2 text-center text-sm font-medium text-foreground"
                >
                  {stat}
                </div>
              ))}
            </div>
            <p className="text-sm text-muted-foreground mt-3">
              Each statistic has its own dedicated model trained with appropriate loss functions.
              Steals and blocks use Poisson regression for rare-event handling.
            </p>
          </AccordionItem>

          <AccordionItem
            title="Confidence Scoring"
            icon={<Target className="h-5 w-5" />}
          >
            <div className="space-y-3 pt-3">
              <p className="text-sm text-muted-foreground">
                Confidence scores (0-100) measure prediction quality based on multiple factors:
              </p>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                <div className="rounded-lg border border-border/40 p-3">
                  <div className="font-medium text-foreground text-sm mb-1">Ensemble Agreement</div>
                  <p className="text-xs text-muted-foreground">How well the 4 models agree on the prediction</p>
                </div>
                <div className="rounded-lg border border-border/40 p-3">
                  <div className="font-medium text-foreground text-sm mb-1">Player Consistency</div>
                  <p className="text-xs text-muted-foreground">Historical variance in the player's stats</p>
                </div>
                <div className="rounded-lg border border-border/40 p-3">
                  <div className="font-medium text-foreground text-sm mb-1">Data Completeness</div>
                  <p className="text-xs text-muted-foreground">Availability of features for prediction</p>
                </div>
                <div className="rounded-lg border border-border/40 p-3">
                  <div className="font-medium text-foreground text-sm mb-1">Experience & Context</div>
                  <p className="text-xs text-muted-foreground">Games played, recent trades, injuries</p>
                </div>
              </div>
            </div>
          </AccordionItem>

          <AccordionItem
            title="Data Leakage Prevention"
            icon={<Shield className="h-5 w-5" />}
          >
            <div className="space-y-3 pt-3">
              <p className="text-sm text-muted-foreground">
                All features use only data available <strong className="text-foreground">before</strong> each game to prevent cheating:
              </p>
              <ul className="space-y-2 text-sm text-muted-foreground">
                <li className="flex items-start gap-2">
                  <div className="h-1.5 w-1.5 rounded-full bg-primary mt-1.5 flex-shrink-0" />
                  <span>Rolling averages exclude the current game (shift by 1)</span>
                </li>
                <li className="flex items-start gap-2">
                  <div className="h-1.5 w-1.5 rounded-full bg-primary mt-1.5 flex-shrink-0" />
                  <span>Team/opponent stats calculated as-of the target date</span>
                </li>
                <li className="flex items-start gap-2">
                  <div className="h-1.5 w-1.5 rounded-full bg-primary mt-1.5 flex-shrink-0" />
                  <span>Temporal cross-validation respects season boundaries</span>
                </li>
                <li className="flex items-start gap-2">
                  <div className="h-1.5 w-1.5 rounded-full bg-primary mt-1.5 flex-shrink-0" />
                  <span>Identical feature logic for training and prediction</span>
                </li>
              </ul>
            </div>
          </AccordionItem>

          <AccordionItem
            title="Prediction Pipeline"
            icon={<GitBranch className="h-5 w-5" />}
          >
            <div className="space-y-3 pt-3">
              <div className="flex flex-col gap-2">
                {[
                  { step: '1', label: 'Load Games', desc: 'Fetch scheduled games for target date' },
                  { step: '2', label: 'Find Players', desc: 'Identify eligible players (≥5 games, not injured)' },
                  { step: '3', label: 'Build Features', desc: 'Calculate 150+ features from historical data' },
                  { step: '4', label: 'Apply Models', desc: 'Run all 4 models, scale features, generate predictions' },
                  { step: '5', label: 'Score Confidence', desc: 'Calculate per-stat and overall confidence' },
                  { step: '6', label: 'Store Results', desc: 'Save to database with explanations' },
                ].map((item, i) => (
                  <div key={i} className="flex items-center gap-3">
                    <div className="flex h-7 w-7 items-center justify-center rounded-full bg-primary/10 text-primary text-xs font-bold flex-shrink-0">
                      {item.step}
                    </div>
                    <div className="flex-1">
                      <span className="font-medium text-foreground text-sm">{item.label}</span>
                      <span className="text-muted-foreground text-sm"> — {item.desc}</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </AccordionItem>

          <AccordionItem
            title="Complete Feature Categories"
            icon={<Cpu className="h-5 w-5" />}
          >
            <div className="space-y-4 pt-3">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <h4 className="font-medium text-foreground text-sm mb-2">Player Performance</h4>
                  <ul className="space-y-1 text-xs text-muted-foreground">
                    <li>• Rolling averages (5/10/20 games)</li>
                    <li>• Exponentially weighted averages</li>
                    <li>• Per-36 minute rates</li>
                    <li>• Shooting percentages (FG/3P/FT/TS%)</li>
                    <li>• Usage rate & efficiency</li>
                    <li>• Offensive/defensive ratings</li>
                  </ul>
                </div>
                <div>
                  <h4 className="font-medium text-foreground text-sm mb-2">Game Context</h4>
                  <ul className="space-y-1 text-xs text-muted-foreground">
                    <li>• Home/away indicator</li>
                    <li>• Days rest & back-to-backs</li>
                    <li>• Schedule density</li>
                    <li>• Season progress</li>
                    <li>• Playoff indicators</li>
                    <li>• All-Star break effects</li>
                  </ul>
                </div>
                <div>
                  <h4 className="font-medium text-foreground text-sm mb-2">Opponent Factors</h4>
                  <ul className="space-y-1 text-xs text-muted-foreground">
                    <li>• Team offensive/defensive ratings</li>
                    <li>• Pace (possessions per game)</li>
                    <li>• Position-specific defense</li>
                    <li>• Points/rebounds/assists allowed</li>
                    <li>• Steals and turnovers forced</li>
                  </ul>
                </div>
                <div>
                  <h4 className="font-medium text-foreground text-sm mb-2">External Factors</h4>
                  <ul className="space-y-1 text-xs text-muted-foreground">
                    <li>• Timezone differences</li>
                    <li>• Arena altitude (Denver effect)</li>
                    <li>• Travel direction (east/west)</li>
                    <li>• Star teammate availability</li>
                    <li>• Recent transactions</li>
                  </ul>
                </div>
              </div>
            </div>
          </AccordionItem>
        </div>
      </div>

      {/* Footer */}
      <div className="rounded-xl border border-border/60 bg-card/50 p-6 text-center space-y-3">
        <p className="text-sm text-muted-foreground">
          Want to dive deeper into the technical details?
        </p>
        <a
          href="https://github.com/yourusername/NBA-Player-Performance-Prediction/blob/main/docs/TECHNICAL_OVERVIEW.md"
          target="_blank"
          rel="noopener noreferrer"
        >
          <Button variant="outline" className="gap-2">
            <ExternalLink className="h-4 w-4" />
            View Full Technical Documentation on GitHub
          </Button>
        </a>
      </div>
    </div>
  );
}

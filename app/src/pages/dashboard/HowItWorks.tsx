import { useState } from 'react';
import {
  Layers,
  Target,
  TrendingUp,
  Database,
  BarChart3,
  Shield,
  ChevronDown,
  ChevronUp,
  Clock,
  MapPin,
  GitBranch,
  ExternalLink,
  Brain
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';

interface AccordionItemProps {
  title: string;
  icon: React.ReactNode;
  children: React.ReactNode;
  defaultOpen?: boolean;
}

function AccordionItem({ title, icon, children, defaultOpen = false }: AccordionItemProps) {
  const [isOpen, setIsOpen] = useState(defaultOpen);

  return (
    <div className="stat-card !p-0 overflow-hidden">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="w-full flex items-center justify-between p-4 text-left hover:bg-accent/5 transition-colors"
      >
        <div className="flex items-center gap-3">
          <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-primary/10 text-primary flex-shrink-0">
            {icon}
          </div>
          <span className="font-medium text-foreground">{title}</span>
        </div>
        <ChevronDown className={cn(
          "h-5 w-5 text-muted-foreground transition-transform duration-200",
          isOpen && "rotate-180"
        )} />
      </button>
      <div className={cn(
        "grid transition-all duration-200",
        isOpen ? "grid-rows-[1fr]" : "grid-rows-[0fr]"
      )}>
        <div className="overflow-hidden">
          <div className="px-4 pb-4 pt-2 border-t border-border/40">
            {children}
          </div>
        </div>
      </div>
    </div>
  );
}

export default function HowItWorks() {
  return (
    <div className="animate-fade-in" style={{ padding: 'var(--density-padding)' }}>
      <div className="space-y-6 max-w-4xl">
        {/* Header - matches other pages */}
        <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
          <div className="min-w-0 flex-1">
            <h1 className="text-3xl font-bold text-foreground leading-tight">How It Works</h1>
            <p className="text-muted-foreground leading-relaxed mt-1">
              Learn how CourtVision generates NBA player predictions
            </p>
          </div>
          <a
            href="https://github.com/kennethhuang7/CourtVision/blob/main/docs/TECHNICAL_OVERVIEW.md"
            target="_blank"
            rel="noopener noreferrer"
            className="flex-shrink-0"
          >
            <Button variant="outline" size="sm" className="gap-2">
              <ExternalLink className="h-4 w-4" />
              Technical Docs
            </Button>
          </a>
        </div>

        {/* Overview Card */}
        <div className="section-gradient">
          <div className="flex items-start gap-4">
            <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-primary/10 text-primary flex-shrink-0">
              <Brain className="h-6 w-6" />
            </div>
            <div className="space-y-1">
              <h2 className="font-semibold text-foreground">Ensemble Machine Learning</h2>
              <p className="text-sm text-muted-foreground leading-relaxed">
                CourtVision combines four different machine learning algorithms to predict player statistics.
                Each model is trained on 150+ features derived from historical player, team, and opponent data.
                By averaging predictions from all four models, individual errors tend to cancel out, resulting in more reliable forecasts.
              </p>
            </div>
          </div>
        </div>

        {/* Accordion Sections */}
        <div className="space-y-3">
          <AccordionItem
            title="The Four Models"
            icon={<Layers className="h-5 w-5" />}
            defaultOpen
          >
            <div className="space-y-4">
              <p className="text-sm text-muted-foreground">
                Each algorithm has unique strengths. The final prediction averages all four to reduce variance.
              </p>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                {[
                  { name: 'XGBoost', color: 'hsl(var(--primary))', desc: 'Strong regularization, prevents overfitting' },
                  { name: 'LightGBM', color: 'hsl(142, 76%, 36%)', desc: 'Fast training, captures complex patterns' },
                  { name: 'CatBoost', color: 'hsl(38, 92%, 50%)', desc: 'Handles categorical data effectively' },
                  { name: 'Random Forest', color: 'hsl(280, 67%, 55%)', desc: 'Stable predictions, resistant to outliers' },
                ].map((model) => (
                  <div key={model.name} className="flex items-start gap-3 rounded-lg border border-border/40 p-3 bg-card/30">
                    <div className="h-2.5 w-2.5 rounded-full mt-1.5 flex-shrink-0" style={{ backgroundColor: model.color }} />
                    <div>
                      <div className="font-medium text-foreground text-sm">{model.name}</div>
                      <div className="text-xs text-muted-foreground">{model.desc}</div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </AccordionItem>

          <AccordionItem
            title="Feature Engineering"
            icon={<Database className="h-5 w-5" />}
          >
            <div className="space-y-4">
              <p className="text-sm text-muted-foreground">
                Over 150 features are calculated for each prediction, using only data available before the game to prevent data leakage.
              </p>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <div className="space-y-2">
                  <div className="flex items-center gap-2 text-sm font-medium text-foreground">
                    <TrendingUp className="h-4 w-4 text-primary" />
                    Player Performance
                  </div>
                  <ul className="text-xs text-muted-foreground space-y-1 ml-6">
                    <li>Rolling averages (last 5, 10, 20 games)</li>
                    <li>Exponentially weighted recent stats</li>
                    <li>Per-36 minute rates</li>
                    <li>Shooting percentages and efficiency</li>
                  </ul>
                </div>
                <div className="space-y-2">
                  <div className="flex items-center gap-2 text-sm font-medium text-foreground">
                    <Shield className="h-4 w-4 text-primary" />
                    Opponent Factors
                  </div>
                  <ul className="text-xs text-muted-foreground space-y-1 ml-6">
                    <li>Team defensive/offensive ratings</li>
                    <li>Position-specific defense</li>
                    <li>Pace and points allowed</li>
                  </ul>
                </div>
                <div className="space-y-2">
                  <div className="flex items-center gap-2 text-sm font-medium text-foreground">
                    <Clock className="h-4 w-4 text-primary" />
                    Rest & Schedule
                  </div>
                  <ul className="text-xs text-muted-foreground space-y-1 ml-6">
                    <li>Days rest, back-to-backs</li>
                    <li>Schedule density</li>
                    <li>Season progress</li>
                  </ul>
                </div>
                <div className="space-y-2">
                  <div className="flex items-center gap-2 text-sm font-medium text-foreground">
                    <MapPin className="h-4 w-4 text-primary" />
                    Context
                  </div>
                  <ul className="text-xs text-muted-foreground space-y-1 ml-6">
                    <li>Home/away, timezone changes</li>
                    <li>Altitude effects (Denver, Utah)</li>
                    <li>Teammate availability</li>
                  </ul>
                </div>
              </div>
            </div>
          </AccordionItem>

          <AccordionItem
            title="Predicted Statistics"
            icon={<BarChart3 className="h-5 w-5" />}
          >
            <div className="space-y-3">
              <p className="text-sm text-muted-foreground">
                Predictions are generated for seven statistical categories. Steals and blocks use Poisson regression
                because they're zero-inflated count data (many games with 0, few with 1-3) rather than continuous values.
              </p>
              <div className="flex flex-wrap gap-2">
                {['Points', 'Rebounds', 'Assists', 'Steals', 'Blocks', 'Turnovers', '3-Pointers'].map((stat) => (
                  <div
                    key={stat}
                    className="rounded-lg bg-primary/10 border border-primary/20 px-3 py-1.5 text-sm font-medium text-foreground"
                  >
                    {stat}
                  </div>
                ))}
              </div>
            </div>
          </AccordionItem>

          <AccordionItem
            title="Confidence Scores"
            icon={<Target className="h-5 w-5" />}
          >
            <div className="space-y-3">
              <p className="text-sm text-muted-foreground">
                Each prediction includes a confidence score (0-100) indicating how favorable the conditions are for an accurate prediction.
              </p>
              <div className="grid grid-cols-2 gap-2">
                {[
                  { label: 'Ensemble Agreement', desc: 'How closely the 4 models agree' },
                  { label: 'Player Consistency', desc: 'Historical variance in stats' },
                  { label: 'Data Quality', desc: 'Feature completeness' },
                  { label: 'Situational', desc: 'Injuries, trades, rest' },
                ].map((item) => (
                  <div key={item.label} className="rounded-lg border border-border/40 p-2.5 bg-card/30">
                    <div className="font-medium text-foreground text-sm">{item.label}</div>
                    <p className="text-xs text-muted-foreground">{item.desc}</p>
                  </div>
                ))}
              </div>
            </div>
          </AccordionItem>

          <AccordionItem
            title="Daily Pipeline"
            icon={<GitBranch className="h-5 w-5" />}
          >
            <div className="space-y-3">
              <p className="text-sm text-muted-foreground">
                Predictions run automatically each day for all scheduled games:
              </p>
              <div className="space-y-2">
                {[
                  'Fetch scheduled games and eligible players (≥5 games played, not injured)',
                  'Build 150+ features from historical data for each player',
                  'Run all four models and average the predictions',
                  'Calculate confidence scores and store results',
                ].map((step, i) => (
                  <div key={i} className="flex items-start gap-3">
                    <div className="flex h-6 w-6 items-center justify-center rounded-full bg-primary/10 text-primary text-xs font-medium flex-shrink-0">
                      {i + 1}
                    </div>
                    <span className="text-sm text-muted-foreground">{step}</span>
                  </div>
                ))}
              </div>
            </div>
          </AccordionItem>
        </div>

        {/* Footer link */}
        <div className="text-sm text-muted-foreground">
          For complete technical details including feature definitions, model parameters, and confidence calculation, see the{' '}
          <a
            href="https://github.com/kennethhuang7/CourtVision/blob/main/docs/TECHNICAL_OVERVIEW.md"
            target="_blank"
            rel="noopener noreferrer"
            className="text-primary hover:underline"
          >
            full documentation on GitHub
          </a>.
        </div>
      </div>
    </div>
  );
}

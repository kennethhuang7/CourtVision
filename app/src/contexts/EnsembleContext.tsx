import React, { createContext, useContext, useState, useEffect, useMemo, useCallback, ReactNode } from 'react';

export type ModelId = 'xgboost' | 'lightgbm' | 'random_forest' | 'catboost';

interface EnsembleContextType {
  selectedModels: ModelId[];
  toggleModel: (model: ModelId) => void;
}

const EnsembleContext = createContext<EnsembleContextType | undefined>(undefined);

const DEFAULT_MODELS: ModelId[] = ['xgboost', 'lightgbm', 'random_forest', 'catboost'];

export function EnsembleProvider({ children }: { children: ReactNode }) {
  const [selectedModels, setSelectedModels] = useState<ModelId[]>(() => {
    const stored = localStorage.getItem('app-ensemble-models');
    if (stored) {
      try {
        const parsed = JSON.parse(stored) as ModelId[];
        
        const validModels = parsed.filter(m => DEFAULT_MODELS.includes(m));
        return validModels.length > 0 ? validModels : DEFAULT_MODELS;
      } catch {
        return DEFAULT_MODELS;
      }
    }
    return DEFAULT_MODELS;
  });

  
  useEffect(() => {
    localStorage.setItem('app-ensemble-models', JSON.stringify(selectedModels));
  }, [selectedModels]);

  const toggleModel = useCallback((model: ModelId) => {
    setSelectedModels((prev) => {
      const newModels = prev.includes(model)
        ? (prev.filter((m) => m !== model) as ModelId[])
        : [...prev, model];
      
      return newModels.length > 0 ? newModels : DEFAULT_MODELS;
    });
  }, []);

  
  const value = useMemo(
    () => ({ selectedModels, toggleModel }),
    [selectedModels, toggleModel]
  );

  return (
    <EnsembleContext.Provider value={value}>
      {children}
    </EnsembleContext.Provider>
  );
}

export function useEnsemble() {
  const ctx = useContext(EnsembleContext);
  if (!ctx) {
    throw new Error('useEnsemble must be used within an EnsembleProvider');
  }
  return ctx;
}



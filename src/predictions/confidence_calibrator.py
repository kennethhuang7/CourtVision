import numpy as np
import joblib
import time
from typing import Optional
from sklearn.isotonic import IsotonicRegression

__all__ = ['ConfidenceCalibrator']


class ConfidenceCalibrator:
    def __init__(self, model_version: Optional[str] = None):
        self.calibrator = IsotonicRegression(out_of_bounds='clip')
        self.is_fitted = False
        self.model_version = model_version or "v1.0"
        self.trained_date = None
    
    def fit(self, raw_scores: np.ndarray, accurate_flags: np.ndarray):
        if len(np.unique(accurate_flags)) < 2:
            raise ValueError(
                "Calibration requires both accurate and inaccurate samples. "
                f"Found only {len(np.unique(accurate_flags))} unique value(s) in accurate_flags."
            )
        
        raw_scores_norm = raw_scores / 105.0
        
        self.calibrator.fit(raw_scores_norm, accurate_flags)
        self.is_fitted = True
        self.trained_date = time.strftime("%Y-%m-%d")
    
    def transform(self, raw_score: float) -> float:
        if not self.is_fitted:
            return raw_score
        
        raw_norm = raw_score / 105.0
        calibrated = self.calibrator.predict([raw_norm])[0]
        return calibrated * 100.0
    
    def save(self, filepath: str):
        metadata = {
            'model_version': self.model_version,
            'trained_date': self.trained_date,
            'is_fitted': self.is_fitted
        }
        joblib.dump({
            'calibrator': self.calibrator,
            'metadata': metadata
        }, filepath)
    
    @staticmethod
    def load(filepath: str) -> 'ConfidenceCalibrator':
        data = joblib.load(filepath)
        calibrator = ConfidenceCalibrator(model_version=data['metadata'].get('model_version'))
        calibrator.calibrator = data['calibrator']
        calibrator.is_fitted = data['metadata'].get('is_fitted', False)
        calibrator.trained_date = data['metadata'].get('trained_date')
        return calibrator





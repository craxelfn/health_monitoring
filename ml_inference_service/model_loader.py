"""Loads and manages PyTorch model for inference"""
import os
import logging
import torch
import torch.nn as nn
from typing import Optional, List, Dict, Any
import numpy as np

logger = logging.getLogger(__name__)


class SimpleAnomalyModel(nn.Module):
    """Simple neural network for anomaly detection (fallback if no model file)"""
    
    def __init__(self, input_dim: int = 9):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(input_dim, 64),
            nn.ReLU(),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 1),
            nn.Sigmoid()
        )
    
    def forward(self, x):
        return self.net(x)


class ModelLoader:
    """Loads PyTorch model for inference"""
    
    def __init__(self, model_path: Optional[str] = None):
        self.model_path = model_path
        self.model = None
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        self._load_model()
    
    def _load_model(self):
        """Load model from file or create default model"""
        if self.model_path and os.path.exists(self.model_path):
            try:
                self.model = torch.load(self.model_path, map_location=self.device)
                self.model.eval()
                logger.info(f"Loaded model from {self.model_path}")
            except Exception as e:
                logger.warning(f"Failed to load model from {self.model_path}: {e}. Using default model.")
                self.model = SimpleAnomalyModel()
                self.model.eval()
        else:
            logger.info("No model file provided, using default simple model")
            self.model = SimpleAnomalyModel()
            self.model.eval()
        
        self.model.to(self.device)
    
    def predict(self, features: List[List[float]]) -> List[Dict[str, Any]]:
        """
        Make predictions on batch of features
        Returns list of predictions with anomaly flag and score
        """
        if not features:
            return []
        
        try:
            # Convert to tensor
            features_tensor = torch.tensor(features, dtype=torch.float32).to(self.device)
            
            # Make prediction
            with torch.no_grad():
                outputs = self.model(features_tensor)
                # If model outputs single value, interpret as anomaly score
                if outputs.dim() == 1 or outputs.shape[1] == 1:
                    scores = outputs.squeeze().cpu().numpy()
                    if isinstance(scores, np.ndarray):
                        scores = scores.tolist()
                    else:
                        scores = [float(scores)]
                else:
                    scores = outputs.cpu().numpy().tolist()
            
            # Convert scores to predictions
            predictions = []
            for score in scores:
                # Threshold for anomaly detection (adjust as needed)
                anomaly_threshold = 0.5
                anomaly = float(score) > anomaly_threshold
                predictions.append({
                    'anomaly': anomaly,
                    'score': float(score),
                    'risk_level': 'high' if anomaly else 'low'
                })
            
            return predictions
            
        except Exception as e:
            logger.error(f"Error making predictions: {e}", exc_info=True)
            # Return default predictions
            return [{'anomaly': False, 'score': 0.0, 'risk_level': 'low'} for _ in features]


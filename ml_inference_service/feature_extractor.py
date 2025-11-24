"""Extracts features from events for ML model"""
from typing import List, Dict, Any


class FeatureExtractor:
    """Extracts feature vectors from events for ML model"""
    
    def extract_features(self, events: List[Dict[str, Any]]) -> List[List[float]]:
        """Extract feature vectors from events"""
        features = []
        for event in events:
            feature_vector = [
                float(event.get('heart_rate', 0.0)),
                float(event.get('temperature', 0.0)),
                float(event.get('oxygen_saturation', 0.0)),
                float(event.get('blood_pressure_systolic', 0.0)),
                float(event.get('blood_pressure_diastolic', 0.0)),
                float(event.get('hrv', 0.0)),
                float(event.get('age', 0.0)) if 'age' in event else 0.0,
                float(event.get('weight_kg', 0.0)) if 'weight_kg' in event else 0.0,
                float(event.get('height_m', 0.0)) if 'height_m' in event else 0.0,
            ]
            features.append(feature_vector)
        return features


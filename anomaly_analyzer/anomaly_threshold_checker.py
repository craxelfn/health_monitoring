"""Checks if predictions exceed critical thresholds"""
from typing import Dict, Any, List


class AnomalyThresholdChecker:
    """Checks if predictions exceed critical thresholds"""
    
    def __init__(self, critical_score_threshold: float = 0.7):
        self.critical_score_threshold = critical_score_threshold
    
    def check_anomalies(self, predictions: List[Dict[str, Any]]) -> bool:
        """Check if any prediction indicates an anomaly"""
        for pred in predictions:
            if pred.get('anomaly', False):
                score = pred.get('score', 0.0)
                if score >= self.critical_score_threshold:
                    return True
        return False
    
    def get_anomalies(self, predictions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Get list of predictions that are anomalies"""
        anomalies = []
        for pred in predictions:
            if pred.get('anomaly', False):
                score = pred.get('score', 0.0)
                if score >= self.critical_score_threshold:
                    anomalies.append(pred)
        return anomalies


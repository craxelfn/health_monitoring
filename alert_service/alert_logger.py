"""Logs alerts for auditing"""
import logging
import json
from datetime import datetime
from typing import Dict, Any
import os

logger = logging.getLogger(__name__)


class AlertLogger:
    """Logs alerts for auditing purposes"""
    
    def __init__(self, log_dir: str = '/var/log/alerts'):
        """
        Initialize alert logger
        Args:
            log_dir: Directory to store alert logs
        """
        self.log_dir = log_dir
        self._ensure_log_dir()
    
    def _ensure_log_dir(self):
        """Ensure log directory exists"""
        try:
            os.makedirs(self.log_dir, exist_ok=True)
        except Exception as e:
            logger.warning(f"Could not create log directory {self.log_dir}: {e}")
            self.log_dir = None
    
    def log_alert(self, alert_data: Dict[str, Any]):
        """
        Log alert to file for auditing
        Args:
            alert_data: Alert data dictionary
        """
        try:
            # Create log entry
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'alert': alert_data
            }
            
            # Log to file if directory available
            if self.log_dir:
                log_file = os.path.join(self.log_dir, f"alerts_{datetime.now().strftime('%Y-%m-%d')}.jsonl")
                with open(log_file, 'a') as f:
                    f.write(json.dumps(log_entry) + '\n')
            
            # Also log to application logger
            patient_id = alert_data.get('patient_id', 'unknown')
            anomaly_count = alert_data.get('anomaly_count', 0)
            logger.warning(
                f"ALERT LOGGED - Patient: {patient_id}, "
                f"Anomalies: {anomaly_count}, "
                f"Timestamp: {alert_data.get('timestamp', 'unknown')}"
            )
            
        except Exception as e:
            logger.error(f"Error logging alert: {e}", exc_info=True)


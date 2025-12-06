"""Redis-based deduplication for alerts"""
import logging
import redis
from typing import Optional

logger = logging.getLogger(__name__)


class RedisDeduplicator:
    """Manages alert deduplication using Redis"""
    
    def __init__(self, redis_host: str = 'redis', redis_port: int = 6379, 
                 redis_db: int = 0, ttl_seconds: int = 300):
        """
        Initialize Redis deduplicator
        Args:
            redis_host: Redis host
            redis_port: Redis port
            redis_db: Redis database number
            ttl_seconds: Time-to-live for alert keys (default 5 minutes = 300 seconds)
        """
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.ttl_seconds = ttl_seconds
        self.redis_client = None
        self._connect()
    
    def _connect(self):
        """Connect to Redis"""
        try:
            self.redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            # Test connection
            self.redis_client.ping()
            logger.info(f"Connected to Redis at {self.redis_host}:{self.redis_port}")
        except Exception as e:
            logger.warning(f"Failed to connect to Redis: {e}. Deduplication disabled.")
            self.redis_client = None
    
    def is_duplicate(self, alert_key: str) -> bool:
        """
        Check if alert is duplicate
        Args:
            alert_key: Unique key for the alert (e.g., "alert:patient123:high_bp")
        Returns:
            True if duplicate, False if new
        """
        if not self.redis_client:
            return False  # If Redis unavailable, don't block alerts
        
        try:
            exists = self.redis_client.exists(alert_key)
            return exists == 1
        except Exception as e:
            logger.error(f"Error checking Redis for duplicate: {e}")
            return False  # On error, allow alert to proceed
    
    def mark_sent(self, alert_key: str) -> bool:
        """
        Mark alert as sent in Redis with TTL
        Args:
            alert_key: Unique key for the alert
        Returns:
            True if successful, False otherwise
        """
        if not self.redis_client:
            return False
        
        try:
            self.redis_client.setex(alert_key, self.ttl_seconds, "1")
            logger.debug(f"Marked alert as sent: {alert_key} (TTL: {self.ttl_seconds}s)")
            return True
        except Exception as e:
            logger.error(f"Error marking alert in Redis: {e}")
            return False
    
    def generate_alert_key(self, patient_id: str, alert_type: str) -> str:
        """
        Generate unique alert key
        Args:
            patient_id: Patient ID
            alert_type: Type of alert (e.g., "high_bp", "anomaly")
        Returns:
            Alert key string
        """
        return f"alert:{patient_id}:{alert_type}"


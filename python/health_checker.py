import time


class ConnectionHealthChecker:
    """Manages connection health checking with exponential backoff retry logic.
    
    Implements smart retry patterns to avoid overwhelming failed services
    while ensuring quick recovery when services come back online.
    """
    
    def __init__(self, initial_delay: float = 1.0, max_delay: float = 60.0, 
                 multiplier: float = 2.0, max_attempts: int = 10):
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.multiplier = multiplier
        self.max_attempts = max_attempts
        
        # Current state
        self.current_delay = initial_delay
        self.consecutive_failures = 0
        self.last_attempt_time = 0.0
    
    def calculate_delay(self) -> float:
        """Calculate delay before next retry attempt using exponential backoff.
        
        Returns:
            Delay in seconds (0 for first attempt, then 1s → 2s → 4s → 8s → ...)
        """
        if self.consecutive_failures == 0:
            return 0.0  # No delay on first attempt
            
        delay = min(self.current_delay, self.max_delay)
        self.current_delay = min(self.current_delay * self.multiplier, self.max_delay)
        return delay
    
    def record_failure(self) -> None:
        """Record a connection failure and update retry state."""
        self.consecutive_failures += 1
        self.last_attempt_time = time.time()
        
        print(f"🔴 Connection failure #{self.consecutive_failures}")
        if self.consecutive_failures >= self.max_attempts:
            print(f"❌ Max retry attempts ({self.max_attempts}) exceeded")
        
    def record_success(self) -> None:
        """Record successful connection and reset retry state."""
        if self.consecutive_failures > 0:
            print(f"✅ Connection recovered after {self.consecutive_failures} failures")
            
        self.consecutive_failures = 0
        self.current_delay = self.initial_delay
        self.last_attempt_time = time.time()
    
    def should_retry(self) -> bool:
        """Check if we should attempt another retry.
        
        Returns:
            True if we haven't exceeded max attempts, False otherwise
        """
        return self.consecutive_failures < self.max_attempts
    
    def get_status(self) -> dict:
        """Get current health checker status for monitoring.
        
        Returns:
            Dict with current state information
        """
        return {
            "consecutive_failures": self.consecutive_failures,
            "current_delay": self.current_delay,
            "is_healthy": self.consecutive_failures == 0,
            "should_retry": self.should_retry(),
            "last_attempt_time": self.last_attempt_time
        }
    
    def wait_with_backoff(self) -> None:
        """Sleep for the calculated backoff delay."""
        delay = self.calculate_delay()
        if delay > 0:
            print(f"⏳ Waiting {delay:.1f}s before retry (attempt {self.consecutive_failures + 1}/{self.max_attempts})")
            time.sleep(delay)
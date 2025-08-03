import time
import random
import logging
from typing import Callable, Any, Dict, Optional
from functools import wraps
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import backoff

class APIOverloadError(Exception):
    """Custom exception for API overload errors (529)."""
    pass

class RateLimitError(Exception):
    """Custom exception for rate limit errors (429)."""
    pass

class CircuitBreaker:
    """Circuit breaker pattern implementation for API resilience."""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
        self.logger = logging.getLogger(__name__)
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection."""
        if self.state == 'OPEN':
            if self._should_attempt_reset():
                self.state = 'HALF_OPEN'
                self.logger.info("Circuit breaker moving to HALF_OPEN state")
            else:
                raise Exception("Circuit breaker is OPEN - API calls blocked")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset."""
        if self.last_failure_time is None:
            return True
        return time.time() - self.last_failure_time >= self.recovery_timeout
    
    def _on_success(self):
        """Handle successful API call."""
        self.failure_count = 0
        self.state = 'CLOSED'
    
    def _on_failure(self):
        """Handle failed API call."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'
            self.logger.warning(f"Circuit breaker OPEN - {self.failure_count} failures")

class AdaptiveRateLimiter:
    """Adaptive rate limiter that adjusts based on API responses."""
    
    def __init__(self, initial_delay: float = 0.5, max_delay: float = 10.0):
        self.current_delay = initial_delay
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.last_request_time = 0
        self.success_count = 0
        self.logger = logging.getLogger(__name__)
    
    def wait(self):
        """Wait appropriate amount of time before next request."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.current_delay:
            sleep_time = self.current_delay - elapsed
            self.logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f}s")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def on_success(self):
        """Handle successful request - gradually decrease delay."""
        self.success_count += 1
        
        # Gradually reduce delay after consecutive successes
        if self.success_count >= 5:
            self.current_delay = max(self.initial_delay, self.current_delay * 0.9)
            self.success_count = 0
            self.logger.debug(f"Reduced rate limit delay to {self.current_delay:.2f}s")
    
    def on_rate_limit(self, retry_after: Optional[int] = None):
        """Handle rate limit response - increase delay."""
        self.success_count = 0
        
        if retry_after:
            self.current_delay = min(self.max_delay, retry_after + 1)
        else:
            self.current_delay = min(self.max_delay, self.current_delay * 2)
        
        self.logger.warning(f"Rate limited - increased delay to {self.current_delay:.2f}s")
    
    def on_overload(self):
        """Handle overload response - significantly increase delay."""
        self.success_count = 0
        self.current_delay = min(self.max_delay, self.current_delay * 3)
        self.logger.warning(f"API overloaded - increased delay to {self.current_delay:.2f}s")

def handle_api_errors(response: requests.Response) -> None:
    """Check response for API errors and raise appropriate exceptions."""
    if response.status_code == 429:
        retry_after = response.headers.get('Retry-After')
        raise RateLimitError(f"Rate limited. Retry after: {retry_after}")
    
    elif response.status_code == 529:
        raise APIOverloadError("API is overloaded")
    
    elif response.status_code >= 500:
        raise requests.exceptions.HTTPError(f"Server error: {response.status_code}")
    
    response.raise_for_status()

@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=1, min=1, max=60),
    retry=retry_if_exception_type((APIOverloadError, RateLimitError, requests.exceptions.ConnectionError))
)
def resilient_api_call(func: Callable, *args, **kwargs) -> Any:
    """
    Make API call with automatic retry logic for overload and rate limit scenarios.
    
    Args:
        func: Function to call
        *args, **kwargs: Arguments for the function
        
    Returns:
        Function result
        
    Raises:
        Exception: If all retry attempts fail
    """
    logger = logging.getLogger(__name__)
    
    try:
        result = func(*args, **kwargs)
        logger.debug("API call successful")
        return result
    except (APIOverloadError, RateLimitError) as e:
        logger.warning(f"API error encountered: {e}. Retrying...")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in API call: {e}")
        raise

@backoff.on_exception(
    backoff.expo,
    (APIOverloadError, RateLimitError, requests.exceptions.ConnectionError),
    max_tries=10,
    max_time=300,  # 5 minutes max
    jitter=backoff.random_jitter
)
def backoff_api_call(func: Callable, *args, **kwargs) -> Any:
    """
    Alternative API call wrapper using backoff library.
    
    Args:
        func: Function to call
        *args, **kwargs: Arguments for the function
        
    Returns:
        Function result
    """
    logger = logging.getLogger(__name__)
    
    try:
        result = func(*args, **kwargs)
        return result
    except (APIOverloadError, RateLimitError) as e:
        logger.warning(f"API error: {e}. Will retry with exponential backoff.")
        raise
    except Exception as e:
        logger.error(f"Non-retryable error: {e}")
        raise

class ResilientAPIClient:
    """Enhanced API client with built-in resilience patterns."""
    
    def __init__(self, circuit_breaker: CircuitBreaker = None, 
                 rate_limiter: AdaptiveRateLimiter = None):
        self.circuit_breaker = circuit_breaker or CircuitBreaker()
        self.rate_limiter = rate_limiter or AdaptiveRateLimiter()
        self.session = requests.Session()
        self.logger = logging.getLogger(__name__)
    
    def make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Make HTTP request with full resilience patterns.
        
        Args:
            method: HTTP method
            url: Request URL
            **kwargs: Additional request parameters
            
        Returns:
            requests.Response: Response object
        """
        @resilient_api_call
        def _request():
            # Rate limiting
            self.rate_limiter.wait()
            
            # Make request through circuit breaker
            def _make_request():
                response = self.session.request(method, url, **kwargs)
                
                # Handle API-specific errors
                if response.status_code == 429:
                    retry_after = response.headers.get('Retry-After')
                    self.rate_limiter.on_rate_limit(int(retry_after) if retry_after else None)
                    raise RateLimitError(f"Rate limited. Retry after: {retry_after}")
                
                elif response.status_code == 529:
                    self.rate_limiter.on_overload()
                    raise APIOverloadError("API is overloaded")
                
                elif response.status_code >= 500:
                    raise requests.exceptions.HTTPError(f"Server error: {response.status_code}")
                
                # Success handling
                if response.status_code < 400:
                    self.rate_limiter.on_success()
                
                response.raise_for_status()
                return response
            
            return self.circuit_breaker.call(_make_request)
        
        return _request()
    
    def get(self, url: str, **kwargs) -> requests.Response:
        """Make GET request with resilience."""
        return self.make_request('GET', url, **kwargs)
    
    def post(self, url: str, **kwargs) -> requests.Response:
        """Make POST request with resilience."""
        return self.make_request('POST', url, **kwargs)
    
    def put(self, url: str, **kwargs) -> requests.Response:
        """Make PUT request with resilience."""
        return self.make_request('PUT', url, **kwargs)
    
    def delete(self, url: str, **kwargs) -> requests.Response:
        """Make DELETE request with resilience."""
        return self.make_request('DELETE', url, **kwargs)
    
    def get_stats(self) -> Dict:
        """Get statistics about API client usage."""
        return {
            'circuit_breaker_state': self.circuit_breaker.state,
            'circuit_breaker_failures': self.circuit_breaker.failure_count,
            'current_rate_limit_delay': self.rate_limiter.current_delay,
            'rate_limiter_success_count': self.rate_limiter.success_count
        }
    
    def reset(self):
        """Reset all resilience components."""
        self.circuit_breaker.failure_count = 0
        self.circuit_breaker.state = 'CLOSED'
        self.circuit_breaker.last_failure_time = None
        
        self.rate_limiter.current_delay = self.rate_limiter.initial_delay
        self.rate_limiter.success_count = 0
        
        self.logger.info("API client reset - all resilience patterns cleared")

def create_resilient_session(initial_delay: float = 0.5, 
                           max_delay: float = 10.0,
                           failure_threshold: int = 5,
                           recovery_timeout: int = 60) -> ResilientAPIClient:
    """
    Factory function to create a resilient API client.
    
    Args:
        initial_delay: Initial rate limiting delay
        max_delay: Maximum rate limiting delay
        failure_threshold: Circuit breaker failure threshold
        recovery_timeout: Circuit breaker recovery timeout
        
    Returns:
        ResilientAPIClient: Configured resilient API client
    """
    circuit_breaker = CircuitBreaker(failure_threshold, recovery_timeout)
    rate_limiter = AdaptiveRateLimiter(initial_delay, max_delay)
    
    return ResilientAPIClient(circuit_breaker, rate_limiter)
import requests
import random
import time
import threading
import logging
import sys
import uuid

class WorkloadSimulator:
    def __init__(self, 
                 base_url: str = "http://localhost:8000", 
                 endpoints: list = ["/api/v1/users"],
                 request_rate: float = 1,  # Reduced to 1 request per second for visibility
                 duration: float = 10,     # Run for 10 seconds
                 method: str = 'get'):
        """
        Initialize WorkloadSimulator with debugging output
        """
        # Configure logging to print to console
        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s - %(levelname)s: %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        self.logger = logging.getLogger(__name__)
        
        self.base_url = base_url
        self.endpoints = endpoints
        self.request_rate = request_rate
        self.duration = duration
        self.method = method.lower()
        
        # Tracking metrics
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
    
    def _send_request(self):
        """
        Send a single request and log the result
        """
        # Select a random endpoint
        endpoint = random.choice(self.endpoints)
        full_url = f"{self.base_url}{endpoint}"
        
        self.logger.info(f"Attempting to send {self.method.upper()} request to {full_url}")
        
        try:
            # Dynamic method selection
            request_method = getattr(requests, self.method)
            
            # Send request
            response = request_method(full_url)
            
            # Update metrics
            self.total_requests += 1
            if 200 <= response.status_code < 300:
                self.successful_requests += 1
                self.logger.info(f"Request successful: {response.status_code}")
            else:
                self.failed_requests += 1
                self.logger.warning(f"Request failed: {response.status_code}")
            
        except Exception as e:
            self.failed_requests += 1
            self.logger.error(f"Request error: {e}")
    
    def simulate_requests(self):
        """
        Simulate requests for a specified duration
        """
        self.logger.info("Starting workload simulation")
        start_time = time.time()
        
        while time.time() - start_time < self.duration:
            self._send_request()
            time.sleep(1 / self.request_rate)
        
        self._print_summary()
    
    def _print_summary(self):
        """
        Print simulation summary
        """
        self.logger.info("\n--- Workload Simulation Summary ---")
        self.logger.info(f"Total Requests: {self.total_requests}")
        self.logger.info(f"Successful Requests: {self.successful_requests}")
        self.logger.info(f"Failed Requests: {self.failed_requests}")
        
        success_rate = (self.successful_requests / self.total_requests * 100) if self.total_requests > 0 else 0
        self.logger.info(f"Success Rate: {success_rate:.2f}%")

# Ensure the script runs when executed directly
if __name__ == "__main__":
    print("Initializing Workload Simulator...")
    simulator = WorkloadSimulator(
        base_url="http://localhost:8000",
        endpoints=["/api/v1/users","/"],
        request_rate=1,  # 1 request per second
        duration=10,     # Run for 10 seconds
        method='get'
    )
    simulator.simulate_requests()
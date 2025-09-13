# api_sensors.py
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import requests

class APISensor(BaseSensorOperator):
    """
    Custom sensor that waits for an API endpoint to return a desired response.
    """

    @apply_defaults
    def __init__(self, endpoint: str, expected_status=200, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.expected_status = expected_status

    def poke(self, context):
        """
        Called repeatedly until True is returned.
        """
        self.log.info(f"Checking API endpoint: {self.endpoint}")
        try:
            response = requests.get(self.endpoint)
            self.log.info(f"API responded with status code {response.status_code}")
            if response.status_code == self.expected_status:
                self.log.info("Expected response received!")
                return True
            return False
        except Exception as e:
            self.log.error(f"Error contacting API: {e}")
            return False

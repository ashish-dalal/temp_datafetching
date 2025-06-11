import os
from dotenv import load_dotenv
from typing import Any, Dict

# Load environment variables from .env file
load_dotenv()




class Config:
    """Configuration class that dynamically loads all environment variables."""
    
    def __init__(self):
        # Get all environment variables
        self._env_vars: Dict[str, Any] = dict(os.environ)
        
        # Convert string values to appropriate types
        for key, value in self._env_vars.items():
            try:
                self._env_vars[key] = int(value)
            except (ValueError, TypeError):
                lower_value = str(value).lower()
                if lower_value in ("true", "false"):
                    self._env_vars[key] = (lower_value == "true")
                else:
                    self._env_vars[key] = value
    
    def __getattr__(self, name: str) -> Any:
        """Dynamically get environment variables as attributes."""
        if name in self._env_vars:
            return self._env_vars[name]
        return None
    
    def __getitem__(self, key: str) -> Any:
        """Allow dictionary-style access to configuration values."""
        return self._env_vars.get(key)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Safely get a configuration value with a default."""
        return self._env_vars.get(key, default)
    
    def validate_required(self, *required_vars: str) -> bool:
        """Validate that required configuration values are present."""
        missing_vars = [var for var in required_vars if var not in self._env_vars or not self._env_vars[var]]
        if missing_vars:
            print(f"Missing required configuration variables: {', '.join(missing_vars)}")
            return False
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Return all configuration as a dictionary."""
        return self._env_vars.copy()

# Create a global config instance
config = Config() 
print(config.get("ZERODHA_ACCESS_TOKEN"))
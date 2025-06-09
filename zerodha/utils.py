import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get the absolute path to the script directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
METRICS_FILE = os.path.join(SCRIPT_DIR, "metrics.json")

def update_metrics(stream_type, processed_count, queue_size, symbol=None):
    """
    Update metrics.json with current processing statistics
    
    Args:
        stream_type (str): Type of data stream (full, quote, ltp)
        processed_count (int): Number of events processed so far
        queue_size (int): Current size of the event queue
        symbol (str): Most recent trading symbol processed
    """
    try:
        logger.info(f"Updating metrics in {METRICS_FILE}")
        
        # Initialize metrics data
        metrics = {
            "last_updated": datetime.now().isoformat(),
            "last_updated_timestamp": int(time.time()),
            "stream_type": stream_type,
            "events_processed": processed_count,
            "queue_size": queue_size,
            "status": "active"
        }
        
        # Add the most recently processed symbol if available
        if symbol and symbol != "STARTUP":
            metrics["last_symbol"] = symbol
            
        # Read existing metrics if file exists
        if os.path.exists(METRICS_FILE):
            try:
                with open(METRICS_FILE, "r") as f:
                    existing = json.load(f)
                    
                # Preserve some fields from existing metrics
                if "start_time" in existing:
                    metrics["start_time"] = existing["start_time"]
                else:
                    metrics["start_time"] = metrics["last_updated"]
                    
                # Calculate uptime if we have start time
                if "start_time" in metrics:
                    start_time = datetime.fromisoformat(metrics["start_time"])
                    current_time = datetime.fromisoformat(metrics["last_updated"])
                    uptime_seconds = (current_time - start_time).total_seconds()
                    metrics["uptime_seconds"] = int(uptime_seconds)
                    
                # Calculate rates
                if "events_processed" in existing and "last_updated_timestamp" in existing:
                    time_diff = metrics["last_updated_timestamp"] - existing["last_updated_timestamp"]
                    if time_diff > 0:
                        events_diff = metrics["events_processed"] - existing["events_processed"]
                        rate = events_diff / time_diff
                        metrics["processing_rate"] = round(rate, 2)
            except Exception as e:
                logger.error(f"Error reading existing metrics: {e}")
                # If we couldn't read the existing file, this is the start time
                metrics["start_time"] = metrics["last_updated"]
        else:
            # New metrics file, this is the start time
            metrics["start_time"] = metrics["last_updated"]
            logger.info(f"Creating new metrics file at {METRICS_FILE}")
        
        # Write updated metrics to file
        with open(METRICS_FILE, "w") as f:
            json.dump(metrics, f, indent=2)
            
        logger.info(f"Updated metrics: processed={processed_count}, queue_size={queue_size}, file={METRICS_FILE}")
        return True
    except Exception as e:
        logger.error(f"Error updating metrics: {e}")
        return False

def get_metrics():
    """
    Read current metrics from metrics.json
    
    Returns:
        dict: Current metrics or empty dict if file doesn't exist
    """
    try:
        if os.path.exists(METRICS_FILE):
            with open(METRICS_FILE, "r") as f:
                return json.load(f)
        else:
            logger.warning(f"Metrics file does not exist at {METRICS_FILE}")
            return {}
    except Exception as e:
        logger.error(f"Error reading metrics: {e}")
        return {}

def save_metrics():
    """Force update metrics at shutdown"""
    try:
        current_metrics = get_metrics()
        if current_metrics:
            current_metrics["status"] = "stopped"
            current_metrics["last_updated"] = datetime.now().isoformat()
            current_metrics["last_updated_timestamp"] = int(time.time())
            
            with open(METRICS_FILE, "w") as f:
                json.dump(current_metrics, f, indent=2)
            logger.info(f"Saved shutdown metrics to {METRICS_FILE}")
        else:
            logger.warning("No metrics to save at shutdown")
    except Exception as e:
        logger.error(f"Error saving metrics at shutdown: {e}")

def check_metrics_file():
    """Check if metrics file exists and is writeable, create if not"""
    try:
        if not os.path.exists(METRICS_FILE):
            # Create an initial metrics file
            initial_metrics = {
                "start_time": datetime.now().isoformat(),
                "last_updated": datetime.now().isoformat(),
                "last_updated_timestamp": int(time.time()),
                "events_processed": 0,
                "queue_size": 0,
                "status": "starting"
            }
            
            with open(METRICS_FILE, "w") as f:
                json.dump(initial_metrics, f, indent=2)
            logger.info(f"Created initial metrics file at {METRICS_FILE}")
            return True
        else:
            # Check if file is writeable
            with open(METRICS_FILE, "a") as f:
                pass
            logger.info(f"Metrics file exists and is writeable at {METRICS_FILE}")
            return True
    except Exception as e:
        logger.error(f"Error checking/creating metrics file: {e}")
        logger.error(f"Check if directory {os.path.dirname(METRICS_FILE)} exists and is writeable")
        return False 
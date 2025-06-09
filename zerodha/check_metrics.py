import json
import os
import sys
import time
from datetime import datetime, timedelta

def format_time_ago(timestamp):
    """Format a timestamp as time ago"""
    if not timestamp:
        return "unknown"
    
    now = int(time.time())
    diff = now - timestamp
    
    if diff < 60:
        return f"{diff} seconds ago"
    elif diff < 3600:
        return f"{diff // 60} minutes ago"
    elif diff < 86400:
        return f"{diff // 3600} hours ago"
    else:
        return f"{diff // 86400} days ago"

def format_uptime(seconds):
    """Format seconds as uptime string"""
    if not seconds:
        return "unknown"
    
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    if days > 0:
        return f"{days}d {hours}h {minutes}m {seconds}s"
    elif hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"

def check_metrics():
    """Check and display metrics from metrics.json"""
    # Get script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    metrics_file = os.path.join(script_dir, "metrics.json")
    
    if not os.path.exists(metrics_file):
        print(f"ERROR: Metrics file not found at {metrics_file}")
        print("The data fetcher may not be running or metrics.json has not been created.")
        sys.exit(1)
    
    try:
        with open(metrics_file, "r") as f:
            metrics = json.load(f)
        
        # Print header
        print("\n" + "=" * 60)
        print("ZERODHA DATA FETCHER STATUS")
        print("=" * 60)
        
        # Status and timing information
        status = metrics.get("status", "unknown")
        status_color = "\033[92m" if status == "active" else "\033[91m"  # Green if active, red otherwise
        print(f"Status: {status_color}{status}\033[0m")
        
        last_updated = metrics.get("last_updated_timestamp")
        if last_updated:
            print(f"Last Updated: {format_time_ago(last_updated)}")
            
            # Check if data fetcher is stale (no updates in 5 minutes)
            if status == "active" and time.time() - last_updated > 300:
                print("\033[93mWARNING: No updates in over 5 minutes!\033[0m")
        
        # Uptime
        if "uptime_seconds" in metrics:
            print(f"Uptime: {format_uptime(metrics['uptime_seconds'])}")
        elif "start_time" in metrics:
            try:
                start_time = datetime.fromisoformat(metrics["start_time"])
                current_time = datetime.now()
                uptime = (current_time - start_time).total_seconds()
                print(f"Uptime: {format_uptime(int(uptime))}")
            except:
                print("Uptime: unknown")
        
        # Processing statistics
        print("\nPROCESSING STATISTICS")
        print("-" * 60)
        print(f"Stream Type: {metrics.get('stream_type', 'unknown')}")
        print(f"Events Processed: {metrics.get('events_processed', 0):,}")
        print(f"Current Queue Size: {metrics.get('queue_size', 0):,}")
        
        if "processing_rate" in metrics:
            rate = metrics["processing_rate"]
            if rate > 50:
                rate_status = "\033[92m"  # Green
            elif rate > 10:
                rate_status = "\033[93m"  # Yellow
            else:
                rate_status = "\033[91m"  # Red
            print(f"Processing Rate: {rate_status}{rate:.2f}\033[0m events/second")
        
        # Last symbol processed
        if "last_symbol" in metrics:
            print(f"\nLast Symbol: {metrics['last_symbol']}")
        
        print("\n" + "=" * 60)
        
        # Quick health assessment
        if status == "active":
            if "processing_rate" in metrics and metrics["processing_rate"] > 0:
                if time.time() - last_updated < 60:
                    print("\033[92mSystem appears to be functioning normally.\033[0m")
                else:
                    print("\033[93mSystem is active but may be experiencing delays in updating metrics.\033[0m")
            else:
                print("\033[93mSystem is active but does not appear to be processing data.\033[0m")
        else:
            print("\033[91mSystem is not active. Start the data fetcher if data collection is required.\033[0m")
        
    except Exception as e:
        print(f"ERROR: Failed to read or parse metrics file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    check_metrics() 
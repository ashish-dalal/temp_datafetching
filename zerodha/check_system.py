import subprocess
import threading
import time
import os
import sys
import signal

def run_ticker_test():
    """Run the ticker test in a subprocess"""
    print("\n" + "=" * 60)
    print("STARTING ZERODHA CONNECTION TEST")
    print("=" * 60)
    
    try:
        # Run ticker_test.py with output streaming to console
        process = subprocess.Popen(
            [sys.executable, "ticker_test.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1  # Line buffered
        )
        
        # Print output in real-time
        for line in process.stdout:
            print(line, end='')
        
        process.wait()
        return process.returncode
    except KeyboardInterrupt:
        print("\nStopping ticker test...")
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
        return 1
    except Exception as e:
        print(f"Error running ticker test: {e}")
        return 1

def run_metrics_check():
    """Run the metrics check in a subprocess"""
    print("\n" + "=" * 60)
    print("CHECKING DATA FETCHER METRICS")
    print("=" * 60)
    
    try:
        # Run check_metrics.py and capture output
        result = subprocess.run(
            [sys.executable, "check_metrics.py"],
            capture_output=True,
            text=True
        )
        
        # Print output
        print(result.stdout)
        
        if result.stderr:
            print("ERRORS:")
            print(result.stderr)
        
        return result.returncode
    except Exception as e:
        print(f"Error checking metrics: {e}")
        return 1

def main():
    """Main function to check system status"""
    print("\n" + "=" * 60)
    print("ZERODHA DATA FETCHER SYSTEM CHECK")
    print("=" * 60)
    print(f"Running in: {os.getcwd()}")
    print("Time:", time.strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 60)
    
    # First check metrics
    metrics_status = run_metrics_check()
    
    # Then run ticker test (can be interrupted with Ctrl+C)
    print("\nRunning Zerodha connection test (press Ctrl+C to stop)...")
    ticker_status = run_ticker_test()
    
    # Check for any issues
    if metrics_status != 0 or ticker_status != 0:
        print("\nWARNING: One or more checks failed. Review the output above for details.")
        return 1
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nSystem check interrupted by user.")
        sys.exit(1) 
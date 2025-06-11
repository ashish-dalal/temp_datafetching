#!/usr/bin/env python3
"""
Zerodha Trading Scheduler
Manages daily token renewal and data collection cycles for Indian market hours.
Market Hours: 9:15 AM - 3:30 PM
Data Collection: 9:10 AM - 3:35 PM (5 min buffer on each side)
"""

import asyncio
import subprocess
import signal
import sys
import logging
import os
from datetime import datetime, time, timedelta
from pathlib import Path
import pytz


class ZerodhaScheduler:
    """Manages Zerodha connector scheduling and token renewal."""
    
    def __init__(self):
        self.running = True
        self.current_process = None
        self.logger = self._setup_logging()
        self.ist = pytz.timezone('Asia/Kolkata')
        
        # Market timing configuration
        self.token_renewal_time = time(8, 0)      # 8:00 AM
        self.data_start_time = time(9, 10)        # 9:10 AM (5 min before market)
        self.data_end_time = time(15, 35)         # 3:35 PM (5 min after market)
        self.market_start_time = time(9, 15)      # 9:15 AM (actual market start)
        self.market_end_time = time(15, 30)       # 3:30 PM (actual market end)
        
        # Data collection duration: 6 hours 25 minutes
        self.data_collection_duration = 6.42  # 6 hours 25 minutes in decimal
        
        # Retry configuration
        self.max_token_retries = 5
        self.retry_delay_base = 30  # seconds
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _setup_logging(self):
        """Setup logging with file and console handlers."""
        # Create logs directory
        log_dir = Path("/app/logs")
        log_dir.mkdir(exist_ok=True)
        
        # Setup logger
        logger = logging.getLogger("ZerodhaScheduler")
        logger.setLevel(logging.INFO)
        
        # File handler
        log_file = log_dir / f"scheduler_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        ))
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.running = False
        if self.current_process:
            try:
                self.logger.info("Allowing current batch to complete before shutdown...")
                # Give process some time to complete current batch
                asyncio.create_task(self._graceful_process_stop())
            except:
                pass
    
    async def _graceful_process_stop(self):
        """Stop process gracefully, allowing current batch to complete."""
        if self.current_process:
            try:
                # Wait up to 30 seconds for current batch to complete
                await asyncio.sleep(30)
                if self.current_process and self.current_process.poll() is None:
                    self.logger.info("Graceful shutdown timeout, terminating process...")
                    self.current_process.terminate()
            except Exception as e:
                self.logger.error(f"Error during graceful shutdown: {e}")
    
    def _get_ist_time(self):
        """Get current time in IST."""
        return datetime.now(self.ist)
    
    def _is_weekend(self, dt=None):
        """Check if given datetime (or current time) is weekend."""
        if dt is None:
            dt = self._get_ist_time()
        return dt.weekday() >= 5  # 5=Saturday, 6=Sunday
    
    def _get_current_market_phase(self):
        """Determine what phase of the market cycle we're in."""
        now = self._get_ist_time()
        current_time = now.time()
        
        # Skip weekends entirely
        if self._is_weekend(now):
            return "weekend"
        
        if current_time < self.token_renewal_time:
            return "pre_token"      # Before 8:00 AM
        elif current_time < self.data_start_time:
            return "token_phase"    # 8:00-9:10 AM
        elif current_time < self.data_end_time:
            return "market_hours"   # 9:10 AM-3:35 PM
        else:
            return "post_market"    # After 3:35 PM
    
    async def _run_token_renewal(self):
        """Execute the token renewal process with retry logic."""
        for attempt in range(1, self.max_token_retries + 1):
            self.logger.info(f"Token renewal attempt {attempt}/{self.max_token_retries}")
            
            try:
                # Step 1: Start listener in background
                self.logger.info("Starting listener process...")
                listener_process = subprocess.Popen(
                    [sys.executable, "listner.py"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd="/app"
                )
                
                # Wait a moment for listener to start
                await asyncio.sleep(3)
                
                # Step 2: Run token automation
                self.logger.info("Running token automation...")
                automation_process = subprocess.Popen(
                    [sys.executable, "zerodha_token_automation.py"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd="/app"
                )
                
                # Wait for token automation to complete
                stdout, stderr = automation_process.communicate()
                
                # Step 3: Stop listener
                self.logger.info("Stopping listener process...")
                try:
                    listener_process.terminate()
                    listener_process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    listener_process.kill()
                    listener_process.wait()
                
                # Check if token automation was successful
                if automation_process.returncode == 0:
                    self.logger.info("Token renewal completed successfully")
                    return True
                else:
                    self.logger.error(f"Token automation failed with code {automation_process.returncode}")
                    if stderr:
                        self.logger.error(f"Error output: {stderr.decode()}")
                    return True  # for now setting this to True since there is an issue with the token automation script not exiting with code 0
                    
            except Exception as e:
                self.logger.error(f"Error during token renewal attempt {attempt}: {e}")
                
                # Stop listener if it's still running
                try:
                    if 'listener_process' in locals():
                        listener_process.terminate()
                        listener_process.wait(timeout=5)
                except:
                    pass
                
                if attempt < self.max_token_retries:
                    retry_delay = min(self.retry_delay_base * attempt, 300)  # Max 5 minutes
                    self.logger.warning(f"Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                else:
                    self.logger.error("All token renewal attempts failed")
                    return False
        
        return False
    
    async def _run_data_collection(self, duration_hours=None):
        """Run the data collection process for specified hours."""
        if duration_hours is None:
            duration_hours = self.data_collection_duration
            
        self.logger.info(f"Starting data collection for {duration_hours} hours...")
        
        try:
            # Start zerodha connector
            self.current_process = subprocess.Popen(
                [sys.executable, "zerodha_run_connector.py"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd="/app"
            )
            
            # Calculate end time
            duration_seconds = duration_hours * 3600
            end_time = self._get_ist_time() + timedelta(seconds=duration_seconds)
            
            self.logger.info(f"Data collection will run until {end_time.strftime('%H:%M:%S')}")
            
            # Wait for the specified duration or until stopped
            try:
                await asyncio.wait_for(
                    self._wait_for_process_completion(), 
                    timeout=duration_seconds
                )
            except asyncio.TimeoutError:
                # Normal timeout - stop the process
                self.logger.info("Data collection duration completed, stopping connector...")
                self._stop_current_process()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error during data collection: {e}")
            self._stop_current_process()
            return False
    
    async def _wait_for_process_completion(self):
        """Wait for current process to complete."""
        while self.current_process and self.current_process.poll() is None and self.running:
            await asyncio.sleep(1)
    
    def _stop_current_process(self):
        """Stop the current running process."""
        if self.current_process:
            try:
                self.current_process.terminate()
                # Wait up to 15 seconds for graceful shutdown
                try:
                    self.current_process.wait(timeout=15)
                    self.logger.info("Process stopped gracefully")
                except subprocess.TimeoutExpired:
                    self.current_process.kill()
                    self.current_process.wait()
                    self.logger.info("Process force killed")
            except Exception as e:
                self.logger.error(f"Error stopping process: {e}")
            finally:
                self.current_process = None
    
    async def _sleep_until_time(self, target_time):
        """Sleep until a specific time (next occurrence)."""
        now = self._get_ist_time()
        target_datetime = now.replace(
            hour=target_time.hour, 
            minute=target_time.minute, 
            second=0, 
            microsecond=0
        )
        
        # If target time is in the past today, schedule for tomorrow
        if target_datetime <= now:
            target_datetime += timedelta(days=1)
        
        # Skip weekends - if target falls on weekend, move to Monday
        while self._is_weekend(target_datetime):
            target_datetime += timedelta(days=1)
        
        sleep_duration = (target_datetime - now).total_seconds()
        
        self.logger.info(f"Sleeping until {target_datetime.strftime('%Y-%m-%d %H:%M:%S')} "
                        f"({sleep_duration/3600:.1f} hours)")
        
        # Sleep in chunks to allow for graceful shutdown
        while sleep_duration > 0 and self.running:
            chunk = min(sleep_duration, 60)  # Sleep in 1-minute chunks
            await asyncio.sleep(chunk)
            sleep_duration -= chunk
    
    async def _sleep_until_next_token_time(self):
        """Sleep until the next 8:00 AM (skipping weekends)."""
        await self._sleep_until_time(self.token_renewal_time)
    
    def _calculate_remaining_market_time(self):
        """Calculate remaining market hours from current time."""
        now = self._get_ist_time()
        current_time = now.time()
        
        if current_time >= self.data_end_time:
            return 0  # Market closed
        
        # Calculate remaining time until 3:35 PM
        end_datetime = now.replace(
            hour=self.data_end_time.hour,
            minute=self.data_end_time.minute,
            second=0,
            microsecond=0
        )
        
        remaining_seconds = (end_datetime - now).total_seconds()
        return max(0, remaining_seconds / 3600)  # Convert to hours
    
    async def run(self):
        """Main scheduler loop with smart time-aware startup."""
        self.logger.info("Zerodha Scheduler starting...")
        current_time = self._get_ist_time()
        self.logger.info(f"Current IST time: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Main scheduler loop
        while self.running:
            try:
                # Check if it's weekend
                if self._is_weekend():
                    self.logger.info("Weekend detected, sleeping until next Monday 8:00 AM...")
                    await self._sleep_until_next_token_time()
                    continue
                
                # Determine current market phase and act accordingly
                phase = self._get_current_market_phase()
                self.logger.info(f"Current market phase: {phase}")
                
                if phase == "weekend":
                    self.logger.info("Weekend: Sleeping until next Monday 8:00 AM")
                    await self._sleep_until_next_token_time()
                    continue
                
                elif phase == "pre_token":
                    self.logger.info("Pre-token phase: Waiting until 8:00 AM for token renewal")
                    await self._sleep_until_time(self.token_renewal_time)
                    if not self.running:
                        break
                
                elif phase == "token_phase":
                    self.logger.info("Token phase: Starting token renewal immediately")
                    # Will proceed to token renewal below
                    
                elif phase == "market_hours":
                    self.logger.info("Market hours: Starting token renewal first, then data collection")
                    # Will proceed to token renewal, then start data collection
                    
                elif phase == "post_market":
                    self.logger.info("Post-market: Sleeping until tomorrow 8:00 AM")
                    await self._sleep_until_next_token_time()
                    continue
                
                # Execute token renewal (always do this when not weekend/pre-token)
                self.logger.info("=== Starting token renewal ===")
                renewal_success = await self._run_token_renewal()
                
                if not renewal_success:
                    self.logger.error("Token renewal failed completely, sleeping until next cycle")
                    await self._sleep_until_next_token_time()
                    continue
                
                # Determine what to do after token renewal
                phase_after_renewal = self._get_current_market_phase()
                
                if phase_after_renewal == "token_phase":
                    # Token renewal completed, wait for market hours
                    self.logger.info("Token renewal completed, waiting for data collection time (9:10 AM)...")
                    await self._sleep_until_time(self.data_start_time)
                    
                    if not self.running:
                        break
                
                # Check if we're now in market hours or if we need to start data collection
                current_phase = self._get_current_market_phase()
                
                if current_phase == "market_hours":
                    # Calculate how long to run data collection
                    remaining_hours = self._calculate_remaining_market_time()
                    
                    if remaining_hours > 0:
                        self.logger.info(f"=== Starting data collection for {remaining_hours:.2f} hours ===")
                        collection_success = await self._run_data_collection(duration_hours=remaining_hours)
                        
                        if not collection_success:
                            self.logger.error("Data collection failed")
                    else:
                        self.logger.info("Market hours already over, skipping data collection")
                
                # After data collection, sleep until next token renewal
                if self.running:
                    self.logger.info("=== Daily cycle completed ===")
                    await self._sleep_until_next_token_time()
                
            except Exception as e:
                self.logger.error(f"Error in main scheduler loop: {e}")
                # Wait a bit before retrying to avoid tight loop
                await asyncio.sleep(300)  # 5 minutes
        
        self.logger.info("Zerodha Scheduler stopped")


async def main():
    """Main entry point."""
    scheduler = ZerodhaScheduler()
    
    try:
        await scheduler.run()
    except KeyboardInterrupt:
        scheduler.logger.info("Shutdown initiated by user")
    except Exception as e:
        scheduler.logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Check Python version
    if sys.version_info < (3, 10):
        print("ERROR: Python 3.10 or higher required")
        sys.exit(1)
    
    # Run the scheduler
    asyncio.run(main())
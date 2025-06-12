#!/usr/bin/env python3

# NOTE: THIS IS FOR `./orchestrator.py`
"""
IBKR Trading Scheduler for Orchestrator
Manages IBKR connector for US market hours.
Market Hours: 7:20 PM - 1:35 AM IST (US trading hours)
"""

import asyncio
import subprocess
import signal
import sys
import logging
import os
import yaml
from datetime import datetime, time, timedelta
from pathlib import Path
import pytz


class IBKRScheduler:
    """Manages IBKR connector scheduling for US market hours."""
    
    def __init__(self):
        self.running = True
        self.current_process = None
        self.logger = self._setup_logging()
        self.ist = pytz.timezone('Asia/Kolkata')
        
        # Load configuration
        self.config = self._load_config()
        
        # Market timing configuration from config
        self.market_start_time = time.fromisoformat(self.config['ibkr']['market_start_time'])
        self.market_end_time = time.fromisoformat(self.config['ibkr']['market_end_time'])
        self.connection_retry_delay = self.config['ibkr']['connection_retry_delay']
        self.max_retries = self.config['ibkr']['max_retries']
        
        # Pre-market check timing (configurable minutes before market open)
        pre_market_buffer = self.config['ibkr']['pre_market_check_minutes']
        pre_market_minutes = self.market_start_time.hour * 60 + self.market_start_time.minute - pre_market_buffer
        if pre_market_minutes < 0:
            pre_market_minutes += 24 * 60  # Handle day boundary
        self.pre_market_check_time = time(pre_market_minutes // 60, pre_market_minutes % 60)
        
        # Market session duration (handles overnight sessions)
        if self.market_start_time > self.market_end_time:
            # Overnight session (19:20 PM to 01:35 AM next day)
            start_minutes = self.market_start_time.hour * 60 + self.market_start_time.minute
            end_minutes = self.market_end_time.hour * 60 + self.market_end_time.minute + 24 * 60  # Next day
            self.market_duration_hours = (end_minutes - start_minutes) / 60
        else:
            # Same day session
            start_minutes = self.market_start_time.hour * 60 + self.market_start_time.minute
            end_minutes = self.market_end_time.hour * 60 + self.market_end_time.minute
            self.market_duration_hours = (end_minutes - start_minutes) / 60
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _load_config(self):
        """Load configuration from YAML file."""
        config_path = Path(__file__).resolve().parent.parent / "config.yaml"
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.logger.error(f"Failed to load config from {config_path}: {e}")
            sys.exit(1)
    
    def _setup_logging(self):
        """Setup logging with file and console handlers."""
        # Create logs directory
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        # Setup logger
        logger = logging.getLogger("IBKRScheduler")
        logger.setLevel(logging.INFO)
        
        # File handler
        log_file = log_dir / f"ibkr_scheduler_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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
        
        # Handle overnight market hours (19:20 PM to 01:35 AM next day)
        if self.market_start_time > self.market_end_time:
            # Market crosses midnight
            if current_time >= self.market_start_time or current_time <= self.market_end_time:
                return "market_hours"
            elif current_time >= self.pre_market_check_time and current_time < self.market_start_time:
                return "pre_market_check"
            else:
                return "post_market"
        else:
            # Market within same day
            if self.market_start_time <= current_time <= self.market_end_time:
                return "market_hours"
            elif self.pre_market_check_time <= current_time < self.market_start_time:
                return "pre_market_check"
            elif current_time < self.pre_market_check_time:
                return "pre_market"
            else:
                return "post_market"
    
    async def _setup_tables_if_needed(self):
        """Setup QuestDB tables for IBKR if needed."""
        try:
            # Import and use test functions
            sys.path.append(str(Path(__file__).parent))
            from test_connection import test_questdb_connection
            
            self.logger.info("Checking IBKR tables in QuestDB...")
            result = test_questdb_connection()
            
            if result == 0:
                self.logger.info("IBKR tables already exist and are accessible")
                return True
            elif result == 10:
                self.logger.error("QuestDB connection failed")
                return False
            elif result >= 20:
                self.logger.info("IBKR tables missing, creating them...")
                
                # Run create_tables.py
                ibkr_path = Path(__file__).parent
                process = subprocess.Popen(
                    [sys.executable, "create_tables.py"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=str(ibkr_path)
                )
                
                stdout, stderr = process.communicate()
                
                if process.returncode == 0:
                    self.logger.info("Tables created successfully")
                    
                    # Re-test to verify
                    result = test_questdb_connection()
                    if result == 0:
                        self.logger.info("Table creation verified")
                        return True
                    else:
                        self.logger.error(f"Table verification failed with code: {result}")
                        return False
                else:
                    self.logger.error(f"Table creation failed: {stderr.decode()}")
                    return False
            else:
                self.logger.warning(f"QuestDB test returned code: {result}, proceeding anyway")
                return True
                
        except Exception as e:
            self.logger.error(f"Error setting up IBKR tables: {e}")
            return False
    
    async def _run_data_collection(self, duration_hours=None):
        """Run the IBKR data collection process for specified hours."""
        if duration_hours is None:
            duration_hours = self.market_duration_hours
            
        self.logger.info(f"Starting IBKR data collection for {duration_hours} hours...")
        
        # Try to start IBKR connector with retries
        for attempt in range(1, self.max_retries + 1):
            try:
                self.logger.info(f"Starting IBKR connector (attempt {attempt}/{self.max_retries})")
                
                # Start IBKR connector
                ibkr_path = Path(__file__).parent
                self.current_process = subprocess.Popen(
                    [sys.executable, "run_connector.py"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=str(ibkr_path)
                )
                
                # Wait briefly to check if process started successfully
                await asyncio.sleep(5)
                
                if self.current_process.poll() is None:
                    # Process is running
                    self.logger.info("IBKR connector started successfully")
                    break
                else:
                    # Process failed to start
                    self.logger.error(f"IBKR connector failed to start (attempt {attempt}/{self.max_retries})")
                    self.current_process = None
                    
                    if attempt < self.max_retries:
                        self.logger.warning(f"Retrying IBKR connection in {self.connection_retry_delay} seconds...")
                        await asyncio.sleep(self.connection_retry_delay)
                    else:
                        self.logger.error("All IBKR connection attempts failed")
                        print("❌ IBKR connection failed after all retries. Please check IBKR Gateway/TWS.")
                        return False
                        
            except Exception as e:
                self.logger.error(f"Error starting IBKR connector (attempt {attempt}): {e}")
                if attempt < self.max_retries:
                    await asyncio.sleep(self.connection_retry_delay)
                else:
                    return False
        
        if not self.current_process:
            return False
        
        try:
            # Calculate end time
            duration_seconds = duration_hours * 3600
            end_time = self._get_ist_time() + timedelta(seconds=duration_seconds)
            
            self.logger.info(f"IBKR data collection will run until {end_time.strftime('%H:%M:%S')}")
            
            # Wait for the specified duration or until stopped
            try:
                await asyncio.wait_for(
                    self._wait_for_process_completion(), 
                    timeout=duration_seconds
                )
            except asyncio.TimeoutError:
                # Normal timeout - stop the process
                self.logger.info("IBKR data collection duration completed, stopping connector...")
                self._stop_current_process()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error during IBKR data collection: {e}")
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
                    self.logger.info("IBKR process stopped gracefully")
                except subprocess.TimeoutExpired:
                    self.current_process.kill()
                    self.current_process.wait()
                    self.logger.info("IBKR process force killed")
            except Exception as e:
                self.logger.error(f"Error stopping IBKR process: {e}")
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
        
        # Handle overnight sessions
        if self.market_start_time > self.market_end_time:
            # If we're targeting the end time and it's an overnight session
            if target_time == self.market_end_time and now.time() >= self.market_start_time:
                # We want next day's end time
                target_datetime += timedelta(days=1)
            elif target_time == self.market_start_time and now.time() <= self.market_end_time:
                # We want today's start time (but it was yesterday, so today is fine)
                pass
            elif target_datetime <= now:
                # Standard case: target time is in the past today, schedule for tomorrow
                target_datetime += timedelta(days=1)
        else:
            # Standard same-day session
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
    
    async def _sleep_until_next_market_open(self):
        """Sleep until the next market opening time (skipping weekends)."""
        await self._sleep_until_time(self.market_start_time)
    
    async def _sleep_until_pre_market_check(self):
        """Sleep until the next pre-market check time (skipping weekends)."""
        await self._sleep_until_time(self.pre_market_check_time)
    
    def _calculate_remaining_market_time(self):
        """Calculate remaining market hours from current time."""
        now = self._get_ist_time()
        current_time = now.time()
        
        # Handle overnight market hours
        if self.market_start_time > self.market_end_time:
            if current_time >= self.market_start_time:
                # We're in today's session, end time is tomorrow
                end_datetime = (now + timedelta(days=1)).replace(
                    hour=self.market_end_time.hour,
                    minute=self.market_end_time.minute,
                    second=0,
                    microsecond=0
                )
            elif current_time <= self.market_end_time:
                # We're in yesterday's session, end time is today
                end_datetime = now.replace(
                    hour=self.market_end_time.hour,
                    minute=self.market_end_time.minute,
                    second=0,
                    microsecond=0
                )
            else:
                return 0  # Outside market hours
        else:
            # Same day session
            if current_time > self.market_end_time:
                return 0  # Market closed
            end_datetime = now.replace(
                hour=self.market_end_time.hour,
                minute=self.market_end_time.minute,
                second=0,
                microsecond=0
            )
        
        remaining_seconds = (end_datetime - now).total_seconds()
        return max(0, remaining_seconds / 3600)  # Convert to hours
    
    async def run(self):
        """Main scheduler loop with smart time-aware startup."""
        self.logger.info("IBKR Scheduler starting...")
        current_time = self._get_ist_time()
        self.logger.info(f"Current IST time: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"Pre-market check time: {self.pre_market_check_time}")
        self.logger.info(f"Market hours: {self.market_start_time} - {self.market_end_time}")
        
        # Track if tables are ready for the session
        tables_ready = False
        
        # Main scheduler loop
        while self.running:
            try:
                # Check if it's weekend
                if self._is_weekend():
                    self.logger.info("Weekend detected, sleeping until next Monday pre-market check...")
                    await self._sleep_until_pre_market_check()
                    tables_ready = False  # Reset for new week
                    continue
                
                # Determine current market phase and act accordingly
                phase = self._get_current_market_phase()
                self.logger.info(f"Current market phase: {phase}")
                
                if phase == "weekend":
                    self.logger.info("Weekend: Sleeping until next Monday pre-market check")
                    await self._sleep_until_pre_market_check()
                    tables_ready = False  # Reset for new week
                    continue
                
                elif phase == "pre_market":
                    self.logger.info("Pre-market: Waiting until pre-market check time")
                    await self._sleep_until_time(self.pre_market_check_time)
                    if not self.running:
                        break
                
                elif phase == "pre_market_check":
                    self.logger.info("=== Pre-market check phase: Verifying QuestDB and tables ===")
                    
                    # Setup tables and verify connection
                    if await self._setup_tables_if_needed():
                        self.logger.info("✅ Pre-market checks passed - QuestDB and tables ready")
                        tables_ready = True
                    else:
                        self.logger.error("❌ Pre-market checks failed - QuestDB/tables not ready")
                        tables_ready = False
                        print("⚠️ ALERT: IBKR pre-market checks failed! Please fix QuestDB/tables before market opens.")
                    
                    # Wait for market open
                    self.logger.info("Pre-market checks completed, waiting for market open...")
                    await self._sleep_until_time(self.market_start_time)
                    if not self.running:
                        break
                
                elif phase == "market_hours":
                    if not tables_ready:
                        self.logger.warning("Market hours started but tables not ready - running emergency check")
                        if not await self._setup_tables_if_needed():
                            self.logger.error("Emergency table check failed - skipping this market session")
                            await self._sleep_until_pre_market_check()
                            continue
                        tables_ready = True
                    
                    self.logger.info("Market hours: Starting data collection")
                    # Calculate how long to run data collection
                    remaining_hours = self._calculate_remaining_market_time()
                    
                    if remaining_hours > 0:
                        self.logger.info(f"=== Starting IBKR data collection for {remaining_hours:.2f} hours ===")
                        collection_success = await self._run_data_collection(duration_hours=remaining_hours)
                        
                        if not collection_success:
                            self.logger.error("IBKR data collection failed")
                    else:
                        self.logger.info("Market hours already over, skipping data collection")
                
                elif phase == "post_market":
                    self.logger.info("Post-market: Sleeping until tomorrow's pre-market check")
                    await self._sleep_until_pre_market_check()
                    tables_ready = False  # Reset for next day
                    continue
                
                # After data collection, sleep until next pre-market check
                if self.running:
                    self.logger.info("=== Daily IBKR cycle completed ===")
                    await self._sleep_until_pre_market_check()
                    tables_ready = False  # Reset for next day
                
            except Exception as e:
                self.logger.error(f"Error in main IBKR scheduler loop: {e}")
                # Wait a bit before retrying to avoid tight loop
                await asyncio.sleep(300)  # 5 minutes
        
        self.logger.info("IBKR Scheduler stopped")


async def main():
    """Main entry point."""
    scheduler = IBKRScheduler()
    
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
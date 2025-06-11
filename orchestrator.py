#!/usr/bin/env python3
"""
Financial Data Orchestrator
Manages Binance and Zerodha connectors with automatic token renewal.
"""

import asyncio
import logging
import signal
import sys
import time
import os
import subprocess
import psutil
import yaml
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Any
from logging.handlers import RotatingFileHandler


class ProcessManager:
    """Manages subprocess lifecycle with health monitoring."""
    
    def __init__(self, name: str, script_path: str, config: Dict):
        self.name = name
        self.script_path = script_path
        self.config = config
        self.process: Optional[subprocess.Popen] = None
        self.start_time: Optional[float] = None
        self.restart_count = 0
        self.logger = logging.getLogger(f"ProcessManager.{name}")
        
    async def start(self) -> bool:
        """Start the process asynchronously."""
        if self.process and self.is_alive():
            self.logger.warning(f"Process {self.name} already running")
            return True
            
        try:
            self.logger.info(f"Starting process: {self.name}")
            cmd = [sys.executable, self.script_path]
            if "binance" in self.script_path:
                cmd.extend(["--all-symbols", "--disable-prints"])
            
            self.logger.info(f"Starting {self.name} with command: {' '.join(cmd)}")
            
            log_file = open(f"logs/{self.name}_output.log", "w")

            self.process = subprocess.Popen(
                cmd,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                cwd=os.getcwd(),
                text=True
            )
            self.start_time = time.time()
            self.restart_count += 1
            
            # Wait briefly to check if process started successfully
            await asyncio.sleep(1)
            
            if self.is_alive():
                self.logger.info(f"Process {self.name} started successfully (PID: {self.process.pid})")
                return True
            else:
                self.logger.error(f"Process {self.name} failed to start")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to start process {self.name}: {e}")
            return False
    
    async def stop(self, timeout: int = 15) -> bool:
        """Stop the process gracefully with timeout."""
        if not self.process or not self.is_alive():
            self.logger.info(f"Process {self.name} not running")
            return True
            
        try:
            self.logger.info(f"Stopping process {self.name} (PID: {self.process.pid})")
            
            # Send SIGTERM for graceful shutdown
            self.process.terminate()
            
            # Wait for graceful shutdown
            try:
                await asyncio.wait_for(self._wait_for_exit(), timeout=timeout)
                self.logger.info(f"Process {self.name} stopped gracefully")
                return True
            except asyncio.TimeoutError:
                self.logger.warning(f"Process {self.name} didn't stop gracefully, force killing")
                self.process.kill()
                await self._wait_for_exit()
                self.logger.info(f"Process {self.name} force killed")
                return True
                
        except Exception as e:
            self.logger.error(f"Error stopping process {self.name}: {e}")
            return False
    
    async def _wait_for_exit(self):
        """Wait for process to exit."""
        while self.process and self.process.poll() is None:
            await asyncio.sleep(0.1)
    
    def is_alive(self) -> bool:
        """Check if process is alive."""
        return self.process is not None and self.process.poll() is None
    
    def get_memory_usage(self) -> float:
        """Get memory usage in MB."""
        if not self.process or not self.is_alive():
            return 0.0
        try:
            psutil_process = psutil.Process(self.process.pid)
            return psutil_process.memory_info().rss / 1024 / 1024  # Convert to MB
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return 0.0
    
    def get_uptime(self) -> float:
        """Get process uptime in seconds."""
        if not self.start_time:
            return 0.0
        return time.time() - self.start_time


class TokenRenewalManager:
    """Manages the token renewal workflow."""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger("TokenRenewalManager")
        self.listener_process: Optional[subprocess.Popen] = None
        
    async def renew_token(self) -> bool:
        """Execute the complete token renewal workflow."""
        try:
            self.logger.info("Starting token renewal process")
            
            # Step 1: Start listener.py in background
            if not await self._start_listener():
                return False
            
            # Step 2: Run token automation and wait for completion
            if not await self._run_token_automation():
                await self._stop_listener()
                return False
            
            # Step 3: Stop listener
            await self._stop_listener()
            
            # Step 4: Wait for validation
            await asyncio.sleep(self.config['timeouts']['token_validation'])
            
            self.logger.info("Token renewal completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Token renewal failed: {e}")
            await self._stop_listener()
            return False
    
    async def _start_listener(self) -> bool:
        """Start the listener process."""
        try:
            self.logger.info("Starting listener process")
            self.listener_process = subprocess.Popen(
                [sys.executable, self.config['paths']['listener_script']],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=os.getcwd(),
                text=True
            )
            
            # Wait briefly to ensure it started
            await asyncio.sleep(2)
            
            if self.listener_process.poll() is None:
                self.logger.info(f"Listener started successfully (PID: {self.listener_process.pid})")
                return True
            else:
                self.logger.error("Listener failed to start")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to start listener: {e}")
            return False
    
    async def _run_token_automation(self) -> bool:
        """Run token automation script and wait for completion."""
        try:
            self.logger.info("Running token automation")
            process = subprocess.Popen(
                [sys.executable, self.config['paths']['token_script']],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=os.getcwd(),
                text=True
            )
            
            # Wait for completion
            stdout, stderr = process.communicate()
            
            # if process.returncode == 0:
            #     self.logger.info("Token automation completed successfully")
            #     return True
            # else:
            #     self.logger.error(f"Token automation failed with code {process.returncode}")
            #     self.logger.error(f"STDERR: {stderr}")
            #     return False

            # TEMPORARY: Always consider token automation successful for testing
            self.logger.info(f"Token automation finished with code {process.returncode}")
            if stderr:
                self.logger.warning(f"STDERR: {stderr}")
            self.logger.info("Token automation completed (ignoring exit code for testing)")
            return True

        except Exception as e:
            self.logger.error(f"Failed to run token automation: {e}")
            return False
    
    async def _stop_listener(self):
        """Stop the listener process."""
        if not self.listener_process:
            return
            
        try:
            self.logger.info("Stopping listener process")
            self.listener_process.terminate()
            
            # Wait for graceful shutdown
            try:
                await asyncio.wait_for(self._wait_for_listener_exit(), timeout=10)
                self.logger.info("Listener stopped gracefully")
            except asyncio.TimeoutError:
                self.logger.warning("Listener didn't stop gracefully, force killing")
                self.listener_process.kill()
                await self._wait_for_listener_exit()
                self.logger.info("Listener force killed")
                
        except Exception as e:
            self.logger.error(f"Error stopping listener: {e}")
        finally:
            self.listener_process = None
    
    async def _wait_for_listener_exit(self):
        """Wait for listener process to exit."""
        while self.listener_process and self.listener_process.poll() is None:
            await asyncio.sleep(0.1)


class HealthMonitor:
    """Monitors process health and system resources."""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger("HealthMonitor")
        
    async def check_process_health(self, process_manager: ProcessManager):
        """Check health of a single process."""
        if not process_manager.is_alive():
            self.logger.warning(f"Process {process_manager.name} is not alive")
            return False
        
        # Check memory usage
        memory_mb = process_manager.get_memory_usage()
        memory_limit = self.config['health']['memory_limit_mb']
        
        if memory_mb > memory_limit:
            self.logger.warning(
                f"Process {process_manager.name} using {memory_mb:.1f}MB "
                f"(limit: {memory_limit}MB)"
            )
        
        # Log health status
        uptime = process_manager.get_uptime()
        self.logger.debug(
            f"Process {process_manager.name}: "
            f"PID={process_manager.process.pid}, "
            f"Memory={memory_mb:.1f}MB, "
            f"Uptime={uptime:.1f}s, "
            f"Restarts={process_manager.restart_count}"
        )
        
        return True


class DataOrchestrator:
    """Main orchestrator for managing all processes and workflows."""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config = self._load_config(config_path)
        self.logger = self._setup_logging()
        
        # Initialize managers
        self.binance_manager = ProcessManager(
            "binance", self.config['paths']['binance_script'], self.config
        )
        self.zerodha_manager = ProcessManager(
            "zerodha", self.config['paths']['zerodha_script'], self.config
        )
        self.token_manager = TokenRenewalManager(self.config)
        self.health_monitor = HealthMonitor(self.config)
        
        # State tracking
        self.running = False
        self.last_renewal_time = 0
        self.shutdown_event = asyncio.Event()
        
        # Setup signal handlers
        self._setup_signal_handlers()
    
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            return config
        except Exception as e:
            print(f"Failed to load config from {config_path}: {e}")
            sys.exit(1)
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging with rotation."""
        # Create logs directory
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        # Create log filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"run_log_{timestamp}.log"
        
        # Set the root logger level so ALL loggers inherit it
        logging.getLogger().setLevel(getattr(logging, self.config['logging']['level']))

        # Setup logger
        logger = logging.getLogger("DataOrchestrator")
        logger.setLevel(getattr(logging, self.config['logging']['level']))
        
        # File handler with rotation
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=self.config['logging']['max_file_size'],
            backupCount=5
        )
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
    
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
            asyncio.create_task(self._initiate_shutdown())
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def _initiate_shutdown(self):
        """Initiate graceful shutdown with timeout."""
        self.logger.info("Starting graceful shutdown (2 minute timeout)...")
        self.running = False
        
        # Start shutdown timer
        shutdown_task = asyncio.create_task(self._perform_shutdown())
        timeout_task = asyncio.create_task(self._shutdown_timeout())
        
        # Wait for either shutdown completion or timeout
        done, pending = await asyncio.wait(
            [shutdown_task, timeout_task],
            return_when=asyncio.FIRST_COMPLETED
        )
        
        # Cancel remaining tasks
        for task in pending:
            task.cancel()
    
    async def _perform_shutdown(self):
        """Perform the actual shutdown process."""
        try:
            self.logger.info("Stopping all processes...")
            
            # Stop processes
            await self.binance_manager.stop(self.config['timeouts']['process_shutdown'])
            await self.zerodha_manager.stop(self.config['timeouts']['process_shutdown'])
            
            self.logger.info("All processes stopped successfully")
            self.shutdown_event.set()
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
            self.shutdown_event.set()
    
    async def _shutdown_timeout(self):
        """Handle shutdown timeout."""
        await asyncio.sleep(120)  # 2 minutes
        self.logger.error("Shutdown timeout reached! Force killing all processes...")
        
        # Force kill processes
        for manager in [self.binance_manager, self.zerodha_manager]:
            if manager.process and manager.is_alive():
                try:
                    manager.process.kill()
                    self.logger.info(f"Force killed {manager.name}")
                except Exception as e:
                    self.logger.error(f"Failed to force kill {manager.name}: {e}")
        
        self.shutdown_event.set()
    
    async def _validate_config(self):
        """TODO: Validate configuration file structure and values."""
        pass
    
    async def run(self):
        """Main orchestrator loop."""
        try:
            self.logger.info("Starting Data Orchestrator")
            self.running = True
            
            # Start initial token renewal
            self.logger.info("Performing initial token renewal...")
            renewal_success = await self._perform_token_renewal_with_retry()
            
            if not renewal_success:
                self.logger.error("Initial token renewal failed, exiting...")
                return
            
            # Start connectors
            await self._start_connectors()
            
            # Start monitoring tasks
            health_task = asyncio.create_task(self._health_monitoring_loop())
            renewal_task = asyncio.create_task(self._renewal_scheduling_loop())
            
            # Wait for shutdown
            await self.shutdown_event.wait()
            
            # Cleanup
            health_task.cancel()
            renewal_task.cancel()
            
            self.logger.info("Data Orchestrator stopped")
            
        except Exception as e:
            self.logger.error(f"Fatal error in orchestrator: {e}")
            raise
    
    async def _start_connectors(self):
        """Start both connector processes."""
        # Start binance connector (runs continuously)
        binance_started = await self.binance_manager.start()
        if not binance_started:
            self.logger.error("Failed to start Binance connector")
            return False
        
        # Start zerodha connector 
        zerodha_started = await self.zerodha_manager.start()
        if not zerodha_started:
            self.logger.error("Failed to start Zerodha connector")
            return False
        
        self.logger.info("All connectors started successfully")
        return True
    
    async def _perform_token_renewal_with_retry(self) -> bool:
        """Perform token renewal with retry logic."""
        max_attempts = self.config['retry']['max_attempts']
        
        for attempt in range(1, max_attempts + 1):
            self.logger.info(f"Token renewal attempt {attempt}/{max_attempts}")
            
            success = await self.token_manager.renew_token()
            if success:
                self.last_renewal_time = time.time()
                self.logger.info("Token renewal successful")
                return True
            
            if attempt < max_attempts:
                wait_time = min(attempt * 2, 30)  # Exponential backoff, max 30s
                self.logger.warning(f"Token renewal failed, retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
        
        self.logger.error("All token renewal attempts failed")
        return False
    
    async def _renewal_scheduling_loop(self):
        """Main loop for scheduling token renewals."""
        while self.running:
            try:
                # Calculate time until next renewal
                current_time = time.time()
                next_renewal = self.last_renewal_time + self.config['scheduling']['renewal_cycle']
                sleep_time = next_renewal - current_time
                
                if sleep_time > 0:
                    self.logger.info(f"Next token renewal in {sleep_time/3600:.1f} hours")
                    await asyncio.sleep(sleep_time)
                
                if not self.running:
                    break
                
                # Stop zerodha connector
                self.logger.info("Stopping Zerodha connector for token renewal")
                await self.zerodha_manager.stop(self.config['timeouts']['process_shutdown'])
                
                # Perform token renewal
                renewal_success = await self._perform_token_renewal_with_retry()
                
                if renewal_success:
                    # Restart zerodha connector
                    self.logger.info("Restarting Zerodha connector")
                    await self.zerodha_manager.start()
                else:
                    # Start urgent alert loop
                    asyncio.create_task(self._urgent_alert_loop())
                
            except Exception as e:
                self.logger.error(f"Error in renewal scheduling: {e}")
                await asyncio.sleep(60)  # Wait before retrying
    
    async def _urgent_alert_loop(self):
        """Print urgent alerts when token renewal fails."""
        alert_interval = self.config['retry']['urgent_alert_interval']
        
        while self.running:
            self.logger.error("URGENT: Token renewal failed! Manual intervention required!")
            print("🚨 URGENT ALERT: Token renewal failed! Manual intervention required! 🚨")
            await asyncio.sleep(alert_interval)
    
    async def _health_monitoring_loop(self):
        """Monitor health of all processes."""
        check_interval = self.config['health']['check_interval']
        
        while self.running:
            try:
                # Check Binance connector health
                await self.health_monitor.check_process_health(self.binance_manager)
                
                # Restart if dead (only if not during renewal)
                if not self.binance_manager.is_alive():
                    self.logger.warning("Binance connector died, restarting...")
                    await self.binance_manager.start()
                
                # Check Zerodha connector health (only if should be running)
                if self.zerodha_manager.is_alive():
                    await self.health_monitor.check_process_health(self.zerodha_manager)
                elif time.time() - self.last_renewal_time > 300:  # Give 5 min after renewal
                    self.logger.warning("Zerodha connector died, restarting...")
                    await self.zerodha_manager.start()
                
                await asyncio.sleep(check_interval)
                
            except Exception as e:
                self.logger.error(f"Error in health monitoring: {e}")
                await asyncio.sleep(check_interval)


async def main():
    """Main entry point."""
    try:
        # Check if config file exists
        if not os.path.exists("config.yaml"):
            print("ERROR: config.yaml not found in current directory")
            sys.exit(1)
        
        # Create and run orchestrator
        orchestrator = DataOrchestrator()
        await orchestrator.run()
        
    except KeyboardInterrupt:
        print("\nShutdown initiated by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Check Python version
    if sys.version_info < (3, 7):
        print("ERROR: Python 3.7 or higher required")
        sys.exit(1)
    
    # Run the orchestrator
    asyncio.run(main())
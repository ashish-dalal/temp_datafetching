#!/usr/bin/env python3
"""
Financial Data Orchestrator
Simple process manager for Binance, Zerodha, and IBKR connectors.
Each connector handles its own scheduling internally.
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
from typing import Dict, Optional
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
    """Main orchestrator for managing all processes."""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config = self._load_config(config_path)
        self.logger = self._setup_logging()
        
        # Initialize process managers
        self.binance_manager = ProcessManager(
            "binance", self.config['paths']['binance_script'], self.config
        )
        self.zerodha_manager = ProcessManager(
            "zerodha", self.config['paths']['zerodha_script'], self.config
        )
        self.ibkr_manager = ProcessManager(
            "ibkr", self.config['paths']['ibkr_script'], self.config
        )
        self.health_monitor = HealthMonitor(self.config)
        
        # State tracking
        self.running = False
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
        log_file = log_dir / f"orchestrator_{timestamp}.log"
        
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
            await self.ibkr_manager.stop(self.config['timeouts']['process_shutdown'])
            
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
        for manager in [self.binance_manager, self.zerodha_manager, self.ibkr_manager]:
            if manager.process and manager.is_alive():
                try:
                    manager.process.kill()
                    self.logger.info(f"Force killed {manager.name}")
                except Exception as e:
                    self.logger.error(f"Failed to force kill {manager.name}: {e}")
        
        self.shutdown_event.set()
    
    async def run(self):
        """Main orchestrator loop."""
        try:
            self.logger.info("Starting Data Orchestrator")
            self.running = True
            
            # Start all connectors
            await self._start_connectors()
            
            # Start monitoring task
            health_task = asyncio.create_task(self._health_monitoring_loop())
            
            # Wait for shutdown
            await self.shutdown_event.wait()
            
            # Cleanup
            health_task.cancel()
            
            self.logger.info("Data Orchestrator stopped")
            
        except Exception as e:
            self.logger.error(f"Fatal error in orchestrator: {e}")
            raise
    
    async def _start_connectors(self):
        """Start all connector processes."""
        # Start binance connector (runs continuously)
        binance_started = await self.binance_manager.start()
        if not binance_started:
            self.logger.error("Failed to start Binance connector")
        else:
            self.logger.info("Binance connector started successfully")
        
        # Start zerodha scheduler (handles its own timing)
        zerodha_started = await self.zerodha_manager.start()
        if not zerodha_started:
            self.logger.error("Failed to start Zerodha scheduler")
        else:
            self.logger.info("Zerodha scheduler started successfully")
        
        # Start IBKR scheduler (handles its own timing)
        ibkr_started = await self.ibkr_manager.start()
        if not ibkr_started:
            self.logger.error("Failed to start IBKR scheduler")
        else:
            self.logger.info("IBKR scheduler started successfully")
        
        self.logger.info("All connectors startup attempted")
    
    async def _health_monitoring_loop(self):
        """Monitor health of all processes."""
        check_interval = self.config['health']['check_interval']
        
        while self.running:
            try:
                # Check Binance connector health
                if self.binance_manager.is_alive():
                    await self.health_monitor.check_process_health(self.binance_manager)
                else:
                    self.logger.warning("Binance connector died, restarting...")
                    await self.binance_manager.start()
                
                # Check Zerodha scheduler health  
                if self.zerodha_manager.is_alive():
                    await self.health_monitor.check_process_health(self.zerodha_manager)
                else:
                    self.logger.warning("Zerodha scheduler died, restarting...")
                    await self.zerodha_manager.start()
                
                # Check IBKR scheduler health
                if self.ibkr_manager.is_alive():
                    await self.health_monitor.check_process_health(self.ibkr_manager)
                else:
                    self.logger.warning("IBKR scheduler died, restarting...")
                    await self.ibkr_manager.start()
                
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
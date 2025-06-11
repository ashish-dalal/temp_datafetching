import shutil
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def copy_metrics(source="metrics.json", destination="metrics_copy.json"):
    try:
        if os.path.exists(source):
            shutil.copy2(source, destination)
            logger.info(f"Copied {source} to {destination}")
        else:
            logger.error(f"Source file {source} does not exist")
    except Exception as e:
        logger.error(f"Error copying metrics file: {e}")

if __name__ == "__main__":
    copy_metrics()
    
    
    
from utils.logger import get_logger

logger = get_logger("ingestion")

logger.info("Pipeline started")
logger.info("Extracting data...")
logger.error("Failed to download file")
logger.info("Pipeline completed")
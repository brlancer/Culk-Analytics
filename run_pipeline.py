"""
Main orchestration script for Culk Analytics ELT pipeline.

This script coordinates the extraction and loading of data from all sources.
Each source is processed independently - no assumptions about relationships.
Failures in one source don't stop the execution of other sources.
"""

import logging
from datetime import datetime

# Import implemented extraction functions
from ingestion.shopify import load_to_postgres as load_shopify
from ingestion.faire import load_to_postgres as load_faire
from ingestion.shiphero import load_to_postgres as load_shiphero

# TODO: Import remaining sources when implemented
# from ingestion.loop_returns import load_to_postgres as load_loop
# from ingestion.meta_ads import load_to_postgres as load_meta
# from ingestion.google_ads import load_to_postgres as load_google
# from ingestion.airtable import load_to_postgres as load_airtable


def setup_logging():
    """Configure logging to file and console."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/pipeline_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def main():
    """
    Main pipeline orchestration.
    
    Executes extraction and loading for all data sources.
    Each source is independent - failures are logged but don't stop the pipeline.
    """
    logger = setup_logging()
    logger.info("="*60)
    logger.info("Starting Culk Analytics ELT pipeline")
    logger.info("="*60)
    
    sources = []
    failed_sources = []
    
    # Shopify (commerce hub: B2B + DTC)
    try:
        logger.info("\n[1/3] Extracting Shopify data...")
        load_shopify()
        sources.append("Shopify")
        logger.info("✅ Shopify load completed successfully")
    except Exception as e:
        logger.error(f"❌ Shopify load failed: {e}", exc_info=True)
        failed_sources.append("Shopify")
    
    # Faire (wholesale orders)
    try:
        logger.info("\n[2/3] Extracting Faire data...")
        load_faire()
        sources.append("Faire")
        logger.info("✅ Faire load completed successfully")
    except Exception as e:
        logger.error(f"❌ Faire load failed: {e}", exc_info=True)
        failed_sources.append("Faire")
    
    # ShipHero (3PL inventory/shipments)
    try:
        logger.info("\n[3/3] Extracting ShipHero data...")
        load_shiphero()
        sources.append("ShipHero")
        logger.info("✅ ShipHero load completed successfully")
    except Exception as e:
        logger.error(f"❌ ShipHero load failed: {e}", exc_info=True)
        failed_sources.append("ShipHero")
    
    # TODO: Add remaining sources when implemented
    # - Loop Returns (returns tracking)
    # - Meta Ads (ad spend)
    # - Google Ads (ad spend)
    # - Airtable (product master)
    
    logger.info("\n" + "="*60)
    logger.info("Pipeline execution completed")
    logger.info(f"Successfully loaded: {len(sources)} sources")
    if sources:
        logger.info(f"  - {', '.join(sources)}")
    if failed_sources:
        logger.warning(f"Failed: {len(failed_sources)} sources")
        logger.warning(f"  - {', '.join(failed_sources)}")
    logger.info("="*60)


if __name__ == "__main__":
    main()

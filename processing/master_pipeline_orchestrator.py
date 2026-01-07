"""
Master Pipeline Orchestrator - Coordinates all data pipeline executions
Follows standardized pipeline framework and naming conventions
Manages dependencies and execution order between different data sources
"""
from __future__ import annotations
import os
import sys
import logging
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# Add processing modules to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from processing.common.base_pipeline import create_standard_config, TableNamingHelper
from processing.common.spark_session import get_spark, setup_standard_databases
from processing.silver.business_pipeline import BusinessPipeline
from processing.silver.review_pipeline import ReviewPipeline
from processing.silver.user_pipeline import UserPipeline
from processing.silver.checkin_pipeline import CheckinPipeline
from processing.silver.tip_pipeline import TipPipeline

logger = logging.getLogger(__name__)


class PipelineExecutionStatus:
    """Track pipeline execution status and results"""
    
    def __init__(self, pipeline_name: str):
        self.pipeline_name = pipeline_name
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.status = "PENDING"  # PENDING, RUNNING, SUCCESS, FAILED
        self.error_message: Optional[str] = None
        self.records_processed: int = 0
        self.execution_time_seconds: float = 0.0
    
    def start(self):
        self.start_time = datetime.now()
        self.status = "RUNNING"
        logger.info(f"Pipeline {self.pipeline_name} started at {self.start_time}")
    
    def success(self, records_processed: int = 0):
        self.end_time = datetime.now()
        self.status = "SUCCESS"
        self.records_processed = records_processed
        if self.start_time:
            self.execution_time_seconds = (self.end_time - self.start_time).total_seconds()
        logger.info(f"Pipeline {self.pipeline_name} completed successfully in "
                   f"{self.execution_time_seconds:.2f}s with {records_processed:,} records")
    
    def failed(self, error_message: str):
        self.end_time = datetime.now()
        self.status = "FAILED"
        self.error_message = error_message
        if self.start_time:
            self.execution_time_seconds = (self.end_time - self.start_time).total_seconds()
        logger.error(f"Pipeline {self.pipeline_name} failed after "
                    f"{self.execution_time_seconds:.2f}s: {error_message}")
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "status": self.status,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "execution_time_seconds": self.execution_time_seconds,
            "records_processed": self.records_processed,
            "error_message": self.error_message
        }


class MasterPipelineOrchestrator:
    """
    Master orchestrator for all YELP data pipelines
    Manages execution order, dependencies, and monitoring
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or create_standard_config()
        self.execution_statuses: Dict[str, PipelineExecutionStatus] = {}
        
        # Pipeline execution order (based on dependencies)
        self.pipeline_execution_order = [
            # Independent pipelines (no dependencies)
            ["business", "user"],  # Can run in parallel
            ["review", "checkin", "tip"]  # Depend on business and user, can run in parallel
        ]
        
        self.pipeline_classes = {
            "business": BusinessPipeline,
            "review": ReviewPipeline, 
            "user": UserPipeline,
            "checkin": CheckinPipeline,
            "tip": TipPipeline
        }
        
        # Initialize execution status tracking
        for pipeline_name in self.pipeline_classes.keys():
            self.execution_statuses[pipeline_name] = PipelineExecutionStatus(pipeline_name)
    
    def setup_environment(self):
        """Setup the data environment and databases"""
        logger.info("Setting up data environment")
        
        try:
            # Initialize Spark for setup
            spark = get_spark("Environment Setup")
            
            # Setup standard database structure
            setup_standard_databases(spark, self.config["base_path"])
            
            # Create necessary directories
            base_path = self.config["base_path"]
            layers = ["bronze", "silver", "gold"]
            tables = ["business", "review", "user", "checkin", "tip"]
            
            for layer in layers:
                for table in tables:
                    table_path = TableNamingHelper.get_layer_path(base_path, layer, table)
                    os.makedirs(table_path, exist_ok=True)
                    logger.info(f"Created directory: {table_path}")
            
            spark.stop()
            logger.info("Environment setup completed successfully")
            
        except Exception as e:
            logger.error(f"Environment setup failed: {e}")
            raise
    
    def execute_pipeline(self, pipeline_name: str) -> PipelineExecutionStatus:
        """Execute a single pipeline and return its status"""
        status = self.execution_statuses[pipeline_name]
        status.start()
        
        try:
            pipeline_class = self.pipeline_classes[pipeline_name]
            pipeline = pipeline_class(self.config)
            
            # Run Bronze -> Silver
            logger.info(f"Starting Bronze -> Silver for {pipeline_name}")
            bronze_silver_result = pipeline.run_bronze_to_silver()
            records_count = bronze_silver_result.count() if bronze_silver_result else 0
            
            # Run Silver -> Gold
            logger.info(f"Starting Silver -> Gold for {pipeline_name}")
            pipeline.run_silver_to_gold()
            
            status.success(records_count)
            
        except Exception as e:
            status.failed(str(e))
            logger.exception(f"Pipeline {pipeline_name} execution failed")
        
        return status
    
    def execute_parallel_pipelines(self, pipeline_names: List[str]) -> Dict[str, PipelineExecutionStatus]:
        """Execute multiple pipelines in parallel"""
        logger.info(f"Starting parallel execution of pipelines: {pipeline_names}")
        
        results = {}
        with ThreadPoolExecutor(max_workers=min(len(pipeline_names), 4)) as executor:
            # Submit all pipeline executions
            future_to_pipeline = {
                executor.submit(self.execute_pipeline, name): name 
                for name in pipeline_names
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_pipeline):
                pipeline_name = future_to_pipeline[future]
                try:
                    status = future.result()
                    results[pipeline_name] = status
                except Exception as e:
                    logger.error(f"Pipeline {pipeline_name} failed with exception: {e}")
                    status = self.execution_statuses[pipeline_name]
                    status.failed(str(e))
                    results[pipeline_name] = status
        
        return results
    
    def run_full_pipeline(self, skip_environment_setup: bool = False) -> Dict[str, Any]:
        """
        Run the complete pipeline orchestration
        Returns execution summary
        """
        logger.info("Starting full pipeline orchestration")
        overall_start_time = datetime.now()
        
        try:
            # Setup environment if needed
            if not skip_environment_setup:
                self.setup_environment()
            
            # Execute pipelines in dependency order
            for stage, pipeline_names in enumerate(self.pipeline_execution_order):
                logger.info(f"Executing pipeline stage {stage + 1}: {pipeline_names}")
                
                stage_results = self.execute_parallel_pipelines(pipeline_names)
                
                # Check for failures in this stage
                failed_pipelines = [
                    name for name, status in stage_results.items() 
                    if status.status == "FAILED"
                ]
                
                if failed_pipelines:
                    logger.error(f"Stage {stage + 1} failed. Failed pipelines: {failed_pipelines}")
                    # Continue with other stages but log the failures
                    for name in failed_pipelines:
                        logger.error(f"Pipeline {name} failure details: "
                                   f"{stage_results[name].error_message}")
            
            overall_end_time = datetime.now()
            total_execution_time = (overall_end_time - overall_start_time).total_seconds()
            
            # Generate execution summary
            summary = self.generate_execution_summary(total_execution_time)
            
            logger.info("Full pipeline orchestration completed")
            return summary
            
        except Exception as e:
            logger.error(f"Full pipeline orchestration failed: {e}")
            raise
    
    def generate_execution_summary(self, total_execution_time: float) -> Dict[str, Any]:
        """Generate a comprehensive execution summary"""
        
        successful_pipelines = [
            name for name, status in self.execution_statuses.items()
            if status.status == "SUCCESS"
        ]
        
        failed_pipelines = [
            name for name, status in self.execution_statuses.items()
            if status.status == "FAILED"
        ]
        
        total_records_processed = sum(
            status.records_processed for status in self.execution_statuses.values()
        )
        
        summary = {
            "execution_timestamp": datetime.now().isoformat(),
            "total_execution_time_seconds": total_execution_time,
            "total_pipelines": len(self.pipeline_classes),
            "successful_pipelines": len(successful_pipelines),
            "failed_pipelines": len(failed_pipelines),
            "total_records_processed": total_records_processed,
            "pipeline_details": {
                name: status.to_dict() 
                for name, status in self.execution_statuses.items()
            },
            "summary_stats": {
                "avg_execution_time": sum(
                    s.execution_time_seconds for s in self.execution_statuses.values()
                ) / len(self.execution_statuses) if self.execution_statuses else 0,
                "fastest_pipeline": min(
                    self.execution_statuses.items(),
                    key=lambda x: x[1].execution_time_seconds
                )[0] if successful_pipelines else None,
                "slowest_pipeline": max(
                    self.execution_statuses.items(),
                    key=lambda x: x[1].execution_time_seconds
                )[0] if successful_pipelines else None
            }
        }
        
        # Log summary
        logger.info("="*60)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("="*60)
        logger.info(f"Total Execution Time: {total_execution_time:.2f} seconds")
        logger.info(f"Successful Pipelines: {len(successful_pipelines)}/{len(self.pipeline_classes)}")
        logger.info(f"Total Records Processed: {total_records_processed:,}")
        
        if successful_pipelines:
            logger.info(f"Successful: {', '.join(successful_pipelines)}")
        
        if failed_pipelines:
            logger.info(f"Failed: {', '.join(failed_pipelines)}")
            for name in failed_pipelines:
                logger.error(f"  {name}: {self.execution_statuses[name].error_message}")
        
        logger.info("="*60)
        
        return summary
    
    def save_execution_report(self, summary: Dict[str, Any], output_path: str = None):
        """Save execution report to JSON file"""
        if not output_path:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"pipeline_execution_report_{timestamp}.json"
        
        try:
            with open(output_path, 'w') as f:
                json.dump(summary, f, indent=2)
            logger.info(f"Execution report saved to {output_path}")
        except Exception as e:
            logger.error(f"Failed to save execution report: {e}")


def main():
    """Main execution function for master pipeline orchestrator"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'pipeline_orchestrator_{datetime.now().strftime("%Y%m%d")}.log'),
            logging.StreamHandler()
        ]
    )
    
    try:
        # Create orchestrator with default config
        orchestrator = MasterPipelineOrchestrator()
        
        # Run full pipeline
        summary = orchestrator.run_full_pipeline()
        
        # Save execution report
        orchestrator.save_execution_report(summary)
        
        logger.info("Master pipeline orchestration completed successfully")
        
    except Exception as e:
        logger.error(f"Master pipeline orchestration failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

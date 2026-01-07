"""
Partition Strategy Analyzer and Optimizer
Analyzes table partitioning patterns and recommends optimal strategies
Helps avoid partition skew and improves query performance
"""
from __future__ import annotations
import os
import logging
from typing import Dict, Any, List, Tuple, Optional
from dataclasses import dataclass
import json
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

from processing.common.spark_session import get_spark
from processing.common.config_manager import get_config_manager

logger = logging.getLogger(__name__)


@dataclass
class PartitionAnalysis:
    """Data class for partition analysis results"""
    table_name: str
    current_partitions: List[str]
    partition_count: int
    avg_partition_size_mb: float
    max_partition_size_mb: float
    min_partition_size_mb: float
    skew_ratio: float  # max/avg
    recommended_partitions: List[str]
    optimization_benefit: str
    analysis_timestamp: datetime


class PartitionStrategyAnalyzer:
    """
    Analyze and optimize partition strategies for Delta tables
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or get_config_manager().get_standard_config()
        self.spark = get_spark("Partition Strategy Analyzer")
        
        # Partition strategy recommendations by table type
        self.partition_recommendations = {
            'business': {
                'primary': ['state', 'city'],  # Geographic distribution
                'secondary': ['category_primary'],  # Business type
                'avoid': ['business_id'],  # High cardinality
                'reasoning': "Geographic partitioning for location-based queries"
            },
            'review': {
                'primary': ['year', 'month'],  # Temporal partitioning
                'secondary': ['year', 'month', 'rating_category'],
                'avoid': ['review_id', 'user_id', 'business_id'],
                'reasoning': "Temporal partitioning for time-range queries and archival"
            },
            'user': {
                'primary': ['user_tier'],  # User segmentation
                'secondary': ['yelping_since_year', 'user_tier'],
                'avoid': ['user_id'],
                'reasoning': "User tier partitioning for analytics and targeted queries"
            },
            'checkin': {
                'primary': ['year', 'month'],  # Temporal partitioning
                'secondary': ['year', 'season'],
                'avoid': ['business_id', 'checkin_id'],
                'reasoning': "Temporal partitioning for seasonal analysis"
            },
            'tip': {
                'primary': ['year', 'month'],  # Temporal partitioning
                'secondary': ['year', 'tip_type'],
                'avoid': ['tip_id', 'user_id', 'business_id'],
                'reasoning': "Temporal partitioning with content type secondary"
            }
        }
    
    def analyze_current_partitions(self, table_path: str, table_name: str) -> PartitionAnalysis:
        """Analyze current partition structure and performance"""
        try:
            # Check if table exists and is partitioned
            if not os.path.exists(table_path):
                logger.warning(f"Table path does not exist: {table_path}")
                return self._create_empty_analysis(table_name)
            
            # Read table metadata
            df = self.spark.read.format("delta").load(table_path)
            
            # Get partition columns from Delta metadata
            delta_log_path = os.path.join(table_path, "_delta_log")
            current_partitions = self._get_delta_partition_columns(table_path)
            
            if not current_partitions:
                logger.info(f"Table {table_name} is not partitioned")
                return self._analyze_unpartitioned_table(table_path, table_name, df)
            
            # Analyze partition distribution
            partition_stats = self._analyze_partition_distribution(df, current_partitions)
            
            # Get recommended partitions
            recommended_partitions = self._get_recommended_partitions(table_name, df)
            
            # Calculate optimization benefit
            optimization_benefit = self._calculate_optimization_benefit(
                current_partitions, recommended_partitions, partition_stats
            )
            
            analysis = PartitionAnalysis(
                table_name=table_name,
                current_partitions=current_partitions,
                partition_count=partition_stats['count'],
                avg_partition_size_mb=partition_stats['avg_size_mb'],
                max_partition_size_mb=partition_stats['max_size_mb'],
                min_partition_size_mb=partition_stats['min_size_mb'],
                skew_ratio=partition_stats['skew_ratio'],
                recommended_partitions=recommended_partitions,
                optimization_benefit=optimization_benefit,
                analysis_timestamp=datetime.now()
            )
            
            logger.info(f"Partition analysis completed for {table_name}")
            return analysis
            
        except Exception as e:
            logger.error(f"Partition analysis failed for {table_name}: {str(e)}")
            return self._create_empty_analysis(table_name)
    
    def _get_delta_partition_columns(self, table_path: str) -> List[str]:
        """Extract partition columns from Delta table metadata"""
        try:
            # Use Delta table API to get partition columns
            from delta.tables import DeltaTable
            delta_table = DeltaTable.forPath(self.spark, table_path)
            
            # Get table schema and find partitioned columns
            schema = delta_table.toDF().schema
            
            # Check for partition information in Delta history
            history_df = delta_table.history()
            
            # For now, return empty list as we need to implement proper Delta metadata reading
            # This would require Delta Lake Python APIs
            return []
            
        except Exception as e:
            logger.debug(f"Could not read Delta partition metadata: {str(e)}")
            return []
    
    def _analyze_partition_distribution(self, df: DataFrame, partition_cols: List[str]) -> Dict[str, float]:
        """Analyze the distribution across partitions"""
        if not partition_cols:
            return {
                'count': 1,
                'avg_size_mb': 0.0,
                'max_size_mb': 0.0,
                'min_size_mb': 0.0,
                'skew_ratio': 1.0
            }
        
        # Group by partition columns and count records
        partition_counts = df.groupBy(*partition_cols).count()
        
        # Calculate statistics
        stats = partition_counts.agg(
            count("*").alias("partition_count"),
            avg("count").alias("avg_count"),
            max("count").alias("max_count"),
            min("count").alias("min_count")
        ).collect()[0]
        
        # Estimate sizes (rough approximation)
        # In production, this would use actual file sizes from Delta metadata
        estimated_avg_size_mb = float(stats.avg_count) * 0.001  # Rough estimate
        estimated_max_size_mb = float(stats.max_count) * 0.001
        estimated_min_size_mb = float(stats.min_count) * 0.001
        
        skew_ratio = float(stats.max_count) / float(stats.avg_count) if stats.avg_count > 0 else 1.0
        
        return {
            'count': stats.partition_count,
            'avg_size_mb': estimated_avg_size_mb,
            'max_size_mb': estimated_max_size_mb,
            'min_size_mb': estimated_min_size_mb,
            'skew_ratio': skew_ratio
        }
    
    def _analyze_unpartitioned_table(self, table_path: str, table_name: str, df: DataFrame) -> PartitionAnalysis:
        """Analyze an unpartitioned table and recommend partitioning"""
        total_count = df.count()
        
        # Get recommended partitions
        recommended_partitions = self._get_recommended_partitions(table_name, df)
        
        return PartitionAnalysis(
            table_name=table_name,
            current_partitions=[],
            partition_count=1,
            avg_partition_size_mb=total_count * 0.001,  # Rough estimate
            max_partition_size_mb=total_count * 0.001,
            min_partition_size_mb=total_count * 0.001,
            skew_ratio=1.0,
            recommended_partitions=recommended_partitions,
            optimization_benefit="High - Table is not partitioned",
            analysis_timestamp=datetime.now()
        )
    
    def _get_recommended_partitions(self, table_name: str, df: DataFrame) -> List[str]:
        """Get recommended partition strategy for a table"""
        table_base = table_name.split('.')[-1]  # Remove layer prefix
        
        if table_base not in self.partition_recommendations:
            return []
        
        recommendations = self.partition_recommendations[table_base]
        
        # Check if recommended columns exist in the DataFrame
        available_cols = set(df.columns)
        
        # Try primary recommendation first
        primary_cols = recommendations['primary']
        if all(col in available_cols for col in primary_cols):
            return primary_cols
        
        # Try secondary recommendation
        secondary_cols = recommendations['secondary']
        if all(col in available_cols for col in secondary_cols):
            return secondary_cols
        
        # Return subset that exists
        return [col for col in primary_cols if col in available_cols]
    
    def _calculate_optimization_benefit(self, current: List[str], recommended: List[str], 
                                      stats: Dict[str, float]) -> str:
        """Calculate the optimization benefit of recommended partitioning"""
        if not current and recommended:
            return "High - Adding partitioning will significantly improve query performance"
        
        if current == recommended:
            if stats['skew_ratio'] > 10:
                return "Medium - Partition skew detected, consider repartitioning"
            else:
                return "Low - Current partitioning strategy is optimal"
        
        if current and recommended and set(current) != set(recommended):
            return "Medium - Different partitioning strategy recommended"
        
        return "Unknown - Unable to determine benefit"
    
    def _create_empty_analysis(self, table_name: str) -> PartitionAnalysis:
        """Create an empty analysis for error cases"""
        return PartitionAnalysis(
            table_name=table_name,
            current_partitions=[],
            partition_count=0,
            avg_partition_size_mb=0.0,
            max_partition_size_mb=0.0,
            min_partition_size_mb=0.0,
            skew_ratio=0.0,
            recommended_partitions=[],
            optimization_benefit="Unknown - Analysis failed",
            analysis_timestamp=datetime.now()
        )
    
    def analyze_all_tables(self) -> List[PartitionAnalysis]:
        """Analyze partition strategies for all tables"""
        config_manager = get_config_manager()
        
        tables_to_analyze = [
            ('silver', 'business'),
            ('silver', 'review'),
            ('silver', 'user'),
            ('silver', 'checkin'),
            ('silver', 'tip'),
            ('gold', 'business_metrics'),
            ('gold', 'review_metrics'),
            ('gold', 'user_metrics'),
        ]
        
        analyses = []
        
        for layer, table in tables_to_analyze:
            table_path = config_manager.data_config.get_table_path(layer, table)
            full_table_name = f"{layer}.{table}"
            
            logger.info(f"Analyzing partition strategy for {full_table_name}")
            analysis = self.analyze_current_partitions(table_path, full_table_name)
            analyses.append(analysis)
        
        return analyses
    
    def generate_optimization_report(self, analyses: List[PartitionAnalysis]) -> Dict[str, Any]:
        """Generate a comprehensive optimization report"""
        report = {
            'analysis_timestamp': datetime.now().isoformat(),
            'total_tables_analyzed': len(analyses),
            'tables': [],
            'summary': {
                'unpartitioned_tables': 0,
                'optimally_partitioned': 0,
                'needs_optimization': 0,
                'high_skew_tables': 0
            },
            'recommendations': []
        }
        
        for analysis in analyses:
            # Add to table details
            table_info = {
                'table_name': analysis.table_name,
                'current_partitions': analysis.current_partitions,
                'partition_count': analysis.partition_count,
                'skew_ratio': analysis.skew_ratio,
                'recommended_partitions': analysis.recommended_partitions,
                'optimization_benefit': analysis.optimization_benefit
            }
            report['tables'].append(table_info)
            
            # Update summary counters
            if not analysis.current_partitions:
                report['summary']['unpartitioned_tables'] += 1
                
            if analysis.optimization_benefit.startswith('Low'):
                report['summary']['optimally_partitioned'] += 1
            else:
                report['summary']['needs_optimization'] += 1
                
            if analysis.skew_ratio > 5:
                report['summary']['high_skew_tables'] += 1
                
            # Add specific recommendations
            if analysis.optimization_benefit.startswith('High') or analysis.optimization_benefit.startswith('Medium'):
                recommendation = {
                    'table': analysis.table_name,
                    'action': 'Repartition' if analysis.current_partitions else 'Add partitioning',
                    'from_partitions': analysis.current_partitions,
                    'to_partitions': analysis.recommended_partitions,
                    'priority': 'High' if analysis.optimization_benefit.startswith('High') else 'Medium',
                    'reason': analysis.optimization_benefit
                }
                report['recommendations'].append(recommendation)
        
        return report
    
    def save_analysis_report(self, report: Dict[str, Any], output_path: str = None):
        """Save partition analysis report to JSON file"""
        if not output_path:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"partition_analysis_report_{timestamp}.json"
        
        try:
            with open(output_path, 'w') as f:
                json.dump(report, f, indent=2)
            logger.info(f"Partition analysis report saved to {output_path}")
        except Exception as e:
            logger.error(f"Failed to save analysis report: {e}")
    
    def print_optimization_summary(self, report: Dict[str, Any]):
        """Print a human-readable optimization summary"""
        print("="*60)
        print("PARTITION OPTIMIZATION ANALYSIS SUMMARY")
        print("="*60)
        
        summary = report['summary']
        print(f"Total Tables Analyzed: {report['total_tables_analyzed']}")
        print(f"Unpartitioned Tables: {summary['unpartitioned_tables']}")
        print(f"Optimally Partitioned: {summary['optimally_partitioned']}")
        print(f"Need Optimization: {summary['needs_optimization']}")
        print(f"High Skew Tables: {summary['high_skew_tables']}")
        
        print("\nTop Recommendations:")
        for i, rec in enumerate(report['recommendations'][:5], 1):
            print(f"{i}. {rec['table']} - {rec['action']}")
            print(f"   From: {rec['from_partitions'] or 'None'}")
            print(f"   To: {rec['to_partitions']}")
            print(f"   Priority: {rec['priority']}")
            print(f"   Reason: {rec['reason']}")
            print()
        
        print("="*60)


def main():
    """Main function for partition strategy analysis"""
    logging.basicConfig(level=logging.INFO)
    
    try:
        analyzer = PartitionStrategyAnalyzer()
        
        # Analyze all tables
        analyses = analyzer.analyze_all_tables()
        
        # Generate report
        report = analyzer.generate_optimization_report(analyses)
        
        # Print summary
        analyzer.print_optimization_summary(report)
        
        # Save report
        analyzer.save_analysis_report(report)
        
        logger.info("Partition strategy analysis completed successfully")
        
    except Exception as e:
        logger.error(f"Partition strategy analysis failed: {e}")
        raise
    finally:
        if 'analyzer' in locals() and analyzer.spark:
            analyzer.spark.stop()


if __name__ == "__main__":
    main()

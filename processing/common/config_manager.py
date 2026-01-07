"""
Pipeline Configuration Management
Centralized configuration for all pipeline operations
"""
import os
from typing import Dict, Any, Optional
from dataclasses import dataclass
from pathlib import Path


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    server: str = "localhost"
    database: str = "YelpRS"
    username: str = "sa"
    password: str = ""
    driver: str = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    port: int = 1433
    
    @property
    def jdbc_url(self) -> str:
        return f"jdbc:sqlserver://{self.server}:{self.port};databaseName={self.database};encrypt=false"


@dataclass
class SparkConfig:
    """Spark configuration settings"""
    app_name_prefix: str = "YELP_RS"
    master: str = "local[*]"
    executor_memory: str = "4g"
    driver_memory: str = "2g"
    max_result_size: str = "2g"
    sql_shuffle_partitions: int = 200
    sql_adaptive_enabled: bool = True
    sql_adaptive_coalescePartitions_enabled: bool = True
    
    def to_dict(self) -> Dict[str, str]:
        return {
            "spark.master": self.master,
            "spark.executor.memory": self.executor_memory,
            "spark.driver.memory": self.driver_memory,
            "spark.driver.maxResultSize": self.max_result_size,
            "spark.sql.shuffle.partitions": str(self.sql_shuffle_partitions),
            "spark.sql.adaptive.enabled": str(self.sql_adaptive_enabled).lower(),
            "spark.sql.adaptive.coalescePartitions.enabled": str(self.sql_adaptive_coalescePartitions_enabled).lower()
        }


@dataclass
class DataPathConfig:
    """Data path configuration"""
    base_path: str = "D:/Project/YELP_RS/data"
    bronze_path: str = "bronze"
    silver_path: str = "silver"
    gold_path: str = "gold"
    
    # Source data paths
    source_business: str = ""
    source_review: str = ""
    source_user: str = ""
    source_checkin: str = ""
    source_tip: str = ""
    
    def get_layer_path(self, layer: str) -> str:
        """Get full path for a data layer"""
        return os.path.join(self.base_path, layer)
    
    def get_table_path(self, layer: str, table: str) -> str:
        """Get full path for a table in a layer"""
        return os.path.join(self.base_path, layer, table)


@dataclass
class PipelineConfig:
    """Pipeline execution configuration"""
    batch_size: int = 10000
    max_parallel_pipelines: int = 4
    retry_attempts: int = 3
    retry_delay_seconds: int = 30
    enable_checkpointing: bool = True
    checkpoint_interval: int = 100
    enable_monitoring: bool = True
    log_level: str = "INFO"


class ConfigManager:
    """Central configuration manager"""
    
    def __init__(self, config_file: Optional[str] = None):
        self.config_file = config_file or os.getenv("YELP_CONFIG_FILE", "config/pipeline_config.env")
        self.db_config = DatabaseConfig()
        self.spark_config = SparkConfig()
        self.data_config = DataPathConfig()
        self.pipeline_config = PipelineConfig()
        
        # Load configuration from environment or file
        self.load_configuration()
    
    def load_configuration(self):
        """Load configuration from environment variables and config file"""
        
        # Database configuration
        self.db_config.server = os.getenv("DB_SERVER", self.db_config.server)
        self.db_config.database = os.getenv("DB_DATABASE", self.db_config.database)
        self.db_config.username = os.getenv("DB_USERNAME", self.db_config.username)
        self.db_config.password = os.getenv("DB_PASSWORD", self.db_config.password)
        self.db_config.port = int(os.getenv("DB_PORT", str(self.db_config.port)))
        
        # Spark configuration
        self.spark_config.master = os.getenv("SPARK_MASTER", self.spark_config.master)
        self.spark_config.executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", self.spark_config.executor_memory)
        self.spark_config.driver_memory = os.getenv("SPARK_DRIVER_MEMORY", self.spark_config.driver_memory)
        
        # Data paths
        self.data_config.base_path = os.getenv("DATA_BASE_PATH", self.data_config.base_path)
        self.data_config.source_business = os.getenv("SOURCE_BUSINESS_PATH", self.data_config.source_business)
        self.data_config.source_review = os.getenv("SOURCE_REVIEW_PATH", self.data_config.source_review)
        self.data_config.source_user = os.getenv("SOURCE_USER_PATH", self.data_config.source_user)
        self.data_config.source_checkin = os.getenv("SOURCE_CHECKIN_PATH", self.data_config.source_checkin)
        self.data_config.source_tip = os.getenv("SOURCE_TIP_PATH", self.data_config.source_tip)
        
        # Pipeline configuration
        self.pipeline_config.batch_size = int(os.getenv("PIPELINE_BATCH_SIZE", str(self.pipeline_config.batch_size)))
        self.pipeline_config.max_parallel_pipelines = int(os.getenv("MAX_PARALLEL_PIPELINES", str(self.pipeline_config.max_parallel_pipelines)))
        self.pipeline_config.log_level = os.getenv("LOG_LEVEL", self.pipeline_config.log_level)
        
        # Load from config file if exists
        if os.path.exists(self.config_file):
            self.load_from_file()
    
    def load_from_file(self):
        """Load additional configuration from file"""
        try:
            with open(self.config_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip()
        except Exception as e:
            print(f"Warning: Could not load config file {self.config_file}: {e}")
    
    def get_standard_config(self) -> Dict[str, Any]:
        """Get standardized configuration dictionary for pipelines"""
        return {
            "base_path": self.data_config.base_path,
            "batch_id": f"batch_{int(os.urandom(4).hex(), 16)}",
            "source_name": "yelp_dataset",
            "database_config": {
                "url": self.db_config.jdbc_url,
                "username": self.db_config.username,
                "password": self.db_config.password,
                "driver": self.db_config.driver
            },
            "spark_config": self.spark_config.to_dict(),
            "source_paths": {
                "business": self.data_config.source_business,
                "review": self.data_config.source_review,
                "user": self.data_config.source_user,
                "checkin": self.data_config.source_checkin,
                "tip": self.data_config.source_tip
            },
            "pipeline_config": {
                "batch_size": self.pipeline_config.batch_size,
                "retry_attempts": self.pipeline_config.retry_attempts,
                "enable_monitoring": self.pipeline_config.enable_monitoring
            }
        }
    
    def create_directories(self):
        """Create necessary data directories"""
        base_path = Path(self.data_config.base_path)
        layers = ["bronze", "silver", "gold"]
        tables = ["business", "review", "user", "checkin", "tip"]
        
        for layer in layers:
            for table in tables:
                path = base_path / layer / table
                path.mkdir(parents=True, exist_ok=True)
                print(f"Created directory: {path}")
    
    def validate_configuration(self) -> bool:
        """Validate current configuration"""
        issues = []
        
        # Check required paths
        if not self.data_config.base_path:
            issues.append("Base data path not configured")
        
        # Check database configuration
        if not self.db_config.server or not self.db_config.database:
            issues.append("Database configuration incomplete")
        
        # Check source data paths
        source_paths = [
            self.data_config.source_business,
            self.data_config.source_review,
            self.data_config.source_user,
            self.data_config.source_checkin,
            self.data_config.source_tip
        ]
        
        if not any(source_paths):
            issues.append("No source data paths configured")
        
        if issues:
            print("Configuration validation failed:")
            for issue in issues:
                print(f"  - {issue}")
            return False
        
        print("Configuration validation passed")
        return True
    
    def print_configuration(self):
        """Print current configuration"""
        print("="*50)
        print("CURRENT PIPELINE CONFIGURATION")
        print("="*50)
        
        print("\nDatabase Configuration:")
        print(f"  Server: {self.db_config.server}:{self.db_config.port}")
        print(f"  Database: {self.db_config.database}")
        print(f"  Username: {self.db_config.username}")
        print(f"  JDBC URL: {self.db_config.jdbc_url}")
        
        print("\nSpark Configuration:")
        print(f"  Master: {self.spark_config.master}")
        print(f"  Executor Memory: {self.spark_config.executor_memory}")
        print(f"  Driver Memory: {self.spark_config.driver_memory}")
        
        print("\nData Path Configuration:")
        print(f"  Base Path: {self.data_config.base_path}")
        print(f"  Bronze Path: {self.data_config.get_layer_path('bronze')}")
        print(f"  Silver Path: {self.data_config.get_layer_path('silver')}")
        print(f"  Gold Path: {self.data_config.get_layer_path('gold')}")
        
        print("\nSource Data Paths:")
        print(f"  Business: {self.data_config.source_business or 'Not configured'}")
        print(f"  Review: {self.data_config.source_review or 'Not configured'}")
        print(f"  User: {self.data_config.source_user or 'Not configured'}")
        print(f"  Checkin: {self.data_config.source_checkin or 'Not configured'}")
        print(f"  Tip: {self.data_config.source_tip or 'Not configured'}")
        
        print("\nPipeline Configuration:")
        print(f"  Batch Size: {self.pipeline_config.batch_size:,}")
        print(f"  Max Parallel: {self.pipeline_config.max_parallel_pipelines}")
        print(f"  Log Level: {self.pipeline_config.log_level}")
        print("="*50)


# Global configuration instance
config_manager = ConfigManager()


def get_config_manager() -> ConfigManager:
    """Get the global configuration manager instance"""
    return config_manager


if __name__ == "__main__":
    # Configuration validation and setup
    config = get_config_manager()
    config.print_configuration()
    config.validate_configuration()
    config.create_directories()

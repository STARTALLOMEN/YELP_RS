"""
Yelp Recommendation Model Training Script

This script trains a hybrid recommendation model combining:
- Collaborative Filtering (ALS)
- Content-Based Filtering (TF-IDF)

Usage:
    python -m api.train --data-path ./data/bronze --output-dir ./api/models
    python -m api.train --help

Author: YELP_RS Project
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Tuple, Optional, Dict, Any

import structlog
from dotenv import load_dotenv

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)

# Load environment variables
load_dotenv()


# =============================================================================
# Configuration
# =============================================================================

class TrainingConfig:
    """
    Training configuration container.
    
    Args:
        data_path: Path to data directory containing Yelp datasets.
        output_dir: Directory to save trained models.
        als_rank: Number of latent factors for ALS.
        als_max_iter: Maximum iterations for ALS.
        als_reg_param: Regularization parameter for ALS.
        alpha: Weight for hybrid model (0=CBF only, 1=CF only).
        use_mlflow: Whether to log to MLflow.
    """
    
    def __init__(
        self,
        data_path: str = "./data/bronze",
        output_dir: str = "./api/models",
        als_rank: int = 10,
        als_max_iter: int = 10,
        als_reg_param: float = 0.1,
        alpha: float = 0.5,
        use_mlflow: bool = False,
    ):
        self.data_path = Path(data_path)
        self.output_dir = Path(output_dir)
        self.als_rank = als_rank
        self.als_max_iter = als_max_iter
        self.als_reg_param = als_reg_param
        self.alpha = alpha
        self.use_mlflow = use_mlflow
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary for logging."""
        return {
            "data_path": str(self.data_path),
            "output_dir": str(self.output_dir),
            "als_rank": self.als_rank,
            "als_max_iter": self.als_max_iter,
            "als_reg_param": self.als_reg_param,
            "alpha": self.alpha,
            "use_mlflow": self.use_mlflow,
        }


# =============================================================================
# Data Loading
# =============================================================================

def create_spark_session(app_name: str = "YelpHybridRecommendation"):
    """
    Create and configure Spark session.
    
    Args:
        app_name: Name for the Spark application.
        
    Returns:
        SparkSession: Configured Spark session.
    """
    from pyspark.sql import SparkSession
    
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    
    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("spark_session_created", app_name=app_name)
    return spark


def load_data(spark, config: TrainingConfig) -> Tuple:
    """
    Load business and review datasets from JSON files.
    
    Args:
        spark: SparkSession instance.
        config: Training configuration.
        
    Returns:
        Tuple of (businesses_df, reviews_df).
        
    Raises:
        FileNotFoundError: If required data files are not found.
    """
    business_path = config.data_path / "yelp_academic_dataset_business.json"
    review_path = config.data_path / "yelp_academic_dataset_review.json"
    
    # Check if files exist
    if not business_path.exists():
        raise FileNotFoundError(f"Business data not found: {business_path}")
    if not review_path.exists():
        raise FileNotFoundError(f"Review data not found: {review_path}")
    
    logger.info("loading_data", business_path=str(business_path), review_path=str(review_path))
    
    # Load datasets
    businesses = spark.read.json(str(business_path))
    reviews = spark.read.json(str(review_path))
    
    # Select relevant columns
    businesses = businesses.select("business_id", "categories", "stars", "review_count")
    reviews = reviews.select("user_id", "business_id", "stars")
    
    business_count = businesses.count()
    review_count = reviews.count()
    
    logger.info(
        "data_loaded",
        business_count=business_count,
        review_count=review_count
    )
    
    return businesses, reviews


# =============================================================================
# Content-Based Filtering (TF-IDF)
# =============================================================================

def train_tfidf_model(spark, businesses, config: TrainingConfig):
    """
    Train TF-IDF model for content-based filtering.
    
    Args:
        spark: SparkSession instance.
        businesses: Business DataFrame.
        config: Training configuration.
        
    Returns:
        Tuple of (transformed_businesses, idf_model).
    """
    from pyspark.sql.functions import col, split
    from pyspark.ml.feature import HashingTF, IDF
    
    logger.info("training_tfidf_model")
    
    # Split categories into array
    businesses = businesses.withColumn(
        "categories", 
        split(col("categories"), ", ")
    )
    
    # HashingTF to convert categories to TF vector
    hashing_tf = HashingTF(
        inputCol="categories", 
        outputCol="tf_features", 
        numFeatures=1000
    )
    businesses = hashing_tf.transform(businesses)
    
    # IDF to adjust word weights
    idf = IDF(inputCol="tf_features", outputCol="tfidf_features")
    idf_model = idf.fit(businesses)
    businesses = idf_model.transform(businesses)
    
    logger.info("tfidf_model_trained")
    
    return businesses, idf_model


# =============================================================================
# Collaborative Filtering (ALS)
# =============================================================================

def train_als_model(spark, reviews, config: TrainingConfig):
    """
    Train ALS model for collaborative filtering.
    
    Args:
        spark: SparkSession instance.
        reviews: Reviews DataFrame.
        config: Training configuration.
        
    Returns:
        Tuple of (als_model, user_indexer_model, business_indexer_model, indexed_reviews).
    """
    from pyspark.sql.functions import col
    from pyspark.ml.feature import StringIndexer
    from pyspark.ml.recommendation import ALS
    
    logger.info(
        "training_als_model",
        rank=config.als_rank,
        max_iter=config.als_max_iter,
        reg_param=config.als_reg_param
    )
    
    # Cast stars to float
    reviews = reviews.withColumn("stars", col("stars").cast("float"))
    
    # Index user_id and business_id
    user_indexer = StringIndexer(inputCol="user_id", outputCol="user_index")
    business_indexer = StringIndexer(inputCol="business_id", outputCol="business_index")
    
    user_indexer_model = user_indexer.fit(reviews)
    reviews = user_indexer_model.transform(reviews)
    
    business_indexer_model = business_indexer.fit(reviews)
    reviews = business_indexer_model.transform(reviews)
    
    # Build ALS model
    als = ALS(
        userCol="user_index",
        itemCol="business_index",
        ratingCol="stars",
        rank=config.als_rank,
        maxIter=config.als_max_iter,
        regParam=config.als_reg_param,
        nonnegative=True,
        implicitPrefs=False,
        coldStartStrategy="drop"
    )
    
    # Train model
    als_model = als.fit(reviews)
    
    logger.info("als_model_trained")
    
    return als_model, user_indexer_model, business_indexer_model, reviews


# =============================================================================
# Model Evaluation
# =============================================================================

def evaluate_model(als_model, reviews) -> float:
    """
    Evaluate ALS model using RMSE.
    
    Args:
        als_model: Trained ALS model.
        reviews: Reviews DataFrame with indexed columns.
        
    Returns:
        RMSE score.
    """
    from pyspark.ml.evaluation import RegressionEvaluator
    
    logger.info("evaluating_model")
    
    # Get predictions
    predictions = als_model.transform(reviews)
    
    # Calculate RMSE
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="stars",
        predictionCol="prediction"
    )
    rmse = evaluator.evaluate(predictions)
    
    logger.info("model_evaluated", rmse=rmse)
    
    return rmse


# =============================================================================
# Model Saving
# =============================================================================

def save_models(
    config: TrainingConfig,
    als_model,
    idf_model,
    user_indexer_model,
    business_indexer_model
) -> None:
    """
    Save trained models to disk.
    
    Args:
        config: Training configuration.
        als_model: Trained ALS model.
        idf_model: Trained IDF model.
        user_indexer_model: User StringIndexer model.
        business_indexer_model: Business StringIndexer model.
    """
    import joblib
    
    # Create output directory
    config.output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info("saving_models", output_dir=str(config.output_dir))
    
    # Save Spark ML models (as directories)
    als_model.save(str(config.output_dir / "als_model"))
    idf_model.save(str(config.output_dir / "idf_model"))
    user_indexer_model.save(str(config.output_dir / "user_indexer"))
    business_indexer_model.save(str(config.output_dir / "business_indexer"))
    
    # Save config as metadata
    metadata = {
        "config": config.to_dict(),
        "model_version": "1.0.0",
        "training_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    joblib.dump(metadata, config.output_dir / "metadata.pkl")
    
    logger.info("models_saved", model_count=5)


# =============================================================================
# MLflow Integration (Enhanced)
# =============================================================================

def log_to_mlflow(
    config: TrainingConfig,
    rmse: float,
    training_time: float,
    register_model: bool = True
) -> Optional[str]:
    """
    Log training run to MLflow with optional model registration.
    
    Args:
        config: Training configuration.
        rmse: Model RMSE score.
        training_time: Total training time in seconds.
        register_model: Whether to register model in registry.
        
    Returns:
        Run ID if successful, None otherwise.
    """
    try:
        from api.mlflow_utils import MLflowTracker
        
        with MLflowTracker(
            experiment_name="yelp-recommendation",
            run_name=f"training_{time.strftime('%Y%m%d_%H%M%S')}",
            tags={
                "model_type": "hybrid",
                "als_rank": str(config.als_rank),
            }
        ) as tracker:
            # Log all parameters
            tracker.log_params({
                "data_path": str(config.data_path),
                "als_rank": config.als_rank,
                "als_max_iter": config.als_max_iter,
                "als_reg_param": config.als_reg_param,
                "alpha": config.alpha,
            })
            
            # Log metrics
            tracker.log_metrics({
                "rmse": rmse,
                "training_time_seconds": training_time,
            })
            
            # Log model artifacts
            tracker.log_artifacts(str(config.output_dir), "models")
            
            # Register model in registry if enabled
            if register_model and tracker.run_id:
                model_uri = f"runs:/{tracker.run_id}/models"
                version = tracker.register_model(
                    model_uri=model_uri,
                    name="yelp-recommendation-hybrid"
                )
                if version:
                    logger.info(
                        "model_registered",
                        name="yelp-recommendation-hybrid",
                        version=version
                    )
            
            return tracker.run_id
            
    except ImportError:
        logger.warning("mlflow_not_installed", message="Skipping MLflow logging")
        return None
    except Exception as e:
        logger.error("mlflow_error", error=str(e))
        return None


# =============================================================================
# Main Training Pipeline
# =============================================================================

def train(config: TrainingConfig) -> Dict[str, Any]:
    """
    Run full training pipeline.
    
    Args:
        config: Training configuration.
        
    Returns:
        Dictionary containing training results.
    """
    start_time = time.time()
    
    logger.info("training_started", config=config.to_dict())
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Load data
        businesses, reviews = load_data(spark, config)
        
        # Train TF-IDF model
        businesses, idf_model = train_tfidf_model(spark, businesses, config)
        
        # Train ALS model
        als_model, user_indexer_model, business_indexer_model, indexed_reviews = \
            train_als_model(spark, reviews, config)
        
        # Evaluate model
        rmse = evaluate_model(als_model, indexed_reviews)
        
        # Save models
        save_models(
            config,
            als_model,
            idf_model,
            user_indexer_model,
            business_indexer_model
        )
        
        training_time = time.time() - start_time
        
        # Log to MLflow if enabled
        if config.use_mlflow:
            log_to_mlflow(config, rmse, training_time)
        
        results = {
            "status": "success",
            "rmse": rmse,
            "training_time_seconds": training_time,
            "output_dir": str(config.output_dir),
        }
        
        logger.info("training_completed", **results)
        
        return results
        
    except Exception as e:
        logger.error("training_failed", error=str(e))
        raise
        
    finally:
        spark.stop()
        logger.info("spark_session_stopped")


# =============================================================================
# CLI Entry Point
# =============================================================================

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Train Yelp Recommendation Model",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        "--data-path",
        type=str,
        default=os.getenv("BRONZE_DATA_PATH", "./data/bronze"),
        help="Path to directory containing Yelp JSON datasets"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        default=os.getenv("MODEL_OUTPUT_DIR", "./api/models"),
        help="Directory to save trained models"
    )
    
    parser.add_argument(
        "--als-rank",
        type=int,
        default=10,
        help="Number of latent factors for ALS"
    )
    
    parser.add_argument(
        "--als-max-iter",
        type=int,
        default=10,
        help="Maximum iterations for ALS training"
    )
    
    parser.add_argument(
        "--als-reg-param",
        type=float,
        default=0.1,
        help="Regularization parameter for ALS"
    )
    
    parser.add_argument(
        "--alpha",
        type=float,
        default=0.5,
        help="Weight for hybrid model (0=CBF only, 1=CF only)"
    )
    
    parser.add_argument(
        "--use-mlflow",
        action="store_true",
        help="Enable MLflow tracking"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print configuration without training"
    )
    
    return parser.parse_args()


def main() -> int:
    """
    Main entry point for training script.
    
    Returns:
        Exit code (0 for success, 1 for failure).
    """
    args = parse_args()
    
    config = TrainingConfig(
        data_path=args.data_path,
        output_dir=args.output_dir,
        als_rank=args.als_rank,
        als_max_iter=args.als_max_iter,
        als_reg_param=args.als_reg_param,
        alpha=args.alpha,
        use_mlflow=args.use_mlflow,
    )
    
    if args.dry_run:
        print("=== Dry Run Configuration ===")
        for key, value in config.to_dict().items():
            print(f"  {key}: {value}")
        return 0
    
    try:
        results = train(config)
        print(f"\n✅ Training completed successfully!")
        print(f"   RMSE: {results['rmse']:.4f}")
        print(f"   Time: {results['training_time_seconds']:.2f}s")
        print(f"   Output: {results['output_dir']}")
        return 0
        
    except FileNotFoundError as e:
        print(f"\n❌ Data not found: {e}")
        print("   Please ensure Yelp datasets are in the data path.")
        return 1
        
    except Exception as e:
        print(f"\n❌ Training failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

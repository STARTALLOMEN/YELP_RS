"""
MLflow Utilities for Yelp Recommendation System

Provides helper functions for experiment tracking, model registry,
and artifact management using MLflow.

Usage:
    from api.mlflow_utils import MLflowTracker
    
    with MLflowTracker("yelp-recommendation", run_name="training_v1") as tracker:
        tracker.log_params({"alpha": 0.5})
        tracker.log_metrics({"rmse": 0.85})
        tracker.log_model_artifacts("./models")
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Any, Optional, List
import time

import structlog

logger = structlog.get_logger(__name__)


# =============================================================================
# Configuration
# =============================================================================

@dataclass
class MLflowConfig:
    """
    MLflow configuration settings.
    
    Args:
        tracking_uri: MLflow tracking server URI. Default uses local file store.
        experiment_name: Name of the experiment.
        artifact_location: Path for artifact storage.
        registry_uri: Model registry URI.
    """
    tracking_uri: str = field(default_factory=lambda: os.getenv(
        "MLFLOW_TRACKING_URI", 
        "file:./mlruns"
    ))
    experiment_name: str = "yelp-recommendation"
    artifact_location: Optional[str] = None
    registry_uri: Optional[str] = None
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if self.artifact_location is None:
            self.artifact_location = f"./mlruns/{self.experiment_name}"


# =============================================================================
# MLflow Tracker Class
# =============================================================================

class MLflowTracker:
    """
    Context manager for MLflow experiment tracking.
    
    Provides a clean interface for logging parameters, metrics,
    artifacts, and registering models.
    
    Example:
        with MLflowTracker("experiment-name", run_name="run-1") as tracker:
            tracker.log_params({"learning_rate": 0.01})
            tracker.log_metrics({"accuracy": 0.95})
            tracker.log_artifact("model.pkl")
            tracker.register_model("model.pkl", "my-model")
    """
    
    def __init__(
        self,
        experiment_name: str = "yelp-recommendation",
        run_name: Optional[str] = None,
        config: Optional[MLflowConfig] = None,
        tags: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize MLflow tracker.
        
        Args:
            experiment_name: Name of the MLflow experiment.
            run_name: Optional name for this specific run.
            config: MLflow configuration object.
            tags: Optional tags to add to the run.
        """
        self.experiment_name = experiment_name
        self.run_name = run_name or f"run_{int(time.time())}"
        self.config = config or MLflowConfig(experiment_name=experiment_name)
        self.tags = tags or {}
        self._run = None
        self._mlflow = None
        self._start_time: Optional[float] = None
        
    def __enter__(self) -> "MLflowTracker":
        """Start MLflow run on context entry."""
        try:
            import mlflow
            self._mlflow = mlflow
            
            # Set tracking URI
            mlflow.set_tracking_uri(self.config.tracking_uri)
            
            # Set or create experiment
            experiment = mlflow.get_experiment_by_name(self.experiment_name)
            if experiment is None:
                experiment_id = mlflow.create_experiment(
                    self.experiment_name,
                    artifact_location=self.config.artifact_location
                )
                logger.info(
                    "mlflow_experiment_created",
                    experiment_name=self.experiment_name,
                    experiment_id=experiment_id
                )
            else:
                mlflow.set_experiment(self.experiment_name)
            
            # Start run
            self._run = mlflow.start_run(run_name=self.run_name)
            self._start_time = time.time()
            
            # Log tags
            if self.tags:
                mlflow.set_tags(self.tags)
            
            # Log system info
            mlflow.set_tag("environment", os.getenv("APP_ENV", "dev"))
            mlflow.set_tag("run_name", self.run_name)
            
            logger.info(
                "mlflow_run_started",
                experiment=self.experiment_name,
                run_name=self.run_name,
                run_id=self._run.info.run_id
            )
            
            return self
            
        except ImportError:
            logger.warning("mlflow_not_installed", message="MLflow not available")
            return self
        except Exception as e:
            logger.error("mlflow_start_error", error=str(e))
            return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """End MLflow run on context exit."""
        if self._mlflow and self._run:
            try:
                # Log total duration
                if self._start_time:
                    duration = time.time() - self._start_time
                    self._mlflow.log_metric("run_duration_seconds", duration)
                
                # Set status based on exception
                if exc_type is not None:
                    self._mlflow.set_tag("run_status", "FAILED")
                    self._mlflow.set_tag("error_type", str(exc_type.__name__))
                else:
                    self._mlflow.set_tag("run_status", "COMPLETED")
                
                self._mlflow.end_run()
                
                logger.info(
                    "mlflow_run_ended",
                    run_id=self._run.info.run_id,
                    status="FAILED" if exc_type else "COMPLETED"
                )
                
            except Exception as e:
                logger.error("mlflow_end_error", error=str(e))
        
        return False  # Don't suppress exceptions
    
    def log_params(self, params: Dict[str, Any]) -> None:
        """
        Log parameters to MLflow.
        
        Args:
            params: Dictionary of parameter names and values.
        """
        if self._mlflow:
            try:
                self._mlflow.log_params(params)
                logger.debug("mlflow_params_logged", count=len(params))
            except Exception as e:
                logger.error("mlflow_log_params_error", error=str(e))
    
    def log_metrics(
        self, 
        metrics: Dict[str, float], 
        step: Optional[int] = None
    ) -> None:
        """
        Log metrics to MLflow.
        
        Args:
            metrics: Dictionary of metric names and values.
            step: Optional step number for metric history.
        """
        if self._mlflow:
            try:
                if step is not None:
                    for key, value in metrics.items():
                        self._mlflow.log_metric(key, value, step=step)
                else:
                    self._mlflow.log_metrics(metrics)
                logger.debug("mlflow_metrics_logged", count=len(metrics))
            except Exception as e:
                logger.error("mlflow_log_metrics_error", error=str(e))
    
    def log_artifact(self, local_path: str, artifact_path: Optional[str] = None) -> None:
        """
        Log a single artifact file to MLflow.
        
        Args:
            local_path: Path to the local file.
            artifact_path: Optional subdirectory in artifact store.
        """
        if self._mlflow:
            try:
                self._mlflow.log_artifact(local_path, artifact_path)
                logger.debug("mlflow_artifact_logged", path=local_path)
            except Exception as e:
                logger.error("mlflow_log_artifact_error", error=str(e))
    
    def log_artifacts(self, local_dir: str, artifact_path: Optional[str] = None) -> None:
        """
        Log all files in a directory as artifacts.
        
        Args:
            local_dir: Path to local directory.
            artifact_path: Optional subdirectory in artifact store.
        """
        if self._mlflow:
            try:
                self._mlflow.log_artifacts(local_dir, artifact_path)
                logger.debug("mlflow_artifacts_logged", dir=local_dir)
            except Exception as e:
                logger.error("mlflow_log_artifacts_error", error=str(e))
    
    def log_model(
        self,
        model,
        artifact_path: str = "model",
        registered_model_name: Optional[str] = None,
        flavor: str = "sklearn"
    ) -> Optional[str]:
        """
        Log a model to MLflow with optional registration.
        
        Args:
            model: The model object to log.
            artifact_path: Path within artifacts to store model.
            registered_model_name: If provided, register model with this name.
            flavor: MLflow model flavor (sklearn, spark, etc.)
            
        Returns:
            Model URI if successful, None otherwise.
        """
        if not self._mlflow:
            return None
            
        try:
            # Get the appropriate log_model function
            if flavor == "sklearn":
                from mlflow.sklearn import log_model as log_sklearn_model
                result = log_sklearn_model(
                    model,
                    artifact_path,
                    registered_model_name=registered_model_name
                )
            elif flavor == "spark":
                from mlflow.spark import log_model as log_spark_model
                result = log_spark_model(
                    model,
                    artifact_path,
                    registered_model_name=registered_model_name
                )
            else:
                # Generic pickle-based logging
                import joblib
                import tempfile
                with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
                    joblib.dump(model, f.name)
                    self._mlflow.log_artifact(f.name, artifact_path)
                    result = None
            
            logger.info(
                "mlflow_model_logged",
                artifact_path=artifact_path,
                registered_name=registered_model_name
            )
            
            return getattr(result, 'model_uri', None) if result else None
            
        except Exception as e:
            logger.error("mlflow_log_model_error", error=str(e))
            return None
    
    def register_model(
        self,
        model_uri: str,
        name: str,
        await_creation: bool = True
    ) -> Optional[str]:
        """
        Register a logged model in the model registry.
        
        Args:
            model_uri: URI of the logged model.
            name: Name for the registered model.
            await_creation: Whether to wait for registration to complete.
            
        Returns:
            Model version if successful, None otherwise.
        """
        if not self._mlflow:
            return None
            
        try:
            from mlflow.tracking import MlflowClient
            
            client = MlflowClient()
            
            # Create registered model if it doesn't exist
            try:
                client.create_registered_model(name)
                logger.info("mlflow_registered_model_created", name=name)
            except Exception:
                pass  # Model already exists
            
            # Create model version
            result = client.create_model_version(
                name=name,
                source=model_uri,
                run_id=self._run.info.run_id
            )
            
            logger.info(
                "mlflow_model_registered",
                name=name,
                version=result.version
            )
            
            return result.version
            
        except Exception as e:
            logger.error("mlflow_register_model_error", error=str(e))
            return None
    
    def transition_model_stage(
        self,
        name: str,
        version: str,
        stage: str = "Production"
    ) -> bool:
        """
        Transition a model version to a new stage.
        
        Args:
            name: Registered model name.
            version: Model version to transition.
            stage: Target stage (Staging, Production, Archived).
            
        Returns:
            True if successful, False otherwise.
        """
        if not self._mlflow:
            return False
            
        try:
            from mlflow.tracking import MlflowClient
            
            client = MlflowClient()
            client.transition_model_version_stage(
                name=name,
                version=version,
                stage=stage
            )
            
            logger.info(
                "mlflow_model_stage_transition",
                name=name,
                version=version,
                stage=stage
            )
            
            return True
            
        except Exception as e:
            logger.error("mlflow_transition_error", error=str(e))
            return False
    
    @property
    def run_id(self) -> Optional[str]:
        """Get current run ID."""
        return self._run.info.run_id if self._run else None
    
    @property
    def artifact_uri(self) -> Optional[str]:
        """Get artifact URI for current run."""
        return self._run.info.artifact_uri if self._run else None


# =============================================================================
# Utility Functions
# =============================================================================

def get_best_model_version(
    model_name: str,
    metric_name: str = "rmse",
    ascending: bool = True
) -> Optional[Dict[str, Any]]:
    """
    Get the best model version based on a metric.
    
    Args:
        model_name: Name of the registered model.
        metric_name: Metric to compare.
        ascending: If True, lower is better. If False, higher is better.
        
    Returns:
        Dictionary with version info or None if not found.
    """
    try:
        import mlflow
        from mlflow.tracking import MlflowClient
        
        client = MlflowClient()
        
        # Get all versions
        versions = client.search_model_versions(f"name='{model_name}'")
        
        if not versions:
            return None
        
        # Get metrics for each version
        version_metrics = []
        for v in versions:
            run = client.get_run(v.run_id)
            metric_value = run.data.metrics.get(metric_name)
            if metric_value is not None:
                version_metrics.append({
                    "version": v.version,
                    "run_id": v.run_id,
                    "stage": v.current_stage,
                    metric_name: metric_value
                })
        
        if not version_metrics:
            return None
        
        # Sort and return best
        version_metrics.sort(key=lambda x: x[metric_name], reverse=not ascending)
        return version_metrics[0]
        
    except Exception as e:
        logger.error("get_best_model_error", error=str(e))
        return None


def load_production_model(model_name: str):
    """
    Load the production version of a registered model.
    
    Args:
        model_name: Name of the registered model.
        
    Returns:
        Loaded model or None if not found.
    """
    try:
        import mlflow
        
        model_uri = f"models:/{model_name}/Production"
        model = mlflow.pyfunc.load_model(model_uri)
        
        logger.info("production_model_loaded", model_name=model_name)
        return model
        
    except Exception as e:
        logger.error("load_production_model_error", error=str(e))
        return None


@contextmanager
def mlflow_run(
    experiment_name: str = "yelp-recommendation",
    run_name: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None
):
    """
    Simplified context manager for MLflow runs.
    
    Example:
        with mlflow_run("my-experiment", run_name="test-run") as run:
            mlflow.log_param("alpha", 0.5)
            mlflow.log_metric("rmse", 0.85)
    """
    tracker = MLflowTracker(
        experiment_name=experiment_name,
        run_name=run_name,
        tags=tags
    )
    
    with tracker:
        yield tracker

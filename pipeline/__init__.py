"""Wikipedia edits pipeline package."""
from .streaming import WikimediaStreamer
from .processing import run_dataflow_pipeline
from .local_testing import run_local_pipeline

__all__ = ['WikimediaStreamer', 'run_dataflow_pipeline', 'run_local_pipeline']

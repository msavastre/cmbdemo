import yaml
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class DataplexConfig:
    scope: Dict[str, Any]

@dataclass
class GCSConfig:
    bucket_name: str
    output_path: str

@dataclass
class BigQueryConfig:
    dataset_id: str

@dataclass
class AppConfig:
    project_id: str
    location: str
    dataplex: DataplexConfig
    gcs: GCSConfig
    bigquery: BigQueryConfig

def load_config(config_path: str) -> AppConfig:
    """Loads configuration from a YAML file."""
    with open(config_path, 'r') as f:
        config_data = yaml.safe_load(f)

    return AppConfig(
        project_id=config_data['project_id'],
        location=config_data['location'],
        dataplex=DataplexConfig(scope=config_data['dataplex']['scope']),
        gcs=GCSConfig(**config_data['gcs']),
        bigquery=BigQueryConfig(**config_data['bigquery'])
    )

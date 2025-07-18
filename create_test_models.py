#!/usr/bin/env python3
"""
Create test models for the data transformation tool.
This script creates models based on the forecast cycle structure you provided.
"""

import os
import yaml
from pathlib import Path


def create_test_models():
    """Create test models directory with forecast cycle models."""
    
    # Create directory structure
    base_dir = Path("my_test_models")
    
    # Create layer directories
    bronze_dir = base_dir / "bronze"
    silver_dir = base_dir / "silver"
    gold_dir = base_dir / "gold"
    cte_dir = base_dir / "ctes"
    
    for dir_path in [bronze_dir, silver_dir, gold_dir, cte_dir]:
        dir_path.mkdir(parents=True, exist_ok=True)
    
    # Model 1: Bronze - Raw Forecast Cycle Data
    raw_forecast_cycle = {
        'model': {
            'name': 'raw_forecast_cycle',
            'description': 'Raw forecast cycle data from source systems',
            'layer': 'bronze',
            'kind': 'VIEW',
            'owner': 'data_engineering',
            'tags': ['forecast', 'raw', 'bronze'],
            'domain': 'planning',
            'refresh_frequency': 'daily'
        },
        'source': {
            'base_table': 'source_systems.forecast_cycles'
        },
        'transformations': {
            'columns': [
                {
                    'name': 'ForecastCycleId',
                    'reference_table': 'source_systems.forecast_cycles',
                    'expression': '',
                    'description': 'Unique forecast cycle identifier',
                    'data_type': 'STRING'
                },
                {
                    'name': 'PeriodStart',
                    'reference_table': 'source_systems.forecast_cycles',
                    'expression': '',
                    'description': 'Start date of forecast period',
                    'data_type': 'DATE'
                },
                {
                    'name': 'PeriodEnd',
                    'reference_table': 'source_systems.forecast_cycles',
                    'expression': '',
                    'description': 'End date of forecast period',
                    'data_type': 'DATE'
                },
                {
                    'name': 'CycleName',
                    'reference_table': 'source_systems.forecast_cycles',
                    'expression': '',
                    'description': 'Name of the forecast cycle',
                    'data_type': 'STRING'
                },
                {
                    'name': 'Status',
                    'reference_table': 'source_systems.forecast_cycles',
                    'expression': '',
                    'description': 'Status of the forecast cycle',
                    'data_type': 'STRING'
                },
                {
                    'name': 'CreatedDate',
                    'reference_table': 'source_systems.forecast_cycles',
                    'expression': '',
                    'description': 'Date when cycle was created',
                    'data_type': 'TIMESTAMP'
                }
            ]
        },
        'audits': {
            'audits': [
                {
                    'type': 'NOT_NULL',
                    'columns': ['ForecastCycleId', 'PeriodStart', 'PeriodEnd']
                },
                {
                    'type': 'ACCEPTED_VALUES',
                    'columns': ['Status'],
                    'values': ['DRAFT', 'ACTIVE', 'CLOSED', 'CANCELLED']
                }
            ]
        }
    }
    
    # Model 2: CTE - Active Forecast Cycles
    active_forecast_cycles = {
        'model': {
            'name': 'active_forecast_cycles',
            'description': 'Filter for active forecast cycles only',
            'layer': 'cte',
            'kind': 'CTE',
            'owner': 'data_engineering',
            'tags': ['forecast', 'cte', 'active'],
            'domain': 'planning',
            'refresh_frequency': 'daily'
        },
        'source': {
            'depends_on_tables': ['raw_forecast_cycle'],
            'base_table': 'raw_forecast_cycle'
        },
        'transformations': {
            'columns': [
                {
                    'name': 'ForecastCycleId',
                    'reference_table': 'raw_forecast_cycle',
                    'expression': '',
                    'description': 'Unique forecast cycle identifier',
                    'data_type': 'STRING'
                },
                {
                    'name': 'PeriodStart',
                    'reference_table': 'raw_forecast_cycle',
                    'expression': '',
                    'description': 'Start date of forecast period',
                    'data_type': 'DATE'
                },
                {
                    'name': 'PeriodEnd',
                    'reference_table': 'raw_forecast_cycle',
                    'expression': '',
                    'description': 'End date of forecast period',
                    'data_type': 'DATE'
                },
                {
                    'name': 'CycleName',
                    'reference_table': 'raw_forecast_cycle',
                    'expression': '',
                    'description': 'Name of the forecast cycle',
                    'data_type': 'STRING'
                }
            ]
        },
        'filters': {
            'where_conditions': [
                {
                    'reference_table': 'raw_forecast_cycle',
                    'condition': 'Status = "ACTIVE"'
                }
            ]
        }
    }
    
    # Model 3: Silver - Clean Forecast Cycle Data
    clean_forecast_cycle = {
        'model': {
            'name': 'clean_forecast_cycle',
            'description': 'Cleaned and standardized forecast cycle data',
            'layer': 'silver',
            'kind': 'TABLE',
            'owner': 'data_engineering',
            'tags': ['forecast', 'clean', 'silver'],
            'domain': 'planning',
            'refresh_frequency': 'daily'
        },
        'source': {
            'depends_on_tables': ['raw_forecast_cycle'],
            'base_table': 'raw_forecast_cycle'
        },
        'transformations': {
            'columns': [
                {
                    'name': 'ForecastCycleId',
                    'reference_table': 'raw_forecast_cycle',
                    'expression': 'TRIM(ForecastCycleId)',
                    'description': 'Cleaned forecast cycle identifier',
                    'data_type': 'STRING'
                },
                {
                    'name': 'PeriodStart',
                    'reference_table': 'raw_forecast_cycle',
                    'expression': '',
                    'description': 'Start date of forecast period',
                    'data_type': 'DATE'
                },
                {
                    'name': 'PeriodEnd',
                    'reference_table': 'raw_forecast_cycle',
                    'expression': '',
                    'description': 'End date of forecast period',
                    'data_type': 'DATE'
                },
                {
                    'name': 'CycleName',
                    'reference_table': 'raw_forecast_cycle',
                    'expression': 'TRIM(UPPER(CycleName))',
                    'description': 'Standardized cycle name',
                    'data_type': 'STRING'
                },
                {
                    'name': 'Status',
                    'reference_table': 'raw_forecast_cycle',
                    'expression': 'UPPER(Status)',
                    'description': 'Standardized status',
                    'data_type': 'STRING'
                },
                {
                    'name': 'PeriodDays',
                    'reference_table': 'raw_forecast_cycle',
                    'expression': 'DATEDIFF(PeriodEnd, PeriodStart)',
                    'description': 'Number of days in the forecast period',
                    'data_type': 'INTEGER'
                },
                {
                    'name': 'IsCurrentCycle',
                    'reference_table': 'raw_forecast_cycle',
                    'expression': 'CASE WHEN Status = "ACTIVE" AND CURRENT_DATE BETWEEN PeriodStart AND PeriodEnd THEN TRUE ELSE FALSE END',
                    'description': 'Flag indicating if this is the current active cycle',
                    'data_type': 'BOOLEAN'
                },
                {
                    'name': 'CreatedDate',
                    'reference_table': 'raw_forecast_cycle',
                    'expression': '',
                    'description': 'Date when cycle was created',
                    'data_type': 'TIMESTAMP'
                },
                {
                    'name': 'LoadTimestamp',
                    'reference_table': 'raw_forecast_cycle',
                    'expression': 'CURRENT_TIMESTAMP()',
                    'description': 'Timestamp when record was loaded',
                    'data_type': 'TIMESTAMP'
                }
            ]
        },
        'filters': {
            'where_conditions': [
                {
                    'reference_table': 'raw_forecast_cycle',
                    'condition': 'ForecastCycleId IS NOT NULL'
                },
                {
                    'reference_table': 'raw_forecast_cycle',
                    'condition': 'PeriodStart <= PeriodEnd'
                }
            ]
        },
        'grain': ['ForecastCycleId'],
        'optimization': {
            'partitioned_by': ['PeriodStart'],
            'clustered_by': ['Status']
        },
        'audits': {
            'audits': [
                {
                    'type': 'NOT_NULL',
                    'columns': ['ForecastCycleId', 'PeriodStart', 'PeriodEnd']
                },
                {
                    'type': 'UNIQUE_COMBINATION',
                    'columns': ['ForecastCycleId']
                },
                {
                    'type': 'POSITIVE_VALUES',
                    'columns': ['PeriodDays']
                }
            ]
        }
    }
    
    # Model 4: Gold - Fact Forecast Cycle (matches your JSON structure)
    fct_forecast_cycle = {
        'model': {
            'name': 'fct_ForecastCycle',
            'description': 'Fact table for forecast cycles - matches your JSON structure',
            'layer': 'gold',
            'kind': 'TABLE',
            'owner': 'data_engineering',
            'tags': ['forecast', 'fact', 'gold'],
            'domain': 'planning',
            'refresh_frequency': 'daily'
        },
        'source': {
            'depends_on_tables': ['clean_forecast_cycle', 'active_forecast_cycles'],
            'base_table': 'clean_forecast_cycle'
        },
        'ctes': {
            'ctes': ['active_forecast_cycles']
        },
        'transformations': {
            'columns': [
                {
                    'name': 'ForecastCycle_Id',
                    'reference_table': 'clean_forecast_cycle',
                    'expression': 'T.ForecastCycleId',
                    'description': 'Forecast cycle identifier (matches your JSON)',
                    'data_type': 'STRING'
                },
                {
                    'name': 'PeriodEnd',
                    'reference_table': 'clean_forecast_cycle',
                    'expression': 'T.PeriodEnd',
                    'description': 'Period end date (matches your JSON)',
                    'data_type': 'DATE'
                },
                {
                    'name': 'PeriodStart',
                    'reference_table': 'clean_forecast_cycle',
                    'expression': 'T.PeriodStart',
                    'description': 'Period start date (matches your JSON)',
                    'data_type': 'DATE'
                },
                {
                    'name': 'CycleName',
                    'reference_table': 'clean_forecast_cycle',
                    'expression': 'T.CycleName',
                    'description': 'Standardized cycle name',
                    'data_type': 'STRING'
                },
                {
                    'name': 'PeriodDays',
                    'reference_table': 'clean_forecast_cycle',
                    'expression': 'T.PeriodDays',
                    'description': 'Number of days in forecast period',
                    'data_type': 'INTEGER'
                },
                {
                    'name': 'IsActive',
                    'reference_table': 'active_forecast_cycles',
                    'expression': 'CASE WHEN A.ForecastCycleId IS NOT NULL THEN TRUE ELSE FALSE END',
                    'description': 'Flag indicating if cycle is active',
                    'data_type': 'BOOLEAN'
                }
            ]
        },
        'relationships': {
            'foreign_keys': [
                {
                    'local_column': 'ForecastCycle_Id',
                    'references_table': 'active_forecast_cycles',
                    'references_column': 'ForecastCycleId',
                    'relationship_type': 'many-to-one',
                    'join_type': 'LEFT'
                }
            ]
        },
        'filters': {
            'where_conditions': [
                {
                    'reference_table': 'clean_forecast_cycle',
                    'condition': 'T.Status IN ("ACTIVE", "CLOSED")'
                }
            ]
        },
        'grain': ['ForecastCycle_Id'],
        'optimization': {
            'partitioned_by': ['PeriodStart'],
            'clustered_by': ['IsActive']
        },
        'audits': {
            'audits': [
                {
                    'type': 'NOT_NULL',
                    'columns': ['ForecastCycle_Id', 'PeriodStart', 'PeriodEnd']
                },
                {
                    'type': 'UNIQUE_COMBINATION',
                    'columns': ['ForecastCycle_Id']
                }
            ]
        }
    }
    
    # Save all models
    models = [
        (bronze_dir / "raw_forecast_cycle.yaml", raw_forecast_cycle),
        (cte_dir / "active_forecast_cycles.yaml", active_forecast_cycles),
        (silver_dir / "clean_forecast_cycle.yaml", clean_forecast_cycle),
        (gold_dir / "fct_ForecastCycle.yaml", fct_forecast_cycle)
    ]
    
    for file_path, model_data in models:
        with open(file_path, 'w') as f:
            yaml.dump(model_data, f, default_flow_style=False, sort_keys=False)
        print(f"Created: {file_path}")
    
    # Create a test configuration file
    config = {
        'models_directory': './my_test_models',
        'output_directory': './my_output',
        'sql_dialect': 'spark',
        'generate_audits': True,
        'generate_lineage': True,
        'validate_on_read': True,
        'log_level': 'INFO',
        'incremental_strategy': 'append'
    }
    
    config_file = base_dir / "config.yaml"
    with open(config_file, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)
    print(f"Created: {config_file}")
    
    print(f"\n✅ Test models created successfully!")
    print(f"📁 Models directory: {base_dir}")
    print(f"📋 Created {len(models)} models:")
    for file_path, _ in models:
        print(f"  - {file_path.name}")


if __name__ == "__main__":
    create_test_models()
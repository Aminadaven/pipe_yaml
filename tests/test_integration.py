import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import yaml

from pipe_yaml.__main__ import main as etl_main


class TestIntegration:
    """Integration tests for complete pipeline execution."""

    def test_complete_spark_pipeline_execution(self):
        """Test complete pipeline execution with Spark engine."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test input data as CSV string
            input_file = Path(temp_dir) / 'input.csv'
            csv_content = """id,name,value
1,Alice,10
2,Bob,20
3,Charlie,30
4,Diana,40
5,Eve,50"""
            input_file.write_text(csv_content)

            # Create test SQL file
            sql_file = Path(temp_dir) / 'transform.sql'
            sql_file.write_text('SELECT * FROM {{ table_name }} WHERE value > {{ min_value }}')

            # Create output directory
            output_dir = Path(temp_dir) / 'output'
            output_dir.mkdir()

            # Create ETL configuration
            config = {
                'pipeline': 'integration_test_spark',
                'engine': 'spark',
                'extract': [
                    {
                        'step': 'extract_data',
                        'path': str(input_file),
                        'df': 'raw_data',
                        'format': 'csv'
                    }
                ],
                'transform': [
                    {
                        'step': 'filter_data',
                        'sql_file': str(sql_file),
                        'inputs': {
                            'table_name': 'raw_data',
                            'min_value': 25
                        },
                        'df': 'filtered_data'
                    }
                ],
                'load': [
                    {
                        'step': 'load_data',
                        'df': 'filtered_data',
                        'mode': 'overwrite',
                        'format': 'parquet',
                        'path': str(output_dir / 'output.parquet')
                    }
                ]
            }

            # Write config to file
            config_file = Path(temp_dir) / 'config.yaml'
            with open(config_file, 'w') as f:
                yaml.dump(config, f)

            # Mock the ETL execution
            with patch('pipe_yaml.spark_etl.spark_context_manager') as mock_context, \
                    patch('argparse.ArgumentParser.parse_args') as mock_args, \
                    patch('pipe_yaml.etl_factory.EtlFactory.create_etl') as mock_create_etl:
                # Setup mocks
                mock_args.return_value = type('Args', (), {'etl_file': str(config_file)})()
                mock_context.return_value.__enter__ = MagicMock()
                mock_context.return_value.__exit__ = MagicMock()

                # Mock ETL creation and execution
                mock_etl_instance = MagicMock()
                mock_create_etl.return_value = mock_etl_instance

                # Execute the pipeline
                etl_main()

                # Verify the pipeline was created and run
                mock_create_etl.assert_called_once_with('spark')
                mock_etl_instance.run.assert_called_once()

    def test_complete_duckdb_pipeline_execution(self):
        """Test complete pipeline execution with DuckDB engine."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test input data as CSV string
            input_file = Path(temp_dir) / 'input.csv'
            csv_content = """id,name,value
1,Alice,10
2,Bob,20
3,Charlie,30
4,Diana,40
5,Eve,50"""
            input_file.write_text(csv_content)

            # Create output directory
            output_dir = Path(temp_dir) / 'output'
            output_dir.mkdir()

            # Create ETL configuration
            config = {
                'pipeline': 'integration_test_duckdb',
                'engine': 'duckdb',
                'extract': [
                    {
                        'step': 'extract_data',
                        'path': str(input_file),
                        'df': 'raw_data',
                        'format': 'csv'
                    }
                ],
                'transform': [
                    {
                        'step': 'filter_data',
                        'sql': 'SELECT * FROM raw_data WHERE value > 25',
                        'df': 'filtered_data'
                    }
                ],
                'load': [
                    {
                        'step': 'load_data',
                        'df': 'filtered_data',
                        'mode': 'overwrite',
                        'format': 'parquet',
                        'path': str(output_dir / 'output.parquet')
                    }
                ]
            }

            # Write config to file
            config_file = Path(temp_dir) / 'config.yaml'
            with open(config_file, 'w') as f:
                yaml.dump(config, f)

            # Mock the ETL execution
            with patch('argparse.ArgumentParser.parse_args') as mock_args, \
                    patch('pipe_yaml.duckdb_etl.duckdb.sql') as mock_duckdb_sql:
                # Setup mocks
                mock_args.return_value = type('Args', (), {'etl_file': str(config_file)})()

                # Mock DuckDB operations
                mock_duckdb_sql.return_value = MagicMock()

                # Mock the DuckDB ETL
                with patch('pipe_yaml.duckdb_etl.DuckdbETL') as mock_duckdb_etl:
                    mock_etl_instance = MagicMock()
                    mock_duckdb_etl.return_value = mock_etl_instance

                    # Execute the pipeline
                    etl_main()

                    # Verify the pipeline was created and run
                    mock_duckdb_etl.assert_called_once()
                    mock_etl_instance.run.assert_called_once()

    def test_pipeline_with_only_extract_and_load(self):
        """Test pipeline with only extract and load steps (no transform)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test input data as CSV string
            input_file = Path(temp_dir) / 'input.csv'
            csv_content = """id,name,value
1,A,100
2,B,200
3,C,300"""
            input_file.write_text(csv_content)

            # Create output directory
            output_dir = Path(temp_dir) / 'output'
            output_dir.mkdir()

            # Create ETL configuration
            config = {
                'pipeline': 'extract_load_only',
                'engine': 'duckdb',
                'extract': [
                    {
                        'step': 'extract_data',
                        'path': str(input_file),
                        'df': 'raw_data',
                        'format': 'csv'
                    }
                ],
                'load': [
                    {
                        'step': 'load_data',
                        'df': 'raw_data',
                        'mode': 'overwrite',
                        'format': 'csv',
                        'path': str(output_dir / 'output.csv')
                    }
                ]
            }

            # Write config to file
            config_file = Path(temp_dir) / 'config.yaml'
            with open(config_file, 'w') as f:
                yaml.dump(config, f)

            # Mock the ETL execution
            with patch('argparse.ArgumentParser.parse_args') as mock_args, \
                    patch('pipe_yaml.duckdb_etl.duckdb.sql') as mock_duckdb_sql:
                # Setup mocks
                mock_args.return_value = type('Args', (), {'etl_file': str(config_file)})()

                # Mock DuckDB operations
                mock_duckdb_sql.return_value = MagicMock()

                # Mock the DuckDB ETL
                with patch('pipe_yaml.duckdb_etl.DuckdbETL') as mock_duckdb_etl:
                    mock_etl_instance = MagicMock()
                    mock_duckdb_etl.return_value = mock_etl_instance

                    # Execute the pipeline
                    etl_main()

                    # Verify the pipeline was created and run
                    mock_duckdb_etl.assert_called_once()
                    mock_etl_instance.run.assert_called_once()

    def test_pipeline_with_only_transform_and_load(self):
        """Test pipeline with only transform and load steps (no extract)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create output directory
            output_dir = Path(temp_dir) / 'output'
            output_dir.mkdir()

            # Create ETL configuration
            config = {
                'pipeline': 'transform_load_only',
                'engine': 'duckdb',
                'transform': [
                    {
                        'step': 'create_data',
                        'sql': 'SELECT 1 as id, \'Test\' as name, 42 as value',
                        'df': 'generated_data'
                    }
                ],
                'load': [
                    {
                        'step': 'load_data',
                        'df': 'generated_data',
                        'mode': 'overwrite',
                        'format': 'csv',
                        'path': str(output_dir / 'output.csv')
                    }
                ]
            }

            # Write config to file
            config_file = Path(temp_dir) / 'config.yaml'
            with open(config_file, 'w') as f:
                yaml.dump(config, f)

            # Mock the ETL execution
            with patch('argparse.ArgumentParser.parse_args') as mock_args, \
                    patch('pipe_yaml.duckdb_etl.duckdb.sql') as mock_duckdb_sql, \
                    patch('pipe_yaml.etl_factory.EtlFactory.create_etl') as mock_create_etl:
                # Setup mocks
                mock_args.return_value = type('Args', (), {'etl_file': str(config_file)})()

                # Mock DuckDB operations
                mock_duckdb_sql.return_value = MagicMock()

                # Mock ETL creation and execution
                mock_etl_instance = MagicMock()
                mock_create_etl.return_value = mock_etl_instance

                # Execute the pipeline
                etl_main()

                # Verify the pipeline was created and run
                mock_create_etl.assert_called_once_with('duckdb')
                mock_etl_instance.run.assert_called_once()

    def test_pipeline_with_multiple_steps(self):
        """Test pipeline with multiple extract, transform, and load steps."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test input files as CSV strings
            input_file1 = Path(temp_dir) / 'input1.csv'
            csv_content1 = """id,name,value
1,A,10
2,B,20"""
            input_file1.write_text(csv_content1)

            input_file2 = Path(temp_dir) / 'input2.csv'
            csv_content2 = """id,name,value
3,C,30
4,D,40"""
            input_file2.write_text(csv_content2)

            # Create output directory
            output_dir = Path(temp_dir) / 'output'
            output_dir.mkdir()

            # Create ETL configuration
            config = {
                'pipeline': 'multiple_steps',
                'engine': 'duckdb',
                'extract': [
                    {
                        'step': 'extract_data1',
                        'path': str(input_file1),
                        'df': 'data1',
                        'format': 'csv'
                    },
                    {
                        'step': 'extract_data2',
                        'path': str(input_file2),
                        'df': 'data2',
                        'format': 'csv'
                    }
                ],
                'transform': [
                    {
                        'step': 'union_data',
                        'sql': 'SELECT * FROM data1 UNION ALL SELECT * FROM data2',
                        'df': 'combined_data'
                    },
                    {
                        'step': 'filter_data',
                        'sql': 'SELECT * FROM combined_data WHERE value > 15',
                        'df': 'filtered_data'
                    }
                ],
                'load': [
                    {
                        'step': 'load_csv',
                        'df': 'combined_data',
                        'mode': 'overwrite',
                        'format': 'csv',
                        'path': str(output_dir / 'combined.csv')
                    },
                    {
                        'step': 'load_parquet',
                        'df': 'filtered_data',
                        'mode': 'overwrite',
                        'format': 'parquet',
                        'path': str(output_dir / 'filtered.parquet')
                    }
                ]
            }

            # Write config to file
            config_file = Path(temp_dir) / 'config.yaml'
            with open(config_file, 'w') as f:
                yaml.dump(config, f)

            # Mock the ETL execution
            with patch('argparse.ArgumentParser.parse_args') as mock_args, \
                    patch('pipe_yaml.duckdb_etl.duckdb.sql') as mock_duckdb_sql, \
                    patch('pipe_yaml.etl_factory.EtlFactory.create_etl') as mock_create_etl:
                # Setup mocks
                mock_args.return_value = type('Args', (), {'etl_file': str(config_file)})()

                # Mock DuckDB operations
                mock_duckdb_sql.return_value = MagicMock()

                # Mock ETL creation and execution
                mock_etl_instance = MagicMock()
                mock_create_etl.return_value = mock_etl_instance

                # Execute the pipeline
                etl_main()

                # Verify the pipeline was created and run
                mock_create_etl.assert_called_once_with('duckdb')
                mock_etl_instance.run.assert_called_once()

    def test_error_handling_invalid_config(self):
        """Test error handling with invalid configuration file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create invalid YAML config
            config_file = Path(temp_dir) / 'invalid.yaml'
            config_file.write_text('invalid: yaml: content: [')

            with patch('argparse.ArgumentParser.parse_args') as mock_args:
                mock_args.return_value = type('Args', (), {'etl_file': str(config_file)})()

                # Should raise YAML error
                with pytest.raises(Exception):  # YAML parsing error
                    etl_main()

    def test_error_handling_invalid_engine(self):
        """Test error handling with invalid engine name."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create config with invalid engine
            config = {
                'pipeline': 'invalid_engine_test',
                'engine': 'invalid_engine',
                'extract': [
                    {
                        'step': 'extract_data',
                        'path': 'input.csv',
                        'df': 'data',
                        'format': 'csv'
                    }
                ]
            }

            config_file = Path(temp_dir) / 'config.yaml'
            with open(config_file, 'w') as f:
                yaml.dump(config, f)

            with patch('argparse.ArgumentParser.parse_args') as mock_args:
                mock_args.return_value = type('Args', (), {'etl_file': str(config_file)})()

                # Should raise ValueError for invalid engine
                with pytest.raises(ValueError, match="Invalid engine: invalid_engine"):
                    etl_main()

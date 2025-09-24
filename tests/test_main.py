from unittest.mock import patch, mock_open

import pytest
import yaml

from pipe_yaml.__main__ import load_pipeline, main
from pipe_yaml.models import Pipeline


class TestMainFunctionality:
    """Test the main entry point functionality."""

    @patch('yaml.safe_load')
    @patch('builtins.open', new_callable=mock_open)
    def test_load_pipeline_valid_yaml(self, mock_file, mock_yaml_safe_load):
        """Test loading pipeline from valid YAML configuration."""
        yaml_content = {
            'pipeline': 'test_pipeline',
            'engine': 'spark',
            'extract': [
                {
                    'step': 'extract_data',
                    'path': 'data/test.csv',
                    'df': 'raw_data',
                    'format': 'csv'
                }
            ],
            'load': [
                {
                    'step': 'load_data',
                    'df': 'raw_data',
                    'mode': 'overwrite',
                    'format': 'parquet',
                    'path': 'output/data.parquet'
                }
            ]
        }

        # Mock yaml.safe_load to return the parsed YAML content
        mock_yaml_safe_load.return_value = yaml_content

        args = type('Args', (), {'etl_file': 'test.yaml'})()
        pipeline = load_pipeline(args)

        # Verify that yaml.safe_load was called with the file object
        mock_yaml_safe_load.assert_called_once()

        assert isinstance(pipeline, Pipeline)
        assert pipeline.name == 'test_pipeline'
        assert pipeline.engine == 'spark'
        assert len(pipeline.extract_steps) == 1
        assert len(pipeline.load_steps) == 1

    @patch('yaml.safe_load')
    @patch('builtins.open', new_callable=mock_open)
    def test_load_pipeline_invalid_yaml(self, mock_file, mock_yaml_safe_load):
        """Test loading pipeline from invalid YAML configuration."""
        mock_yaml_safe_load.side_effect = yaml.YAMLError('Invalid YAML')

        args = type('Args', (), {'etl_file': 'test.yaml'})()

        with pytest.raises(yaml.YAMLError):
            load_pipeline(args)

    @patch('builtins.open', side_effect=FileNotFoundError)
    def test_load_pipeline_file_not_found(self, mock_file):
        """Test loading pipeline when file doesn't exist."""
        args = type('Args', (), {'etl_file': 'nonexistent.yaml'})()

        with pytest.raises(FileNotFoundError):
            load_pipeline(args)

    @patch('pipe_yaml.__main__.EtlFactory.create_etl')
    @patch('pipe_yaml.__main__.load_pipeline')
    def test_main_execution_success(self, mock_load_pipeline, mock_create_etl):
        """Test successful main execution."""
        # Setup mocks
        mock_pipeline = type('Pipeline', (), {
            'engine': 'spark',
            'name': 'test_pipeline'
        })()
        mock_load_pipeline.return_value = mock_pipeline

        mock_etl = type('MockETL', (), {
            'run': lambda self, pipeline: None
        })()
        mock_create_etl.return_value = mock_etl

        # Execute main
        main()

        # Verify calls
        mock_load_pipeline.assert_called_once()
        mock_create_etl.assert_called_once_with('spark')
        mock_etl.run.assert_called_once_with(mock_pipeline)

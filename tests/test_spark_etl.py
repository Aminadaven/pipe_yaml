from unittest.mock import patch, MagicMock

import pytest

from pipe_yaml.models import ExtractStep, TransformStep, LoadStep, Pipeline
from pipe_yaml.spark_etl import SparkETL


class TestSparkETL:
    """Test the Spark ETL implementation."""

    def test_spark_etl_initialization(self):
        """Test Spark ETL initialization."""
        etl = SparkETL()

        # Should inherit from BaseETL
        from pipe_yaml.base_etl import BaseETL
        assert isinstance(etl, BaseETL)

    def test_extract_step_csv_file(self):
        """Test extracting data from CSV file."""
        etl = SparkETL()

        step = ExtractStep(
            step='extract_csv',
            path='data/test.csv',
            df='raw_data',
            format='csv'
        )

        with patch.object(etl, '_spark') as mock_spark:
            mock_df = MagicMock()
            mock_spark.read.load.return_value = mock_df

            with patch.object(etl, '_prepare_df') as mock_prepare:
                mock_prepare.return_value = mock_df

                etl._extract_step(step)

                # Should read CSV file
                mock_spark.read.load.assert_called_once_with('data/test.csv', 'csv')

                # Should prepare DataFrame
                mock_prepare.assert_called_once_with(mock_df, 'raw_data', False)

    def test_transform_step_with_sql(self):
        """Test transforming data with SQL query."""
        etl = SparkETL()

        step = TransformStep(
            step='transform_sql',
            sql='SELECT * FROM input_table WHERE id > 10',
            df='output_table'
        )

        with patch.object(etl, '_spark') as mock_spark:
            mock_df = MagicMock()
            mock_spark.sql.return_value = mock_df

            with patch.object(etl, '_prepare_df') as mock_prepare:
                mock_prepare.return_value = mock_df

                etl._transform_step(step)

                # Should execute SQL
                mock_spark.sql.assert_called_once_with('SELECT * FROM input_table WHERE id > 10')

                # Should prepare output DataFrame
                mock_prepare.assert_called_once_with(mock_df, 'output_table', False)

    def test_load_step_overwrite_mode(self):
        """Test loading data with overwrite mode."""
        etl = SparkETL()

        step = LoadStep(
            step='load_overwrite',
            df='source_table',
            mode='overwrite',
            format='parquet',
            path='output/path'
        )

        with patch.object(etl, '_spark') as mock_spark:
            mock_df = MagicMock()
            mock_spark.sql.return_value = mock_df

            etl._load_step(step)

            # Should query the source table
            mock_spark.sql.assert_called_once_with('SELECT * FROM source_table')

            # Should write with overwrite mode
            mock_df.write.mode.assert_called_once_with('overwrite')
            mock_df.write.mode.return_value.format.assert_called_once_with('parquet')
            mock_df.write.mode.return_value.format.return_value.save.assert_called_once_with('output/path')

    def test_complete_spark_etl_flow(self):
        """Test a complete ETL flow with Spark."""
        etl = SparkETL()

        with patch('pipe_yaml.spark_etl.spark_context_manager') as mock_context_manager, \
                patch.object(etl, '_spark') as mock_spark:
            mock_spark.read.load.return_value = MagicMock()
            mock_spark.sql.return_value = MagicMock()

            # Mock the context manager
            mock_context_manager.return_value.__enter__ = MagicMock()
            mock_context_manager.return_value.__exit__ = MagicMock()

            # Create a complete pipeline
            pipeline = Pipeline(
                name='complete_test',
                engine='spark',
                extract_steps=[
                    ExtractStep(step='extract', path='input.csv', df='raw_data', format='csv')
                ],
                transform_steps=[
                    TransformStep(step='transform', sql='SELECT * FROM raw_data WHERE value > 0', df='filtered_data')
                ],
                load_steps=[
                    LoadStep(step='load', df='filtered_data', mode='overwrite', format='parquet', path='output.parquet')
                ]
            )

            with patch.object(etl, 'extract_data') as mock_extract, \
                    patch.object(etl, 'transform_data') as mock_transform, \
                    patch.object(etl, 'load_data') as mock_load:
                etl.run(pipeline)

                # Should call all ETL methods
                mock_extract.assert_called_once()
                mock_transform.assert_called_once()
                mock_load.assert_called_once()

    def test_error_handling_spark_session_creation(self):
        """Test error handling when Spark session creation fails."""
        etl = SparkETL()

        with patch('pipe_yaml.spark_etl.spark_context_manager', side_effect=Exception('Spark not available')):
            # Create a valid pipeline
            pipeline = Pipeline(
                name='test',
                engine='spark',
                transform_steps=[
                    TransformStep(step='dummy_transform', sql='SELECT 1', df='dummy_df')
                ]
            )

            with pytest.raises(Exception, match='Spark not available'):
                etl.run(pipeline)

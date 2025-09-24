import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from pipe_yaml.duckdb_etl import DuckdbETL
from pipe_yaml.models import ExtractStep, TransformStep, LoadStep


class TestDuckdbETL:
    """Test the DuckDB ETL implementation."""

    def test_duckdb_etl_initialization(self):
        """Test DuckDB ETL initialization."""
        etl = DuckdbETL()

        # Should inherit from BaseETL
        from pipe_yaml.base_etl import BaseETL
        assert isinstance(etl, BaseETL)

    def test_extract_step_csv_file(self):
        """Test extracting data from CSV file."""
        etl = DuckdbETL()

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a test CSV file as string
            csv_file = Path(temp_dir) / 'test_data.csv'
            csv_content = """id,name,value
1,Alice,10.5
2,Bob,20.5
3,Charlie,30.5"""
            csv_file.write_text(csv_content)

            step = ExtractStep(
                step='extract_csv',
                path=str(csv_file),
                df='test_table',
                format='csv'
            )

            # Mock duckdb.sql to avoid actual database operations
            with patch('pipe_yaml.duckdb_etl.duckdb.sql') as mock_duckdb_sql:
                mock_duckdb_sql.return_value = None

                etl._extract_step(step)

                # Should call duckdb.sql to create table
                mock_duckdb_sql.assert_called_once()
                call_args = str(mock_duckdb_sql.call_args)
                assert 'CREATE TABLE test_table' in call_args

    def test_transform_step_with_sql(self):
        """Test transforming data with SQL query."""
        etl = DuckdbETL()

        step = TransformStep(
            step='transform_sql',
            sql='SELECT * FROM input_table WHERE id > 10',
            df='output_table'
        )

        with patch('pipe_yaml.duckdb_etl.duckdb.sql') as mock_duckdb_sql:
            mock_duckdb_sql.return_value = None

            etl._transform_step(step)

            # Should call duckdb.sql with the SQL
            mock_duckdb_sql.assert_called_once_with('SELECT * FROM input_table WHERE id > 10')

    def test_load_step_parquet_format(self):
        """Test loading data to Parquet format."""
        etl = DuckdbETL()

        with tempfile.TemporaryDirectory() as temp_dir:
            output_file = Path(temp_dir) / 'output.parquet'

            step = LoadStep(
                step='load_parquet',
                df='source_table',
                mode='overwrite',
                format='parquet',
                path=str(output_file)
            )

            with patch('pipe_yaml.duckdb_etl.duckdb.sql') as mock_duckdb_sql:
                mock_df = MagicMock()
                mock_duckdb_sql.return_value = mock_df

                etl._load_step(step)

                # Should query the source table
                mock_duckdb_sql.assert_called_once_with('SELECT * FROM source_table')

                # Should write to parquet
                mock_df.write_parquet.assert_called_once_with(str(output_file))

    def test_complete_etl_flow(self):
        """Test a complete ETL flow with DuckDB."""
        etl = DuckdbETL()

        with patch('pipe_yaml.duckdb_etl.duckdb.sql') as mock_duckdb_sql:
            # Mock the database operations
            mock_duckdb_sql.return_value = MagicMock()

            # Test extract
            extract_step = ExtractStep(
                step='extract',
                path='input.csv',
                df='raw_data',
                format='csv'
            )
            etl._extract_step(extract_step)

            # Test transform
            transform_step = TransformStep(
                step='transform',
                sql='SELECT * FROM raw_data WHERE value > 0',
                df='filtered_data'
            )
            etl._transform_step(transform_step)

            # Test load
            load_step = LoadStep(
                step='load',
                df='filtered_data',
                mode='overwrite',
                format='parquet',
                path='output.parquet'
            )
            etl._load_step(load_step)

            # Verify all operations were called
            assert mock_duckdb_sql.call_count == 3

    def test_error_handling_invalid_sql(self):
        """Test error handling for invalid SQL."""
        etl = DuckdbETL()

        step = TransformStep(
            step='invalid_transform',
            sql='INVALID SQL SYNTAX',
            df='output_table'
        )

        with patch('pipe_yaml.duckdb_etl.duckdb.sql', side_effect=Exception('SQL Error')):
            with pytest.raises(Exception, match='SQL Error'):
                etl._transform_step(step)

    def test_error_handling_file_not_found(self):
        """Test error handling for missing files."""
        etl = DuckdbETL()

        # Test extract with non-existent file
        step = ExtractStep(
            step='extract_missing',
            path='nonexistent.csv',
            df='test_table',
            format='csv'
        )

        with patch('pipe_yaml.duckdb_etl.duckdb.sql', side_effect=Exception('File not found')):
            with pytest.raises(Exception, match='File not found'):
                etl._extract_step(step)

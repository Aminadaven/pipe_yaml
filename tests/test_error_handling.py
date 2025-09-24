import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from pipe_yaml.duckdb_etl import DuckdbETL
from pipe_yaml.etl_factory import EtlFactory
from pipe_yaml.models import Pipeline, ExtractStep, TransformStep, LoadStep
from pipe_yaml.spark_etl import SparkETL


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_pipeline_validation_empty_steps(self):
        """Test pipeline validation fails with completely empty configuration."""
        config = {
            "pipeline": "empty_pipeline",
            "engine": "spark"
        }

        with pytest.raises(ValueError, match="Invalid ETL configuration file"):
            Pipeline(**config)

    def test_pipeline_validation_only_engine(self):
        """Test pipeline validation fails with only engine specified."""
        config = {
            "pipeline": "engine_only",
            "engine": "duckdb",
            "extract": [],
            "transform": [],
            "load": []
        }

        with pytest.raises(ValueError, match="Invalid ETL configuration file"):
            Pipeline(**config)

    def test_extract_step_missing_required_fields(self):
        """Test extract step validation fails with missing required fields."""
        # Missing path
        with pytest.raises(ValueError):
            ExtractStep(step="test", df="test_df", format="csv")

        # Missing df
        with pytest.raises(ValueError):
            ExtractStep(step="test", path="test.csv", format="csv")

        # Missing format
        with pytest.raises(ValueError):
            ExtractStep(step="test", path="test.csv", df="test_df")

    def test_transform_step_missing_sql_and_sql_file(self):
        """Test transform step validation fails without sql or sql_file."""
        with pytest.raises(ValueError, match="Invalid Transformation"):
            TransformStep(step="test")

    def test_transform_step_both_sql_and_sql_file(self):
        """Test transform step validation fails with both sql and sql_file specified."""
        with pytest.raises(ValueError, match="Invalid Transformation"):
            TransformStep(
                step="test",
                sql="SELECT * FROM table",
                sql_file="queries/test.sql"
            )

    def test_load_step_missing_required_fields(self):
        """Test load step validation fails with missing required fields."""
        # Missing mode
        with pytest.raises(ValueError):
            LoadStep(step="test", df="test_df", format="parquet", path="output.parquet")

        # Missing format
        with pytest.raises(ValueError):
            LoadStep(step="test", df="test_df", mode="overwrite", path="output.parquet")

        # Missing path
        with pytest.raises(ValueError):
            LoadStep(step="test", df="test_df", mode="overwrite", format="parquet")

    def test_factory_invalid_engine_name(self):
        """Test ETL factory with invalid engine name."""
        with pytest.raises(ValueError, match="Invalid engine: nonexistent"):
            EtlFactory.create_etl("nonexistent")

    def test_factory_case_sensitive_engines(self):
        """Test ETL factory with different case variations."""
        invalid_engines = ["SPARK", "Spark", "DUCKDB", "DuckDb", "duckdb-engine"]

        for engine in invalid_engines:
            with pytest.raises(ValueError, match=f"Invalid engine: {engine}"):
                EtlFactory.create_etl(engine)

    def test_duckdb_extract_step_file_not_found(self):
        """Test DuckDB extract step with non-existent file."""
        etl = DuckdbETL()

        step = ExtractStep(
            step="extract_missing",
            path="nonexistent_file.csv",
            df="test_table",
            format="csv"
        )

        with patch('pipe_yaml.duckdb_etl.duckdb.sql', side_effect=Exception("File not found")):
            with pytest.raises(Exception, match="File not found"):
                etl._extract_step(step)

    def test_duckdb_transform_step_invalid_sql(self):
        """Test DuckDB transform step with invalid SQL."""
        etl = DuckdbETL()

        step = TransformStep(
            step="invalid_sql",
            sql="INVALID SQL SYNTAX HERE",
            df="output_table"
        )

        with patch('pipe_yaml.duckdb_etl.duckdb.sql', side_effect=Exception("SQL syntax error")):
            with pytest.raises(Exception, match="SQL syntax error"):
                etl._transform_step(step)

    def test_duckdb_load_step_unsupported_format(self):
        """Test DuckDB load step with unsupported format."""
        etl = DuckdbETL()

        step = LoadStep(
            step="load_unsupported",
            df="source_table",
            mode="overwrite",
            format="json",
            path="output.json"
        )

        # Mock the duckdb.sql call to avoid catalog error
        with patch('pipe_yaml.duckdb_etl.duckdb.sql') as mock_duckdb_sql:
            mock_duckdb_sql.side_effect = Exception("Table with name source_table does not exist!")

            with pytest.raises(ValueError, match="Unsupported target type: json"):
                etl._load_step(step)

    def test_spark_extract_step_file_not_found(self):
        """Test Spark extract step with non-existent file."""
        etl = SparkETL()

        step = ExtractStep(
            step="extract_missing",
            path="nonexistent_file.csv",
            df="test_table",
            format="csv"
        )

        with patch.object(etl, '_spark') as mock_spark:
            mock_spark.read.load.side_effect = Exception("File not found")

            with pytest.raises(Exception, match="File not found"):
                etl._extract_step(step)

    def test_spark_transform_step_invalid_sql(self):
        """Test Spark transform step with invalid SQL."""
        etl = SparkETL()

        step = TransformStep(
            step="invalid_sql",
            sql="INVALID SQL SYNTAX HERE",
            df="output_table"
        )

        with patch.object(etl, '_spark') as mock_spark:
            mock_spark.sql.side_effect = Exception("SQL syntax error")

            with pytest.raises(Exception, match="SQL syntax error"):
                etl._transform_step(step)

    def test_spark_context_creation_failure(self):
        """Test Spark context creation failure."""
        etl = SparkETL()

        # Create a valid pipeline first
        pipeline = Pipeline(
            name="test_pipeline",
            engine="spark",
            transform_steps=[
                TransformStep(step="dummy_transform", sql="SELECT 1", df="dummy_df")
            ]
        )

        with patch('pipe_yaml.spark_etl.spark_context_manager', side_effect=Exception("Spark not available")):
            with pytest.raises(Exception, match="Spark not available"):
                etl.run(pipeline)

    def test_empty_extract_steps_list(self):
        """Test ETL execution with empty extract steps list."""
        etl = DuckdbETL()

        pipeline = Pipeline(
            name="empty_extract",
            engine="duckdb",
            extract_steps=[],
            transform_steps=[
                TransformStep(step="create", sql="SELECT 1 as id", df="data")
            ],
            load_steps=[
                LoadStep(step="load", df="data", mode="overwrite", format="csv", path="output.csv")
            ]
        )

        with patch.object(etl, 'extract_data') as mock_extract, \
                patch.object(etl, 'transform_data') as mock_transform, \
                patch.object(etl, 'load_data') as mock_load:
            etl.run(pipeline)

            # Should call extract_data with empty list
            mock_extract.assert_called_once_with([])
            mock_transform.assert_called_once()
            mock_load.assert_called_once()

    def test_empty_transform_steps_list(self):
        """Test ETL execution with empty transform steps list."""
        etl = DuckdbETL()

        # Create a valid pipeline with extract and load steps
        pipeline = Pipeline(
            name="empty_transform",
            engine="duckdb",
            extract_steps=[
                ExtractStep(step="extract", path="input.csv", df="raw_data", format="csv")
            ],
            transform_steps=[],  # Empty transform steps
            load_steps=[
                LoadStep(step="load", df="raw_data", mode="overwrite", format="csv", path="output.csv")
            ]
        )

        with patch.object(etl, 'extract_data') as mock_extract, \
                patch.object(etl, 'transform_data') as mock_transform, \
                patch.object(etl, 'load_data') as mock_load:
            etl.run(pipeline)

            mock_extract.assert_called_once()
            # Should call transform_data with empty list
            mock_transform.assert_called_once_with([])
            mock_load.assert_called_once()

    def test_empty_load_steps_list(self):
        """Test ETL execution with empty load steps list."""
        etl = DuckdbETL()

        pipeline = Pipeline(
            name="empty_load",
            engine="duckdb",
            extract_steps=[
                ExtractStep(step="extract", path="input.csv", df="raw_data", format="csv")
            ],
            transform_steps=[
                TransformStep(step="transform", sql="SELECT * FROM raw_data", df="processed_data")
            ],
            load_steps=[]
        )

        with patch.object(etl, 'extract_data') as mock_extract, \
                patch.object(etl, 'transform_data') as mock_transform, \
                patch.object(etl, 'load_data') as mock_load:
            etl.run(pipeline)

            mock_extract.assert_called_once()
            mock_transform.assert_called_once()
            # Should call load_data with empty list
            mock_load.assert_called_once_with([])

    def test_pipeline_with_special_characters_in_names(self):
        """Test pipeline execution with special characters in step names."""
        etl = DuckdbETL()

        pipeline = Pipeline(
            name="test_pipeline_with_special_chars_!@#$%",
            engine="duckdb",
            extract_steps=[
                ExtractStep(step="extract-step_with-dashes", path="input.csv", df="df-with-dashes", format="csv")
            ],
            transform_steps=[
                TransformStep(step="transform.step.with.dots", sql="SELECT * FROM `df-with-dashes`", df="output-table")
            ],
            load_steps=[
                LoadStep(step="load_step_with_underscores", df="output-table", mode="overwrite", format="parquet",
                         path="output.parquet")
            ]
        )

        with patch.object(etl, 'extract_data') as mock_extract, \
                patch.object(etl, 'transform_data') as mock_transform, \
                patch.object(etl, 'load_data') as mock_load:
            etl.run(pipeline)

            # Should execute all steps successfully
            mock_extract.assert_called_once()
            mock_transform.assert_called_once()
            mock_load.assert_called_once()

    def test_pipeline_with_unicode_characters(self):
        """Test pipeline execution with Unicode characters."""
        etl = DuckdbETL()

        pipeline = Pipeline(
            name="测试管道",  # Chinese characters
            engine="duckdb",
            extract_steps=[
                ExtractStep(step="提取数据", path="input.csv", df="原始数据", format="csv")
            ],
            transform_steps=[
                TransformStep(step="转换数据", sql="SELECT * FROM `原始数据` WHERE id > 0", df="处理数据")
            ],
            load_steps=[
                LoadStep(step="加载数据", df="处理数据", mode="overwrite", format="parquet", path="输出.parquet")
            ]
        )

        with patch.object(etl, 'extract_data') as mock_extract, \
                patch.object(etl, 'transform_data') as mock_transform, \
                patch.object(etl, 'load_data') as mock_load:
            etl.run(pipeline)

            # Should execute all steps successfully despite Unicode
            mock_extract.assert_called_once()
            mock_transform.assert_called_once()
            mock_load.assert_called_once()

    def test_transform_step_with_empty_inputs(self):
        """Test transform step with empty inputs dictionary."""
        etl = DuckdbETL()

        with tempfile.TemporaryDirectory() as temp_dir:
            sql_file = Path(temp_dir) / 'empty_inputs.sql'
            sql_file.write_text('SELECT * FROM {{ table_name }}')

            step = TransformStep(
                step="empty_inputs",
                sql_file=str(sql_file),
                inputs={},  # Empty inputs
                df="output_table"
            )

            with patch('builtins.open', create=True) as mock_open_file, \
                    patch('pipe_yaml.duckdb_etl.duckdb.sql') as mock_duckdb_sql:
                mock_open_file.return_value.__enter__.return_value.read.return_value = \
                    'SELECT * FROM {{ table_name }}'

                etl._transform_step(step)

                # Should render template successfully even with empty inputs
                mock_duckdb_sql.assert_called_once()

    def test_load_step_with_empty_options(self):
        """Test load step with empty options dictionary."""
        etl = DuckdbETL()

        step = LoadStep(
            step="empty_options",
            df="source_table",
            mode="overwrite",
            format="parquet",
            path="output.parquet",
            options={}  # Empty options
        )

        with patch('pipe_yaml.duckdb_etl.duckdb.sql') as mock_duckdb_sql:
            mock_df = MagicMock()
            mock_duckdb_sql.return_value = mock_df

            etl._load_step(step)

            # Should write successfully with empty options
            mock_df.write_parquet.assert_called_once_with("output.parquet")

    def test_extract_step_with_empty_options(self):
        """Test extract step with empty options dictionary."""
        etl = DuckdbETL()

        step = ExtractStep(
            step="empty_options",
            path="input.csv",
            df="test_table",
            format="csv",
            options={}  # Empty options
        )

        with patch('pipe_yaml.duckdb_etl.duckdb.sql') as mock_duckdb_sql:
            mock_duckdb_sql.return_value = None

            etl._extract_step(step)

            # Should extract successfully with empty options
            mock_duckdb_sql.assert_called_once()

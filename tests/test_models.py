import pytest

from pipe_yaml.models import Pipeline, ExtractStep, TransformStep


class TestPipeline:
    def test_pipeline_creation(self):
        """Test that a pipeline can be created with valid configuration."""
        config = {
            "pipeline": "test_pipeline",
            "engine": "spark",
            "extract": [
                {
                    "step": "extract_data",
                    "path": "data/sales.csv",
                    "df": "raw_sales",
                    "format": "csv"
                }
            ],
            "load": [
                {
                    "step": "load_data",
                    "df": "raw_sales",
                    "mode": "overwrite",
                    "format": "parquet",
                    "path": "output/sales.parquet"
                }
            ]
        }

        pipeline = Pipeline(**config)

        assert pipeline.name == "test_pipeline"
        assert pipeline.engine == "spark"
        assert len(pipeline.extract_steps) == 1
        assert len(pipeline.load_steps) == 1
        assert len(pipeline.transform_steps) == 0

    def test_pipeline_validation_fails_without_steps(self):
        """Test that pipeline validation fails when no ETL steps are provided."""
        config = {
            "pipeline": "invalid_pipeline",
            "engine": "spark"
        }

        with pytest.raises(ValueError, match="Invalid ETL configuration file"):
            Pipeline(**config)

    def test_extract_step_creation(self):
        """Test that an extract step can be created."""
        step_config = {
            "step": "extract_csv",
            "path": "data/test.csv",
            "df": "test_data",
            "format": "csv"
        }

        step = ExtractStep(**step_config)

        assert step.name == "extract_csv"
        assert step.path == "data/test.csv"
        assert step.df == "test_data"
        assert step.format == "csv"
        assert step.cache is False

    def test_transform_step_validation(self):
        """Test that transform step validation works correctly."""
        # Valid transform step with sql
        step_config = {
            "step": "transform_data",
            "sql": "SELECT * FROM raw_data WHERE id > 100",
            "df": "filtered_data"
        }

        step = TransformStep(**step_config)
        assert step.sql == "SELECT * FROM raw_data WHERE id > 100"
        assert step.df == "filtered_data"

        # Invalid transform step without sql or sql_file
        invalid_config = {
            "step": "invalid_transform",
            "df": "output_data"
        }

        with pytest.raises(ValueError, match="Invalid Transformation"):
            TransformStep(**invalid_config)

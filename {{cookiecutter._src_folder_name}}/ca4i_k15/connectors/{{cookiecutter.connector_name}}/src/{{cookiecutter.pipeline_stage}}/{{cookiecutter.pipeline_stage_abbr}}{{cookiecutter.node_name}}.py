from pyspark.sql import DataFrame


def {{cookiecutter.pipeline_stage_abbr}}{{cookiecutter.node_name}}:({{cookiecutter.raw_data_abbr}}{{cookiecutter.node_name}}: DataFrame) -> DataFrame:
    pass

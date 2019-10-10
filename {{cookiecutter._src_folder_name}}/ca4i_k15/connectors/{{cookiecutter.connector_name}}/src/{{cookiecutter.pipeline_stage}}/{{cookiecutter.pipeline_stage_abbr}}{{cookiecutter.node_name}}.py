from pyspark.sql import DataFrame


def {{cookiecutter.pipeline_stage_abbr}}{{cookiecutter.node_name}}({{cookiecutter.raw_data_abbr}}{{cookiecutter.node_name}}: DataFrame) -> DataFrame:
    """
    This function runs the {{cookiecutter.pipeline_stage}} node for {{cookiecutter.connector_name}} {{cookiecutter.node_name}}.

    Args:
        {{cookiecutter.raw_data_abbr}}{{cookiecutter.node_name}} (DataFrame): Raw data frame for {{cookiecutter.connector_name}} {{cookiecutter.node_name}}.

    Returns:
        DataFrame: Returns the {{cookiecutter.pipeline_stage}} data frame for {{cookiecutter.connector_name}} {{cookiecutter.node_name}}.

    """

    pass

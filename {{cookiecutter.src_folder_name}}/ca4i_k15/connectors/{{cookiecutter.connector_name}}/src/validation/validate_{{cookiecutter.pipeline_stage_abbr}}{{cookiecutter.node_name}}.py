from ca4i_k15.custom_functions.validate_datasets import validate_datasets


def validate_{{cookiecutter.pipeline_stage_abbr}}{{cookiecutter.node_name}}({{cookiecutter.pipeline_stage_abbr}}{{cookiecutter.node_name}}, {{cookiecutter.ref_data_abbr}}{{cookiecutter.pipeline_stage_abbr}}{{cookiecutter.node_name}}):
    """
    This function is to validate the {{pipeline_stage}} {{cookiecutter.node_name}}.

    Args:
        {{cookiecutter.pipeline_stage_abbr}}{{cookiecutter.node_name}} (DataFrame): {{pipeline_stage}} dataframe for {{connector_name}} {{node_name}}.
        {{cookiecutter.ref_data_abbr}}{{cookiecutter.pipeline_stage_abbr}}{{cookiecutter.node_name}} (DataFrame): Reference {{pipeline_stage}} dataframe for {{connector_name}} {{node_name}}

    Returns:
        None

    """

    validate_datasets({{cookiecutter.pipeline_stage_abbr}}{{cookiecutter.node_name}}, {{cookiecutter.ref_data_abbr}}{{cookiecutter.pipeline_stage_abbr}}{{cookiecutter.node_name}})

# First get cookiecutter context dictionary:
from cookiecutter.main import cookiecutter
from cookiecutter.prompt import read_user_yes_no, read_user_variable

context = {{cookiecutter}}

if read_user_yes_no("Should i try to append the new node to pipeline.yml?", default_value='no'):
    import os
    import shutil

    # get pipeline directory
    package_dir = '{{cookiecutter._template}}'
    pipeline_file = os.path.join(package_dir, "{{cookiecutter._src_folder_name}}", "ca4i_k15", "connectors",
                                 "{{cookiecutter.connector_name}}", "conf", "pipelines", "pipeline.yml")

    # backup pipeline.yml
    if os.path.isfile(pipeline_file):
        import datetime
        dt = datetime.datetime.utcnow().isoformat()
        pipeline_bu = os.path.join(package_dir, "{{cookiecutter._src_folder_name}}", "ca4i_k15", "connectors",
                                   "{{cookiecutter.connector_name}}", "conf", "pipelines", "bu",
                                   dt, "pipeline.yml")

        shutil.copy(pipeline_file, pipeline_bu)
    else:
        with open(pipeline_file, 'a'):
            os.utime(pipeline_file, None)

    # Parse pipeline.yml
    import yaml
    with open(pipeline_file, 'r') as stream:
        try:
            pipelines = yaml.safe_load(stream)
            print("Parsed pipelines from pipeline.yml:")
            print(pipelines)

            for pipeline in pipelines:
                probable_pipeline_name = "{{cookiecutter.connector_name}}" + "_" + "{{cookiecutter.pipeline_stage}}"
                probable_node_name = "{{cookiecutter.pipeline_stage}}" + "/" + "{{cookiecutter.pipeline_stage_abbr}}" + "{{cookiecutter.node_name}}"
                if not pipeline[probable_pipeline_name]:
                    print("Created pipeline {} on pipeline.yml".format(probable_pipeline_name))
                    pipeline[probable_pipeline_name] = []
                if not pipeline[probable_pipeline_name][probable_node_name]:
                    print("Created node {} on pipeline {} on pipeline.yml".format(probable_node_name, probable_pipeline_name))
                    pipeline[probable_pipeline_name] = []

            with open(pipeline_file, "w") as f:
                yaml.dump(pipelines, f)

        except yaml.YAMLError as exc:
            print(exc)


if read_user_yes_no("Want to create an additional node?", default_value='no'):
    new_node_name = read_user_variable("node_name", None)
    if not new_node_name:
        print("Skipping new node creation!")
    else:
        print("Creating new node!")
        new_context = context = context.copy()
        new_context["node_name"] = new_node_name
        cookiecutter(
            'k15-connector-cookiecutter',
            extra_context=new_context,
            no_input=True,
            output_dir='../',
            overwrite_if_exists=True
        )

print("All done!")

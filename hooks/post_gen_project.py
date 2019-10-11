# First get cookiecutter context dictionary:
from cookiecutter.main import cookiecutter
from cookiecutter.prompt import read_user_yes_no, read_user_variable
import os
import yaml

cookiecutter_context = {{cookiecutter}}


def build_initial_pipeline_config():
    initial_pipelines = dict()
    initial_pipelines["{{cookiecutter.connector_name}}_{{cookiecutter.pipeline_stage}}"]["nodes"] = []
    initial_pipelines["{{cookiecutter.connector_name}}_validation"]["nodes"] = []
    initial_pipelines["{{cookiecutter.connector_name}}"]["pipelines"] = [
        "{{cookiecutter.connector_name}}_{{cookiecutter.pipeline_stage}}",
        "{{cookiecutter.connector_name}}_validation"
    ]
    return initial_pipelines


def should_create_additional_nodes(context):
    if not read_user_yes_no("Want to create an additional node?", default_value='no'):
        print("All nodes added now!")
        return

    new_node_name = read_user_variable("node_name", None)
    if not new_node_name:
        print("Skipping new node creation!")
    else:
        print("Creating node: " + new_node_name)
        new_context = context = context.copy()
        new_context["node_name"] = new_node_name
        cookiecutter(
            'k15-connector-cookiecutter',
            extra_context=new_context,
            no_input=True,
            output_dir='../',
            overwrite_if_exists=True
        )


def create_or_copy_pipeline_file(pipeline_file):
    import shutil

    # backup pipeline.yml
    if os.path.isfile(pipeline_file):
        import datetime
        dt = datetime.datetime.utcnow().isoformat()
        pipeline_bu = os.path.join(os.getcwd(), "ca4i_k15", "connectors",
                                   "{{cookiecutter.connector_name}}", "conf", "pipelines", "bu",
                                   dt, "pipeline.yml")
        os.makedirs(pipeline_bu)
        shutil.copy(pipeline_file, pipeline_bu)
    else:
        print("No pipeline.yml found. Creating a new one!")
        if not os.path.exists(os.path.dirname(pipeline_file)):
            os.makedirs(os.path.dirname(pipeline_file))
        os.mknod(pipeline_file)
        with open(pipeline_file, "w") as f:
            yaml.dump(build_initial_pipeline_config(), f)


def parse_pipeline_file(pipeline_file):
    with open(pipeline_file, 'r') as stream:
        try:
            pipelines = yaml.safe_load(stream)
            print("Parsed pipelines from pipeline.yml:" + str(pipelines))

            if not pipelines:
                print("Could not parse yml on pipeline.yml")
                return

            for pipeline in pipelines:
                probable_pipeline_name = "{{cookiecutter.connector_name}}" + "_" + "{{cookiecutter.pipeline_stage}}"
                probable_node_name = "{{cookiecutter.pipeline_stage}}" + "/" + "{{cookiecutter.pipeline_stage_abbr}}" + "{{cookiecutter.node_name}}"
                if not pipeline[probable_pipeline_name]:
                    print("Created pipeline {} on pipeline.yml".format(probable_pipeline_name))
                    pipeline[probable_pipeline_name] = []
                if not pipeline[probable_pipeline_name]["nodes"][probable_node_name]:
                    print("Created node {} on pipeline {} on pipeline.yml".format(probable_node_name,
                                                                                  probable_pipeline_name))
                    pipeline[probable_pipeline_name]["nodes"] = pipeline[probable_pipeline_name][
                        "nodes"].append(probable_node_name)

            with open(pipeline_file, "w") as f:
                yaml.dump(pipelines, f)

        except yaml.YAMLError as exc:
            print("Exception while trying to parse pipeline.yml")
            print(exc)


def should_add_to_pipeline():
    if not read_user_yes_no("Should i try to append the new node to pipeline.yml?", default_value='no'):
        print("Skipping node addition to pipeline.yml")
        return

    pipeline_file_path = os.path.join(os.getcwd(), "ca4i_k15", "connectors",
                                      "{{cookiecutter.connector_name}}", "conf", "pipelines", "pipeline.yml")
    create_or_copy_pipeline_file(pipeline_file_path)
    parse_pipeline_file(pipeline_file_path)


should_add_to_pipeline()
should_create_additional_nodes(cookiecutter_context)

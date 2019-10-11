from cookiecutter.main import cookiecutter
from cookiecutter.prompt import read_user_yes_no, read_user_variable
import os
import yaml

cookiecutter_context = {{cookiecutter}}


def build_initial_pipeline_config():
    initial_pipelines = {"{{cookiecutter.connector_name}}_{{cookiecutter.pipeline_stage}}": {"nodes": []},
                         "{{cookiecutter.connector_name}}_validation": {"nodes": []},
                         "{{cookiecutter.connector_name}}": {"pipelines": []}
                         }
    return initial_pipelines


def should_create_additional_nodes(context):
    if not read_user_yes_no("Want to create an additional node?", default_value='no'):
        print("All nodes added now!")
        exit(0)

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
            print(" - Parsed pipelines from pipeline.yml!")

            if not pipelines:
                print("Error: Could not parse yml on pipeline.yml. Skipping creation.")
                return

            connector = "{{cookiecutter.connector_name}}"
            connector_stage = "{{cookiecutter.connector_name}}" + "_" + "{{cookiecutter.pipeline_stage}}"
            connector_validate = "{{cookiecutter.connector_name}}" + "_validation"
            connector_stage_node = "{{cookiecutter.pipeline_stage}}" + "/" + "{{cookiecutter.pipeline_stage_abbr}}" + "{{cookiecutter.node_name}}"
            connector_validate_node = "validation/validate_" + "{{cookiecutter.pipeline_stage_abbr}}" + "{{cookiecutter.node_name}}"

            if not pipelines[connector]:
                print("Could not find the expected pipeline on pipelines.yml for connector {}".format(connector))
            else:
                pipelines[connector]["pipelines"] = [connector_stage, connector_validate]
                print(" - Created master pipeline {} for pipeline {} on pipeline.yml".format(connector_stage_node, connector_stage))

            if not pipelines[connector_stage]:
                print("Could not find the expected pipeline on pipelines.yml for stage {}".format(connector_stage))
            else:
                nodes = pipelines[connector_stage]["nodes"]
                if nodes:
                    pipelines[connector_stage]["nodes"] = list(nodes).append(connector_stage_node)
                else:
                    pipelines[connector_stage]["nodes"] = [connector_stage_node]
                print(" - Created node {} for pipeline {} on pipeline.yml".format(connector_stage_node, connector_stage))

            if not pipelines[connector_validate]:
                print("Could not find the expected validate pipeline on pipelines.yml for stage {}".format(connector_validate))
            else:
                nodes = pipelines[connector_validate]["nodes"]
                if nodes:
                    pipelines[connector_validate]["nodes"] = list(nodes).append(connector_validate_node)
                else:
                    pipelines[connector_validate]["nodes"] = [connector_validate_node]
                print(" - Created node {} for validation pipeline {} on pipeline.yml".format(connector_validate_node, connector_validate))

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

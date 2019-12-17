from cookiecutter.main import cookiecutter
from cookiecutter.prompt import read_user_yes_no, read_user_variable
import os
import yaml
import datetime

CC_CONTEXT = {{cookiecutter}}
DT = datetime.datetime.utcnow().isoformat()
FILE_PATH = "s3a://$s3_bucket/$project_directory/"


def get_stage_number(stage):
    if stage == "intermediate":
        return "02-int"
    if stage == "primary":
        return "03-prm"
    if stage == "feature":
        return "04-ftr"
    if stage == "master":
        return "05-mst"
    else:
        return "0x-{{cookiecutter.pipeline_stage_abbr[-1]}}"


USR_PATH = "$s3_user/" + get_stage_number(
    "{{cookiecutter.pipeline_stage}}") + "/{{cookiecutter.connector_name}}/{{cookiecutter.pipeline_stage_abbr}}{{cookiecutter.node_name}}"
RAW_PATH = "$s3_user_raw/{{cookiecutter.connector_name}}/{{cookiecutter.pipeline_stage_abbr}}{{cookiecutter.node_name}}.csv?"
REF_PATH = "$s3_user_ref/" + get_stage_number(
    "{{cookiecutter.pipeline_stage}}") + "/{{cookiecutter.connector_name}}/{{cookiecutter.pipeline_stage_abbr}}{{cookiecutter.node_name}}"
TYPE = "kedro.contrib.io.pyspark.SparkDataSet"

INITIAL_PIPELINES = {"{{cookiecutter.connector_name}}_{{cookiecutter.pipeline_stage}}": {"nodes": []},
                     "{{cookiecutter.connector_name}}_validation": {"nodes": []},
                     "{{cookiecutter.connector_name}}": {"pipelines": []}
                     }

RAW_CATALOG_KEYS = {"type": TYPE,
                    "file_format": "csv",
                    "filepath": FILE_PATH + RAW_PATH,
                    "load_args":
                        {"sep": "|",
                         "header": True,
                         "inferSchema": True}
                    }


def get_catalog_keys(usr_path):
    return {"type": TYPE,
            "file_format": "parquet",
            "filepath": FILE_PATH + usr_path,
            "save_args": {"mode": "overwrite"}
            }


BASE_CATALOG = {"{{cookiecutter.pipeline_stage_abbr}}{{cookiecutter.node_name}}": get_catalog_keys(USR_PATH)}
REF_CATALOG = {
    "{{cookiecutter.ref_data_abbr}}{{cookiecutter.pipeline_stage_abbr}}{{cookiecutter.node_name}}": get_catalog_keys(
        REF_PATH)}


def get_initial_catalog(pipeline_stage):
    if pipeline_stage == "intermediate":
        return {"{{cookiecutter.raw_data_abbr}}{{cookiecutter.node_name}}": RAW_CATALOG_KEYS,
                "{{cookiecutter.pipeline_stage_abbr}}{{cookiecutter.node_name}}": get_catalog_keys(USR_PATH)}
    else:
        return {"{{cookiecutter.pipeline_stage_abbr}}{{cookiecutter.node_name}}": get_catalog_keys(USR_PATH)}


def should_create_additional_nodes(context):
    if not read_user_yes_no("Want to create an additional node?", default_value='no'):
        print("All nodes added now!")
        exit(0)

    new_node_name = read_user_variable("node_name", None)
    if not new_node_name:
        print("Skipping new node creation!")
    else:
        new_context = context.copy()
        new_context["node_name"] = new_node_name
        cookiecutter(
            'k15-connector-cookiecutter',
            extra_context=new_context,
            no_input=True,
            output_dir='../',
            overwrite_if_exists=True
        )


def create_or_copy_yml_file(file, backup_file, initial_yml, name):
    import shutil

    # make a backup!
    if os.path.isfile(file) and backup_file:
        os.makedirs(backup_file)
        shutil.copy(file, backup_file)
    else:
        print("No {} found. Creating a new one!".format(name))
        if not os.path.exists(os.path.dirname(file)):
            os.makedirs(os.path.dirname(file))
        os.mknod(file)
        with open(file, "w") as f:
            yaml.dump(initial_yml, f)


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

            # Master pipeline
            if connector not in pipelines:
                print(" - Could not find the expected pipeline on pipelines.yml for connector {}. Creating it!".format(
                    connector))
                pipelines[connector] = {"pipelines": []}
            if connector_stage not in pipelines[connector]["pipelines"]:
                pipelines[connector]["pipelines"].append(connector_stage)
            if connector_validate not in pipelines[connector]["pipelines"]:
                pipelines[connector]["pipelines"].append(connector_validate)
            print(" - Created master pipeline {} for pipeline {} on pipeline.yml".format(connector_stage_node,
                                                                                         connector_stage))
            # Stage
            if connector_stage not in pipelines:
                print(" - Could not find the expected pipeline on pipelines.yml for stage {}. Creating it!".format(
                    connector_stage))
                pipelines[connector_stage] = {"nodes": []}
            nodes = pipelines[connector_stage]["nodes"]
            if nodes:
                print(" - Pipeline already has nodes: " + str(nodes))
                pipelines[connector_stage]["nodes"].append(connector_stage_node)
            else:
                pipelines[connector_stage]["nodes"] = [connector_stage_node]
            print(
                " - Created node {} for pipeline {} on pipeline.yml".format(connector_stage_node, connector_stage))

            # Validate
            if connector_validate not in pipelines:
                print(
                    " - Could not find the expected validate pipeline on pipelines.yml for stage {}. Creating it!".format(
                        connector_validate))
                pipelines[connector_validate] = {"nodes": []}
            nodes = pipelines[connector_validate]["nodes"]
            if nodes:
                print(" - Pipeline already has nodes: " + str(nodes))
                pipelines[connector_validate]["nodes"].append(connector_validate_node)
            else:
                pipelines[connector_validate]["nodes"] = [connector_validate_node]
            print(" - Created node {} for validation pipeline {} on pipeline.yml".format(connector_validate_node,
                                                                                         connector_validate))

            with open(pipeline_file, "w") as f:
                yaml.dump(pipelines, f)

        except yaml.YAMLError as exc:
            print("Exception while trying to parse pipeline.yml")
            print(exc)


def parse_catalog_file(catalog_file, config_keys, is_ref=False):
    with open(catalog_file, 'r') as stream:
        try:
            catalogs = yaml.safe_load(stream)
            print(" - Parsed catalog from catalog.yml!")

            if not catalogs:
                print("Error: Could not parse yml on catalog.yml. Skipping creation.")
                return

            ds = "{{cookiecutter.pipeline_stage_abbr}}" + "{{cookiecutter.node_name}}"
            if is_ref:
                ds = "{{cookiecutter.ref_data_abbr}}" + ds

            if ds not in catalogs:
                print(" - Could not find the expected datasource on catalog.yml for ds {}. Creating it!".format(ds))
                catalogs[ds] = config_keys

            with open(catalog_file, "w") as f:
                yaml.dump(catalogs, f)

        except yaml.YAMLError as exc:
            print("Exception while trying to parse catalog.yml")
            print(exc)


def should_add_to_pipeline():
    if not read_user_yes_no("Should i try to append the new node to pipeline.yml?", default_value='yes'):
        print("Skipping node addition to pipeline.yml")
        return

    pipeline_file_path = os.path.join(os.getcwd(), "ca4i_k15", "connectors",
                                      "{{cookiecutter.connector_name}}", "conf", "pipelines", "pipeline.yml")
    pipeline_bu = os.path.join(os.getcwd(), "ca4i_k15", "connectors",
                               "{{cookiecutter.connector_name}}", "conf", "pipelines", "bu",
                               DT, "pipeline.yml")
    create_or_copy_yml_file(pipeline_file_path, pipeline_bu, INITIAL_PIPELINES, "pipeline.yml")
    parse_pipeline_file(pipeline_file_path)


def should_add_to_catalog():
    if not read_user_yes_no("Should i try to append the new node to catalog_connector.yml?", default_value='yes'):
        print("Skipping node addition to catalog_connector.yml")
        return

    catalog_file_path = os.path.join(os.getcwd(), "ca4i_k15", "connectors",
                                     "{{cookiecutter.connector_name}}", "conf", "catalogs",
                                     "catalog_{{cookiecutter.connector_name}}.yml")
    catalog_bu = os.path.join(os.getcwd(), "ca4i_k15", "connectors",
                              "{{cookiecutter.connector_name}}", "conf", "catalogs", "bu",
                              DT, "catalog_{{cookiecutter.connector_name}}.yml")
    catalog_ref_file_path = os.path.join(os.getcwd(), "ca4i_k15", "connectors",
                                         "{{cookiecutter.connector_name}}", "conf", "catalogs",
                                         "catalog_{{cookiecutter.connector_name}}_ref.yml")
    catalog_ref_bu = os.path.join(os.getcwd(), "ca4i_k15", "connectors",
                                  "{{cookiecutter.connector_name}}", "conf", "catalogs", "bu",
                                  DT, "catalog_{{cookiecutter.connector_name}}_ref.yml")

    create_or_copy_yml_file(catalog_file_path, catalog_bu, get_initial_catalog("{{cookiecutter.pipeline_stage}}"),
                            "catalog_{{cookiecutter.connector_name}}.yml")
    create_or_copy_yml_file(catalog_ref_file_path, catalog_ref_bu, REF_CATALOG,
                            "catalog_{{cookiecutter.connector_name}}_ref.yml")

    parse_catalog_file(catalog_file_path, get_catalog_keys(USR_PATH), is_ref=False)
    parse_catalog_file(catalog_ref_file_path, get_catalog_keys(REF_PATH), is_ref=True)


def open_in_pycharm():
    print(" - Trying to open new node files!")
    import subprocess
    subprocess.call(['/opt/pycharm/bin/pycharm.sh',
                     os.path.join(os.getcwd(), "ca4i_k15", "connectors",
                                  "{{cookiecutter.connector_name}}", "conf", "nodes",
                                  "{{cookiecutter.pipeline_stage}}",
                                  "{{cookiecutter.pipeline_stage_abbr}}{{cookiecutter.node_name}}",
                                  "config.yml")])

    subprocess.call(['/opt/pycharm/bin/pycharm.sh',
                     os.path.join(os.getcwd(), "ca4i_k15", "connectors",
                                  "{{cookiecutter.connector_name}}", "src",
                                  "{{cookiecutter.pipeline_stage}}",
                                  "{{cookiecutter.pipeline_stage_abbr}}{{cookiecutter.node_name}}.py")])


def add_manifest():
    connector_manifest_file_path = os.path.join(os.getcwd(), "ca4i_k15", "connectors",
                                                "{{cookiecutter.connector_name}}", "manifest.yml")
    if os.path.isfile(connector_manifest_file_path):
        print("manifest.yml exists for connector")
        return

    if not read_user_yes_no("Should i add a manifest.yml with a connector description?", default_value='yes'):
        print("Skipping manifest.yml creation!")
        return

    connector_description = read_user_variable("connector_description", None)
    if not connector_description:
        print("Skipping manifest.yml creation!")
    else:
        create_or_copy_yml_file(connector_manifest_file_path, None, {"description": connector_description},
                                "manifest.yml")


add_manifest()
should_add_to_pipeline()
should_add_to_catalog()
open_in_pycharm()
should_create_additional_nodes(CC_CONTEXT)

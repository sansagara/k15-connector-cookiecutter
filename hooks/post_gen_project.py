# First get cookiecutter context dictionary:
from cookiecutter.main import cookiecutter
from cookiecutter.prompt import read_user_yes_no, read_user_variable

context = {{cookiecutter}}

add_more_nodes = read_user_yes_no("Want to create an additional node?", default_value='no')
if add_more_nodes:
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
            output_dir='../'
        )

    print("Context:")
    print(context)

# First get cookiecutter context dictionary:
import cookiecutter
from cookiecutter import prompt

context = {{cookiecutter}}

add_more_nodes = prompt.read_user_yes_no("Want to create an additional node?", default_value='no')
if add_more_nodes:
    new_node_name = prompt.read_user_variable("node_name", None)
    if not new_node_name:
        print("Skipping new node creation!")

    print("Context:")
    print(context)

SHELL_FOLDER=$(cd "$(dirname "$0")";pwd)
script_path=$SHELL_FOLDER/../scripts/set_up.py

export PYTHONPATH=$PYTHONPATH:$SHELL_FOLDER/..
python3 $script_path
import yaml
import os

def read_yaml(path):
    with open(path, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
   


def read_config(path,config_key):
    if not os.path.exists(path):
        print(path)
    try:
        config = read_yaml(path)
        config = config[config_key]
        return [(key,rf"{value}") for key,value in config.items()]
    except Exception as e:
        print(e)



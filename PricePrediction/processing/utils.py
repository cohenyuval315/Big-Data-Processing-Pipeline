import yaml
import os
import json
import numpy as np
from keras.models import model_from_json


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


def save_model(model_path, weights_path, model):
    """
    Save model.
    """
    np.save(weights_path, model.get_weights())
    with open(model_path, 'w') as f:
        json.dump(model.to_json(), f)
    
def load_model(model_path, weights_path):
    """
    Load model.
    """
    with open(model_path, 'r') as f:
        data = json.load(f)

    model = model_from_json(data)
    weights = np.load(weights_path)
    model.set_weights(weights)

    return model



import tensorflow as tf
import numpy as np
import json
import pickle

def save_model(model_path, weights_path, model):
    np.save(weights_path, model.get_weights())
    with open(model_path, 'w') as f:
        json.dump(model.to_json(), f)
    
def load_model(model_path, weights_path):
    with open(model_path, 'r') as f:
        data = json.load(f)
    model = tf.keras.models.model_from_json(data)
    weights = np.load(weights_path)
    model.set_weights(weights)
    return model

def save_sklearn_model(model,model_path):
    pickle.dump(model, open(model_path, "wb"))

def load_sklearn_model(model_path):
    loaded_model = pickle.load(open(model_path, "rb"))
    return loaded_model
import tensorflow as tf
import numpy as np
import json

def build_and_compile_lstm_model(seq_length):
    model = tf.keras.Sequential()
    model.add(tf.keras.layers.LSTM(256, input_shape=(seq_length, 1), return_sequences=True))
    model.add(tf.keras.layers.Dropout(0.2))

    model.add(tf.keras.layers.LSTM(128, return_sequences=True))
    model.add(tf.keras.layers.Dropout(0.3))

    model.add(tf.keras.layers.LSTM(64))
    model.add(tf.keras.layers.Dropout(0.3))

    model.add(tf.keras.layers.Dense(1))
    model.compile(loss='mean_squared_error', optimizer='adam')
    return model

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
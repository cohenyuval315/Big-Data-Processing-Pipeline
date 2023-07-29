import tensorflow as tf
import numpy as np
import json
import pickle
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import SGDRegressor
from sklearn.preprocessing import StandardScaler
from .utils import save_sklearn_model,load_sklearn_model
import os

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


def sgd_regressor(model_path):
    if os.path.exists(model_path):
        sgdr = load_sklearn_model(model_path)
    else:
        sgdr = SGDRegressor()
    return sgdr
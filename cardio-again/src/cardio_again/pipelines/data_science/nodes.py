"""
This is a boilerplate pipeline 'data_science'
generated using Kedro 0.19.0
"""

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score
from sklearn.neighbors import KNeighborsClassifier

def split_data(model_input: pd.DataFrame, parameters: dict) -> tuple:
    """
    Split the dataset into training and test sets.

    Args:
        model_input: Kedro DataCatalog containing the dataset to be split.
        parameters: A dictionary in the catalog containing the parameters required to split the data.
        test_size_param: Parameter name in the catalog for the test size.
        random_state_param: Parameter name in the catalog for the random state.

    Returns:
        Tuple containing the training and test datasets as pandas DataFrames.
    """
    X = model_input[parameters["features"]]
    y = model_input[parameters["target"]]
    # Split the data into training and test sets
    X_train, X_test, y_train, y_test = train_test_split(model_input, parameters["test_size"], parameters["random_state"])

    return X_train, X_test, y_train, y_test





def train_model(X_train: pd.DataFrame, y_train: pd.Series, parameters: dict) -> KNeighborsClassifier:
    """
    Train a K-Nearest Neighbors (KNN) classifier model.

    Args:
        X_train: The feature matrix of the training dataset.
        y_train: The target labels of the training dataset.
        parameters: Dictionary containing parameters for the KNN model.

    Returns:
        KNeighborsClassifier: Trained KNN classifier model.
    """
    knn = KNeighborsClassifier(parameters["n_neighbors"])
    knn.fit(X_train, y_train)

    return knn

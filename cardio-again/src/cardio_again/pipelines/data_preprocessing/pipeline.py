"""
This is a boilerplate pipeline 'data_preprocessing'
generated using Kedro 0.19.0
"""

from kedro.pipeline import Pipeline, pipeline, node

from .nodes import input_data_males, input_data_females, concatenate_dfs


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=input_data_males,
            inputs=["heart", "params:num_intervals", "params:columns_strange_zeros"],
            outputs="male",
            name="preprocess_males",

        ),
        node(
            func = input_data_females,
            inputs= ["heart", "params:num_intervals", "params:columns_strange_zeros"],
            outputs= "female",
            name= "preprocess_females",   
        ),
        node(
            func = concatenate_dfs,
            inputs= ["male","female"],
            outputs="heartDF",
            name="preprocess_heart_df",
        ),
    ])

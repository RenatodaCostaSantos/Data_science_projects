"""
This is a boilerplate pipeline 'data_preprocessing'
generated using Kedro 0.19.0
"""

from kedro.pipeline import Pipeline, pipeline, node

from .nodes import input_data_males, input_data_females, concatenate_dfs


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node{
            func = input_data_males,
            input = "heart",
            output= "male",
            name= "preprocess_males",

        },
        node{
            func = input_data_females,
            input = "heart",
            output= "female",
            name= "preprocess_females",   

        },
        node{
            func = concatenate_dfs,
            input= ["male","female"],
            output="heartDF",
            name="preprocess_heart_df",
        },
    ])

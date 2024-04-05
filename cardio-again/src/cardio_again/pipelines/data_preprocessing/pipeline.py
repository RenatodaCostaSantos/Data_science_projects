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
            outputs="preprocess_males",
            name="preprocess_males_node",

        ),
        node(
            func = input_data_females,
            inputs= ["heart", "params:num_intervals", "params:columns_strange_zeros"],
            outputs= "preprocess_females",
            name= "preprocess_females_node",   
        ),
        node(
            func = concatenate_dfs,
            inputs= ["preprocess_males","preprocess_females"],
            outputs="preprocess_heart_df",
            name="preprocess_heart_df_node",
        ),
    ])

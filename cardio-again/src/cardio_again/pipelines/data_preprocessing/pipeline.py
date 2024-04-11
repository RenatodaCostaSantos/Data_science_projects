"""
This is a boilerplate pipeline 'data_preprocessing'
generated using Kedro 0.19.0
"""

from kedro.pipeline import Pipeline, pipeline, node


from .nodes import input_data_males, input_data_females, concatenate_dfs, scale_columns, replace_binary_strings_with_float


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
            outputs="join_males_females",
            name="join_males_females_node",
        ),
        node(
            replace_binary_strings_with_float,
            inputs='join_males_females',
            outputs='preprocessed_heartDF',
            name='replace_binary_strings_node',
        ),
        node(
            func=scale_columns,
            inputs=["preprocessed_heartDF","params:columns_to_rescale"],
            outputs="model_input_table",
            name="model_input_table_node",
        )
    ])

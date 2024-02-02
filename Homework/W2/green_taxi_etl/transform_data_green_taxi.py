if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# Function to convert CamelCase to snake_case
def camel_to_snake(name):
    import re
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    data = data[(data['passenger_count']!=0) & (data['trip_distance']!=0)]
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date 
   # Apply the function to each column name
    data.columns = [camel_to_snake(column) for column in data.columns]
    print(data['lpep_pickup_date'].nunique())
    return data

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert 'vendor_id' in output.columns, 'vendor_id is one of the existing values in the column (currently)'
    assert (output['passenger_count']>0).all(), 'There are rows with passenger_count <= 0There are rows with passenger_count <= 0'
    assert (output['trip_distance']>0).all(),'There are rows with trip_distance <= 0'
#Q2: 139,370 rows
#Q3: data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
#Q4: 1 or 2
#Q5: 4
#Q6: 95
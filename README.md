The project shows how to integrate Cerberus JSON validation framework with PySpark.

# Tests
If you want to test the code, follow these steps:

1. Download https://github.com/bartosz25/data-generator and execute `examples/local_filesystem` generator. 
You can increase `percentage_incomplete_data=2, percentage_inconsistent_data=2` to have more invalid data.
2. Create virtualenv and install the dependencies from requirements.txt *TODO: add command*
3. Execute `cerberus_talk/data_validation_pipeline.py`
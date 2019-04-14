# Python Processor

This contains several Python applications that use the Pravega Gateway to read and write Pravega streams.

# Run Instructions

1. Run the [Pravega Gateway](../pravega-gateway/README.md).

2. Install [Miniconda Python 3.7](https://docs.conda.io/en/latest/miniconda.html) or
   [Anaconda Python 3.7](https://www.anaconda.com/distribution/#download-section).

3. Create Conda environment.
    ```
    ./create_conda_env.sh
    ```

4. Run applications.
    ```
    source activate ./env
    export PYTHONPATH=$PWD/../pravega-gateway/src/main/python
    ./event_generator.py
    ```

# See Also

- https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html

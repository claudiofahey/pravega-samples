
# Create Build Environment

Download and install Anaconda 2018.12 for Linux, Python 3.7 version from
https://www.anaconda.com/distribution/#download-section.

```
conda env create -f environment.yml
conda activate pravega-samples
```

(TODO) Install protoc.

# Other

To create GRPC Stubs:
```
conda activate pravega-samples
./make_protobuf.sh
```

To create environment.yaml:
```
conda env export > environment.yml
```

# See Also

- https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html

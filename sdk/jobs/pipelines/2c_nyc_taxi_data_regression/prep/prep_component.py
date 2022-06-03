# Converts MNIST-formatted files at the passed-in input path to training data output path and test data output path
import os
from pathlib import Path
from mldesigner import command_component, Input, Output


@command_component(
    name="prepare_taxi_data",
    version="1",
    display_name="PrepTaxiData",
    environment=dict(
        conda_file=Path(__file__).parent / "conda.yaml",
        image="mcr.microsoft.com/azureml/openmpi3.1.2-ubuntu18.04",
    ),
)
def prepare_data_component(
    raw_green_data: Input(type="uri_file"),
    raw_yellow_data: Input(type="uri_file"),
    prep_green_data: Output(type="uri_folder"),
    prep_yellow_data: Output(type="uri_folder"),
    merged_data: Output(type="uri_folder"),
):
    from pathlib import Path


    def _fix_path(path):
        path = Path(path)
        if path.is_dir():
            # path = path / path.name
            path = path / "file"
        return str(path)


    print("Raw paths:", raw_green_data, raw_yellow_data)
    print("Pre-fix paths:", prep_green_data, prep_yellow_data, merged_data)
    prep_green_data = _fix_path(prep_green_data)
    prep_yellow_data = _fix_path(prep_yellow_data)
    merged_data = _fix_path(merged_data)
    print("Post-fix paths:", prep_green_data, prep_yellow_data, merged_data)

    from kedro.extras.datasets.pandas import CSVDataSet
    from kedro.io import DataCatalog
    from kedro.runner import SequentialRunner

    from pipeline import create_pipeline

    prepare_data = create_pipeline()
    io = DataCatalog(
        dict(
            raw_green_data=CSVDataSet(raw_green_data),
            raw_yellow_data=CSVDataSet(raw_yellow_data),
            prep_green_data=CSVDataSet(prep_green_data, save_args={"index": True}),
            prep_yellow_data=CSVDataSet(prep_yellow_data, save_args={"index": True}),
            merged_data=CSVDataSet(merged_data, save_args={"index": True}),
        )
    )
    SequentialRunner().run(prepare_data, catalog=io)

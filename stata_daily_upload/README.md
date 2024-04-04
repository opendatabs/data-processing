# stata_daily_upload Overview

`stata_daily_upload` is a critical component in the data management and uploading pipeline. It is designed to streamline the process of uploading datasets to an FTP server while integrating seamlessly with ODS (OpenDataSoft) for dataset management and publishing.

## Key Features:

1. **Automated Dataset Uploading**: Simplify the uploading process by specifying dataset paths and the respective destination directories on the FTP server.

2. **Embargo Management**: Control when your datasets go public. If you wish to set an embargo (a temporary restriction) on your datasets, `stata_daily_upload` provides an inbuilt feature to manage and lift the embargo based on your specified datetime.

3. **Integration with ODS**: After uploading the dataset(s), it communicates with ODS to either keep datasets private or make them public based on the embargo settings.

4. **Multiple Files & IDs**: Datasets that span multiple files or correspond to multiple ODS IDs are seamlessly handled with the flexibility to specify a list of files or IDs.

To understand how to effectively configure and utilize `stata_daily_upload`, proceed to the [stata_daily_uploads Configuration Guide](#stata_daily_uploads-Configuration-Guide) below.



# stata_daily_uploads Configuration Guide

Setting up and managing the `stata_daily_uploads.json` is a critical part of ensuring that your datasets are uploaded correctly. This guide will walk you through adding new entries to the configuration file.

The file is resided in `{File Server Root}\PD\PD-StatA-FST-OGD-DataExch\StatA\harvesters\StatA`

## How to Add a New Entry

1. **Comma Delimiter**: Ensure there's a comma `,` after the last entry. 
2. **New Dataset Entry**: Add a new dataset entry encapsulated within `{...}`.
3. **Mandatory Fields**:
   - **file**: Specifies the location/path of the dataset on your system. It can be a single string or a list of strings. Use a list when you have multiple files related to a single dataset or upload batch. E.g., 
     - Single file:
     ```json
     "file": "/path/to/dataset.csv"
     ````
     - Multiple files: 
     ```json
     "file": [
         "/path/to/dataset1.csv",
         "/path/to/dataset2.csv"
     ]
     ```
   - **dest_dir**: Provides the destination directory on the FTP server where the dataset should be uploaded.
   - **ods-id**: This field holds the ODS-ID of the dataset. Similar to `file`, this can be a single string or a list of strings. Use a list if your dataset corresponds to multiple ODS IDs.
     - Single ID:
     ```json
     "ods-id": "000123"
     ```
     - Multiple IDs:
     ```json
     "ods-id": [
         "000123",
         "000124"
     ]
     ```
**Note**: The dataset has to exist in ODS with the specified **ods_id** before making the entry or it will generate an error.

4. **Optional Fields**:
   - **embargo**: For setting an embargo on the dataset, refer to the [Embargo Feature](#embargo-feature) section below.
   - **make_public_embargo**: Set this to `true` if the dataset should be made public after the embargo period is over.

### Example Entry Structure

Below is an example:

```json
[
    {
        "file": "dummy_file.csv",
        "dest_dir": "dummy_directory",
        "ods_id": "dummy_id"
    },
    {
        "file": [
            "dummy_file_2a.txt",
            "dummy_file_2b.txt"
        ],
        "dest_dir": "dummy_directory_2",
        "ods_id": "dummy_id_2"
    },
    {
        "file": "dummy_file_3.xlsx",
        "dest_dir": "dummy_directory_3",
        "ods_id": [
            "dummy_id_3a",
            "dummy_id_3b"
        ]
    }
]
```

### `embargo`-Feature
- To create an embargo on a dataset based on a csv file named "data.csv", place a file named "data_embargo.txt" into the folder where the data file resides. 
- The "_embargo.txt" file must contain a datetime string in the form YYYY-MM-DDThh:mm, e.g.
~~~
2021-10-22T09:00
 ~~~
- The data processing job must be enhanced to use the embargo function:
~~~
common.is_embargo_over(data_file_path)
~~~
- Always update the embargo file before uploading new data!

### `make_public_embargo`-Feature

Set this field to true if you want the dataset to be automatically made public after its embargo period is over. If this is not set or set to false, the dataset will remain private even after the embargo.

## Note

Always review your stata_daily_uploads.json to ensure that the formatting is correct. Invalid JSON formatting can lead to processing errors. Consider using online JSON validators for an additional check.

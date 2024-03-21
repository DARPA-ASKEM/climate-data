[![Build and Publish](https://github.com/DARPA-ASKEM/climate-data/actions/workflows/publish.yaml/badge.svg?event=push)](https://github.com/DARPA-ASKEM/climate-data/actions/workflows/publish.yaml)

# climate-data 

On first container launch, caching data for search will be created - this may take around a minute. 

## Requirements
* **ERA5** data requires a `.cdsapirc` file in the user's home directory with an API key to run requests. This is copied from the root of the project at build and .gitignored away from being committed on accident. The API key can be acquired [here](https://cds.climate.copernicus.eu/api-how-to). You have to accept an online form while logged in to make the key "live" otherwise it will throw an exception. 

## Endpoints

`/status/<uuid>`

Gets the current status of a job. 

Output:

```json
{
    "id": "<uuid>",
    "status":"queued",
    "result": {
        "created_at": "2024-01-09T22:18:22.910371",
        "enqueued_at": "2024-01-09T22:18:22.911473",
        "started_at": null,
        "job_result": null,
        "job_error": null
    }
}
```

`job_result` will contain the returned data from a job once it completes, unless there is an error. In that case, `job_error` will have details. 


### CMIP6 (ESGF)

By default, climate-data will search all possible given mirrors for reliability - for endpoints, IDs with mirrors associated in the following form: (`CMIP6.CMIP.NCAR.CESM2.historical.r11i1p1f1.CFday.ua.gn.v20190514|esgf-data.ucar.edu`) should be considered **interchangeable** with mirrorless versions (`CMIP6.CMIP.NCAR.CESM2.historical.r11i1p1f1.CFday.ua.gn.v20190514`). Mirrorless versions should be considered the preferred form. 

#### Search

`/search/esgf`

Required Parameters:
  * `query`: Natural language string with search terms to retrieve datasets for. 

Example: `/search/esgf?query=historical eastward wind 100 km cesm2 r11i1p1f1 cfday`

Output:  
```json
{
    "results": [
        {
            "metadata": {
                "id": "CMIP6.CMIP.NCAR.CESM2.historical.r11i1p1f1.CFday.ua.gn.v20190514|aims3.llnl.gov",
                "version": "20190514"...
            }
        }, ...
    ]
}
```

`results` is a list of datasets, sorted by relevance. 

Each dataset contains a `metadata` field. 

`metadata` contains all of the stored metadata for the data set, provided by ESGF, such as experiment name, title, variables, geospatial coordinates, time, frequency, resolution, and more. 

The `metadata` field contains an `id` field that is used for subsequent processing and lookups, containing the full dataset ID with revision and node information, such as: `CMIP6.CMIP.NCAR.CESM2.historical.r11i1p1f1.CFday.ua.gn.v20190514|esgf-data.ucar.edu`

#### Preview

`/preview/esgf`

Required Parameters:
  * `dataset_id`: ID of the dataset provided by search in full format. 

Optional Parameters:
  * `variable_id`: override the variable to render in the preview. 
  * `timestamps`: plot over a list of times. 
    * The format should be `start,end` -- two values, comma separated.
    * Example: `1970,1979`
  * `time_index`: override time index to use. 


#### Subset 

`/subset/esgf`

Required Parameters:
  * `dataset_id`: ID of the dataset provided by search in full format. 

Optional Parameters:
  * `parent_dataset_id`: Terarium parent dataset ID - retains provenance info stored in the metadata, so that the subset can keep a pointer to the original it was created from.
  * `timestamps`: 
    * String of two ISO-8601 timestamps or the terms `start` or `end` separated by commas.
    * Examples:
      * `timestamps=2000-01-01T00:00:00,2010-01-01T00:00:00`
      * `timestamps=start,2010-01-01T00:00:00`
      * `timestamps=1999-01-01T00:00:00,end`
  * `envelope`:
    * Geographical envelope provided as a comma-separated series of 4 degrees: lon, lon, lat, lat. 
    * Examples:
      * `envelope=90,95,90,100`
        * Restrict output data to the longitude range [90 deg, 95 deg] and latitude range [90 deg, 100 deg]
  * `thin_factor`:
    * Take every nth datapoint along specified fields given by `thin_fields` (defaulting to all).
    * Examples:
      * `thin_factor=2`
        * Take every other data point in every field
      * `thin_factor=3&thin_fields=lat,lon`
        * Preserving all other fields, take every third data point from the fields `lat` and `lon`
      * `thin_factor=2&thin_fields=!time,lev`
        * Preserving all other fields, take every other data point from all fields *except* `time` and `lev`. 
  * `variable_id`:
    * Which variable to render in the preview. Defaults to `""`. Will attempt to choose the best relevant variable if none is specified.

Output:  
Returns a job description of the current process, queued to be completed. 

```json
{
    "id": "<uuid>",
    "status":"queued",
    "result": {
        "created_at": "2024-01-09T22:18:22.910371",
        "enqueued_at": "2024-01-09T22:18:22.911473",
        "started_at": null,
        "job_result": null,
        "job_error": null
    }
}
```

When completed, checking it with `/status/<job id>` will have an S3 link to the dataset in `job_result`.

```json
{
    "id": "<uuid>",
    "status":"queued",
    "result": {
        "created_at": "2024-01-09T22:18:22.910371",
        "enqueued_at": "2024-01-09T22:18:22.911473",
        "started_at": "2024-01-09T22:18:23.911473",
        "job_result": "s3://bucket-example-climate-data/<uuid>.nc",
        "job_error": null
    }
}
```

#### Fetch

`/fetch/esgf`  

Required Parameters:
  * `dataset_id`: ID of the dataset provided by search in full format. 

Example:  
`/fetch/esgf?dataset_id=CMIP6.CMIP.NCAR.CESM2.historical.r11i1p1f1.CFday.ua.gn.v20190514|esgf-data.ucar.edu`  

Output:
```json
{
    "dataset": "CMIP6.CMIP....",
    "urls": [
        "http://esgf-data.node.example/...",
        "http://esgf-data.node.example/..."
    ],
    "metadata": {}
}
```

The `urls` field specifically contains OPENDAP URLs which can be passed directly to `xarray.open_mfdataset()` for lazy network usage and disk usage. 

## License

[Apache License 2.0](LICENSE)

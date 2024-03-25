from typing import Any
import xarray
import numpy


def extract_esgf_specific_fields(ds: xarray.Dataset) -> dict[str, Any]:
    return {
        "dataSourceDate": ds.attrs.get("creation_date", "UNKNOWN"),
        "datasetUrl": ds.attrs.get("further_info_url", "UNKNOWN"),
        "source": ds.attrs.get("source", "UNKNOWN"),
        "variableId": ds.attrs.get("variable_id", ""),
    }


def extract_metadata(ds: xarray.Dataset) -> dict[str, Any]:
    def extract_numpy_item(x):
        return x.item() if isinstance(x, numpy.generic) else x

    return {
        "format": "netcdf",
        "dataStructure": {
            k: {
                "attrs": {
                    ak: extract_numpy_item(ds[k].attrs[ak])
                    for ak in ds[k].attrs
                    # _ChunkSizes is an unserializable ndarray, safely ignorable
                    if ak != "_ChunkSizes"
                },
                "indexes": list(ds[k].indexes.keys()),
                "coordinates": list(ds[k].coords.keys()),
            }
            for k in ds.variables.keys()
        },
        "raw": {k: extract_numpy_item(ds.attrs[k]) for k in ds.attrs.keys()},
    }

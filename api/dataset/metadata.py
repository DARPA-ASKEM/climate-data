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
    def extract_numpy_item(field):
        return field.item() if isinstance(field, numpy.generic) else field

    return {
        "format": "netcdf",
        "dataStructure": {
            var_name: {
                "attrs": {
                    var_attribute: extract_numpy_item(ds[var_name].attrs[var_attribute])
                    for var_attribute in ds[var_name].attrs
                    # _ChunkSizes is an unserializable ndarray, safely ignorable
                    if var_attribute != "_ChunkSizes"
                },
                "indexes": list(ds[var_name].indexes.keys()),
                "coordinates": list(ds[var_name].coords.keys()),
            }
            for var_name in ds.variables.keys()
        },
        "raw": {
            ds_attribute: extract_numpy_item(ds.attrs[ds_attribute])
            for ds_attribute in ds.attrs.keys()
        },
    }

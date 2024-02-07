import xarray
from typing import List, Dict, Any
from api.dataset.models import (
    DatasetQueryParameters,
    DatasetSubsetOptions,
    GeospatialSubsetOptions,
    TemporalSubsetOptions,
    ThinningSubsetOptions,
)


def location_bbox(
    dataset: xarray.Dataset, bounding_box: List[float], fields=["lat", "lon"]
):
    return dataset.sel({fields[0]: slice(bounding_box[0], bounding_box[1])}).sel(
        {fields[1]: slice(bounding_box[2], bounding_box[3])}
    )


def timestamps(dataset: xarray.Dataset, timestamps: List[str], field="time"):
    ts = timestamps[:]
    print(ts)
    if ts[0] == "start":
        ts[0] = "0001-01"
    if ts[1] == "end":
        ts[1] = "9999-01"
    print(ts, flush=True)
    return dataset.sel({field: slice(ts[0], ts[1])})


def thin(
    dataset, factor=1, fields: List[str] | None = None, negated=False, square=False
):
    nths = (
        {k: dataset.sizes[k] / int(factor) for k in dataset.sizes}
        if square
        else {k: factor for k in dataset.dims}
    )
    if fields:
        nths = (
            {k: v for k, v in nths.items() if k in fields}
            if not negated
            else {k: v for k, v in nths.items() if k not in fields}
        )
    return dataset.thin(nths)


def subset_with_options(dataset: xarray.Dataset, options: DatasetSubsetOptions):
    """
    Performs a dataset subsetting and returns the subset dataset based on the options defined in
    `DatasetSubsetOptions` given they exist. All fields set to None in the given options will be treated
    as the identity funciton.
    """
    ds = dataset
    if options.temporal is not None:
        ds = timestamps(ds, options.temporal.timestamp_range, options.temporal.field)
    if options.geospatial is not None:
        ds = location_bbox(ds, options.geospatial.envelope, options.geospatial.fields)
    if options.thinning is not None:
        ds = thin(
            ds,
            options.thinning.factor,
            options.thinning.fields,
            options.thinning.negated,
            options.thinning.squared,
        )
    if options.custom is not None:
        print("unimplemented! custom filtering will be added later.")
    return ds


def parse_bbox_string(s: str) -> List[float]:
    coords = [float(v.strip()) for v in s.split(",")]
    if len(coords) != 4:
        raise Exception(
            "Invalid bounding box for latitude and longitude. Proper format: x0,x1,y0,y1"
        )
    return coords


def parse_timestamps_string(s: str) -> List[str]:
    timestamps = s.split(",")
    # todo: validate iso8601, start, and end
    if len(timestamps) != 2:
        raise Exception("Invalid format for timestamps")
    return timestamps


def options_from_url_parameters(parameters: Dict[str, Any]) -> DatasetSubsetOptions:
    """
    constructs a `DatasetSubsetOptions` from a list of url query parameters.
    the purpose of this is to provide a unified method of creating defined dataset subset transformations
    without relying on query arguments one by one in each and every URL route.
    """

    options = DatasetSubsetOptions()

    envelope = parameters.get(DatasetQueryParameters.envelope.value, None)
    if envelope is not None:
        options.geospatial = GeospatialSubsetOptions(
            envelope=parse_bbox_string(envelope)
        )

    thin_factor = int(parameters.get(DatasetQueryParameters.thin_factor.value, 1))
    if thin_factor != 1:
        fields = parameters.get(DatasetQueryParameters.thin_fields.value, None)
        negated = False
        if fields is not None:
            if fields.startswith("!"):
                negated = True
            fields = fields.replace("!", "")
            fields = fields.split(",")
        options.thinning = ThinningSubsetOptions(
            factor=thin_factor, fields=fields, negated=negated, squared=False
        )

    timestamps = parameters.get(DatasetQueryParameters.timestamps.value, None)
    if timestamps is not None:
        options.temporal = TemporalSubsetOptions(
            timestamp_range=parse_timestamps_string(s=timestamps)
        )

    return options

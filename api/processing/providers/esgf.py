from .. import filters
from api.search import esgf
import xarray


def slice_esgf_dataset(
    provider: esgf.ESGFProvider,
    dataset_id: str,
    envelope: str,
    timestamps: str,
    divide_by: str,
    custom: str,
):
    urls = provider.get_access_urls_by_id(dataset_id)
    ds = xarray.open_mfdataset(urls)
    if envelope != "":
        bbox = [float(v.strip()) for v in envelope.split(",")]
        if len(bbox) != 4:
            print(
                "Invalid bounding box for latitude and longitude. Proper format: x0,x1,y0,y1"
            )
            return
        ds = filters.location_bbox(ds, bbox)

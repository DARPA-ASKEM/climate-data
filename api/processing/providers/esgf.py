from .. import filters
from api.search.providers import esgf
import xarray
from typing import Any, Dict


def slice_esgf_dataset(
    provider: esgf.ESGFProvider, dataset_id: str, params: Dict[str, Any]
):
    urls = provider.get_access_urls_by_id(dataset_id)
    ds = xarray.open_mfdataset(urls)
    options = filters.options_from_url_parameters(params)
    return filters.subset_with_options(ds, options)

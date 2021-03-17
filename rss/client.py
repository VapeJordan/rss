import numpy as np
import os
import s3f3
import zarr


def load_line(
    seismic, scalers, bounds, line_number, mask_val=np.nan, sort_order="inline"
):
    """
    Loads a line from the input seismic array and resizes it to a standardized size, with
    constant padding.

    Parameters
    ----------
    seismic : array like object containing sort order.
    scalers : array like object containing dynamic range of the line.
    bounds : dict containing min/max values for the inline/crossline coords.
    line_number : int, the line number to access.
    mask_val : scalar, a value to use in padding.
    sort_order : str, the sort order of the seismic array input.

    Returns
    -------
    traces : 2-D float array containing the trace data for the specified line.
    mask : 2-D boolean array, False value indicated data that has been added by padding.
    """

    sort_order = sort_order.lower()
    if sort_order not in ("inline", "crossline"):
        raise RuntimeError(
            f"{sort_order} not supported, sort order should be on of inline or crossline."
        )

    min_inline, min_crossline, max_inline, max_crossline = bounds

    if sort_order == "inline":
        min_line = min_inline
        max_line = max_inline
    else:
        min_line = min_crossline
        max_line = max_crossline

    if line_number < min_line or line_number > max_line:
        raise RuntimeError(
            f"{line_number} out of bounds [{min_line}, {max_line}]."
        )

    traces = seismic[:, :, line_number - min_line]
    mask = traces < 1
    min_val, max_val = scalers[line_number - min_line, :]
    traces -= 1
    traces = traces.astype(np.float32) * max_val / (65535 - 1)
    traces += min_val
    traces[mask] = mask_val
    return traces, mask


class rssFromFile:
    def __init__(self, filename):
        store = zarr.DirectoryStore(f"{filename}")
        root = zarr.open(store, mode="r")

    def line(self, line_number, sort_order="inline"):
        raise NotImplementedError()

    def query_by_xy(self, xy, k=4):
        raise NotImplementedError()


class rssFromS3:
    def __init__(
        self, filename, client_kwargs=None, cache_size=256 * (1024 ** 2)
    ):
        """
        An object for accessing rss data from s3 blob storage.

        Parameters
        ----------
        filename : path to rss data object on s3.
        client_kwargs : dict containing aws_access_key_id and aws_secret_access_key or None.
                        If this variable is none, anonymous access is assumed.
        cache_size : max size of the LRU cache.
        """
        anon = client_kwargs is None

        s3 = s3fs.S3FileSystem(anon=anon, client_kwargs=client_kwargs)
        store = s3fs.S3Map(root=filename, s3=s3, check=False)

        # don't cache meta-data read once
        root = zarr.open(store, mode="r")

        cache = zarr.LRUStoreCache(store, max_size=cache_size)
        inline_root = zarr.open(cache, mode="r")
        self.inline_root = inline_root["inline"]

        cache = zarr.LRUStoreCache(store, max_size=cache_size)
        crossline_root = zarr.open(cache, mode="r")
        self.crossline_root = crossline_root["crossline"]

        self.bounds = root["bounds"]

        self.ilxl = np.vstack(
            [root["coords"]["inlines"][:], root["coords"]["crosslines"][:]]
        ).T

        self.xy = np.vstack(
            [root["coords"]["cdpx"][:], root["coords"]["cdpy"][:]]
        ).T

        self.kdtree = None

    def query_by_xy(self, xy, k=4):
        """
        Query k inline/crossline coordinates closest to this x/y coordinate.

        Parameters
        ----------
        xy - An array containing the [easting, northing] coordinates.
        k - The number of nearest points to look up.

        Returns
        -------
        dist - array, the euclidean distance from the point x/y to the nearest inline/xline grid coordinate.
        ilxl - list, a list of inline/crossling coordinate nearest to the point x/y.
        """
        if self.kdtree is None:
            print(
                "Assembling a tree to map il/xl to x/y. \n"
                + "This could take a couple of minutes, \n"
                + "But only happens one time."
            )
            self.kdtree = KDTree(data=self.xy)

        dist, index = self.kdtree.query(np.atleast_2d(xy), k=k)
        ilxl = [self.ilxl[i, :] for i in index]
        return dist, ilxl

    def line(self, line_number, sort_order="inline"):
        """
        Read a line from the rss data.

        Parameters
        ----------
        line_number : the line number to read.
        sort_order : one of 'inline' or 'crossline' depending on your preference.

        Returns
        -------
        traces : 2-D float array containing the trace data for the specified line.
        mask : 2-D boolean array, False value indicated data that has been added by padding.
        """

        sort_order = sort_order.lower()
        if sort_order not in ("inline", "crossline"):
            raise RuntimeError(
                f"{sort_order} not supported, sort order should be on of inline or crossline."
            )

        if sort_order == "inline":
            seismic = self.inline_root["seismic"]
            scalers = self.inline_root["scalers"]
        else:
            seismic = self.crossline_root["seismic"]
            scalers = self.crossline_root["scalers"]

        return load_line(
            seismic, scalers, self.bounds, line_number, sort_order=sort_order
        )

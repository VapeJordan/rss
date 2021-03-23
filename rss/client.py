from IPython.display import clear_output
import numpy as np
import os
import s3fs
from scipy.spatial import KDTree
import zarr


def load_trace(seismic, scalers, bounds, inline, crossline):
    """
    Loads a trace from the input seismic array.

    Parameters
    ----------
    seismic : array like object containing sort order.
    scalers : array like object containing dynamic range of the line.
    bounds : dict containing min/max values for the inline/crossline coords.
    inline : int, the inline number to access.
    crossline : int, the crossline number to access.

    Returns
    -------
    trace : 1-D float array containing the trace data.
    is_live : boolean, is this a live trace or one added by padding.
    """

    min_inline, min_crossline, max_inline, max_crossline = bounds

    if inline < min_inline or inline > max_inline:
        raise RuntimeError(
            f"{inline} out of bounds [{min_inline}, {max_inline}]."
        )

    if crossline < min_crossline or crossline > max_crossline:
        raise RuntimeError(
            f"{crossline} out of bounds [{min_crossline}, {max_crossline}]."
        )

    trace = seismic[:, crossline - min_crossline, inline - min_inline]
    mask = trace < 1
    min_val, max_val = scalers[inline - min_inline, :]
    trace -= 1
    trace = trace.astype(float) * max_val / (65535 - 1)
    trace += min_val

    is_live = np.all(~mask)

    return trace, mask


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
    mask : 2-D boolean array, True value indicated data that has been added by padding.
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
    traces = traces.astype(float) * max_val / (65535 - 1)
    traces += min_val
    traces[mask] = mask_val
    return traces, mask


class rssClient:
    def __init__(self, store, cache_size=512 * (1024 ** 2)):
        """
        rss format data access.

        Parameters
        ----------
        store - Instance of s ZArr storage object,
                see s3fs.S3Map for remote s3 storage, or zarr.DirectoryStore as common
                types of store.
        """

        # don't cache meta-data read once
        self.root = zarr.open(store, mode="r")

        clear_output()
        print("Mounting line access.")

        cache = zarr.LRUStoreCache(store, max_size=cache_size)

        inline_root = zarr.open(cache, mode="r")
        self.inline_root = inline_root["inline"]

        crossline_root = zarr.open(cache, mode="r")
        self.crossline_root = crossline_root["crossline"]

        clear_output()
        print("Configuring meta-data.")

        self.bounds = self.root["bounds"]

        self.ilxl = np.vstack(
            [
                self.root["coords"]["inlines"][:],
                self.root["coords"]["crosslines"][:],
            ]
        ).T

        self.xy = np.vstack(
            [self.root["coords"]["cdpx"][:], self.root["coords"]["cdpy"][:]]
        ).T

        self.kdtree = None

        clear_output()
        print("Connection complete.")

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

    def trace(self, inline, crossline):
        """
        Read a line from the rss data.

        Parameters
        ----------
        inline : int, inline coordinate.
        crossline : int, crossline coordinate.

        Returns
        -------
        trace : array, the trace at the coordinates.
        is_live : boolean, is this a live trace
        """

        seismic = self.inline_root["seismic"]
        scalers = self.inline_root["scalers"]

        return load_trace(seismic, scalers, self.bounds, inline, crossline)


class rssFromS3(rssClient):
    def __init__(
        self, filename, client_kwargs=None, cache_size=512 * (1024 ** 2)
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
        print("Establishing Connection, may take a minute ......")

        anon = client_kwargs is None

        s3 = s3fs.S3FileSystem(anon=anon, client_kwargs=client_kwargs)

        clear_output()
        print("Connected to S3.")

        store = s3fs.S3Map(root=filename, s3=s3, check=False)

        super().__init__(store, cache_size=cache_size)


class rssFromFile(rssClient):
    def __init__(self, filename, cache_size=512 * (1024 ** 2)):
        """
        An object for accessing rss data from s3 blob storage.

        Parameters
        ----------
        filename : path to rss data object on disk.
        """

        store = zarr.DirectoryStore(f"{filename}")
        root = zarr.open(store, mode="r")
        super().__init__(store, cache_size=cache_size)

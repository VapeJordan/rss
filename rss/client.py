from itertools import chain
from glob import glob
import json
import numpy as np
import os
import s3f3
import tqdm
import zarr


def load_line(seismic, scalers, bounds, line_number, mask_val=np.nan, sort_order='inline'):
    sort_order = sort_order.lower()
    if(sort_order not in ('inline', 'crossline')):
        raise RuntimeError(
            f'{sort_order} not supported, sort order should be on of inline or crossline.')
    
    min_inline, min_crossline, max_inline, max_crossline = bounds

    if(sort_order == 'inline'):
        min_line = min_inline        
        max_line = max_inline
    else:
        min_line = min_crossline        
        max_line = max_crossline
    
    if(line_number < min_line or line_number > max_line):
        raise RuntimeError(f'{line_number} out of bounds [{min_line}, {max_line}].')
    
    traces = seismic[:,:,line_number - min_line]
    mask = traces < 1
    min_val, max_val = scalers[line_number - min_line,:]
    traces -= 1
    traces = traces.astype(np.float32) * max_val/(65535 - 1)
    traces += min_val
    traces[mask] = mask_val
    return traces, mask

class rssFromFile():
    def __init__(self, filename):
        store = zarr.DirectoryStore(f'{filename}')
        root = zarr.open(store, mode='r')
            
    def line(self, line_number, sort_order='inline'):
        raise NotImplementedError()

    def query_by_xy(self, xy, k=4):
        raise NotImplementedError()

class rssFromS3():
    def __init__(self, filename, client_kwargs, cache_size=256*(1024**2)):
        s3 = s3fs.S3FileSystem(client_kwargs=client_kwargs)
        store = s3fs.S3Map(root=filename, s3=s3, check=False)

        # don't cache meta-data read once
        root = zarr.open(store, mode='r')
        
        cache = zarr.LRUStoreCache(store, max_size=cache_size)
        inline_root = zarr.open(cache, mode='r')
        self.inline_root = inline_root["inline"]

        cache = zarr.LRUStoreCache(store, max_size=cache_size)
        crossline_root = zarr.open(cache, mode='r')
        self.crossline_root = crossline_root["crossline"]
        
        self.bounds = root["bounds"]

        self.ilxl = np.vstack([root['coords']['inlines'][:],
                               root['coords']['crosslines'][:]]).T
        
        self.xy = np.vstack([root['coords']['cdpx'][:],
                             root['coords']['cdpy'][:]]).T
        
        self.kdtree = KDTree(data=self.xy)

    def query_by_xy(self, xy, k=4):
        dist, index = self.kdtree.query(np.atleast_2d(xy), k=k)
        ilxl = [self.ilxl[i,:] for i in index]
        return dist, ilxl
        
    def line(self, line_number, sort_order='inline'):
        sort_order = sort_order.lower()
        if(sort_order not in ('inline', 'crossline')):
            raise RuntimeError(
                f'{sort_order} not supported, sort order should be on of inline or crossline.')

        if(sort_order == "inline"):
            seismic = self.inline_root["seismic"]
            scalers = self.inline_root["scalers"]
        else:
            seismic = self.crossline_root["seismic"]
            scalers = self.crossline_root["scalers"]
                    
        return load_line(seismic, scalers, self.bounds, line_number, sort_order=sort_order)

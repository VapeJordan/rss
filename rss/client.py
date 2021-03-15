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

class ZArrFromFile():
    def __init__(self, filename):
        store = zarr.DirectoryStore(f'{filename}')
        root = zarr.open(store, mode='r')
        
        self.seismic = root["seismic"]
        self.scalers = root["scalers"]
        self.bounds = root["bounds"]
            
    def line(self, line_number, sort_order='inline'):
        raise NotImplementedError()

class ZArrFromS3():
    def __init__(self, filename, client_kwargs):
        s3 = s3fs.S3FileSystem(client_kwargs=client_kwargs)
        store = s3fs.S3Map(root=filename, s3=s3, check=False)
        cache = zarr.LRUStoreCache(store, max_size=2**28)
        root = zarr.open(cache, mode='r')

        self.root = root
        self.bounds = self.root["bounds"]

    def line(self, line_number, sort_order='inline'):
        sort_order = sort_order.lower()
        if(sort_order not in ('inline', 'crossline')):
            raise RuntimeError(
                f'{sort_order} not supported, sort order should be on of inline or crossline.')

        root = self.root[sort_order]
        seismic = root["seismic"]
        scalers = root["scalers"]

        return load_line(seismic, scalers, self.bounds, line_number, sort_order=sort_order)

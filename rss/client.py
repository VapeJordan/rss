from itertools import chain
from glob import glob
import json
import numpy as np
import os
import s3f3
import tqdm
import zarr


def load_inline(seismic, scalers, bounds, line_number, mask_val=np.nan):
    min_inline, min_crossline, max_inline, max_crossline = bounds
    
    if(line_number < min_inline or line_number > max_inline):
        min_inline = min_inline        
        max_inline = max_inline
        raise RuntimeError(f'{line_number} out of bounds [{min_inline}, {max_inline}].')
    
    traces = seismic[:,:,line_number - min_inline]
    mask = traces < 1
    min_val, max_val = scalers[line_number - min_inline,:]
    traces -= 1
    traces = traces.astype(np.float32) * max_val/(65535 - 1)
    traces += min_val
    traces[mask] = mask_val
    return traces, mask


def load_crossline(seismic, scalers, bounds, line_number, mask_val=np.nan):
    min_inline, min_crossline, max_inline, max_crossline = bounds

    if(line_number < min_crossline or line_number > max_crossline):
        min_crossline = min_crossline      
        max_crossline = max_crossline
        raise RuntimeError(f'{line_number} out of bounds [{min_crossline}, {max_crossline}].')
    
    def rescale(trace, min_val, max_val):
        mask = trace < 1
        trace = np.copy(trace)
        trace -= 1
        trace = trace.astype(np.float32) * max_val/(65535 - 1)
        trace += min_val
        return trace, mask

    x = seismic[:,line_number-min_crossline,:]
    y = list(chain(*[rescale(x[:,i], *scalers[i,:]) for i in 
                         range(0, max_inline - min_inline+1)]))
    crossline = np.vstack(y[::2])
    mask = np.vstack(y[1::2])
    crossline[mask] = mask_val
    
    return crossline, mask

class ZArrFromFile():
    def __init__(self, filename):
        store = zarr.DirectoryStore(f'{filename}')
        root = zarr.open(store, mode='r')
        
        self.seismic = root["seismic"]
        self.scalers = root["scalers"]
        self.bounds = root["bounds"]
            
    def inline(self, line_number):
        return load_inline(self.seismic, self.scalers, self.bounds, line_number)

    def crossline(self, line_number):
        return load_crossline(self.seismic, self.scalers, self.bounds, line_number)

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

        return load_inline(seismic, scalers, self.bounds, line_number)

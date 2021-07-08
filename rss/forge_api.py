from rss.api import (parse_ebcdic, parse_binary_header, read_trace_data_unstructured)

from numcodecs import LZ4
import numpy as np
import os
import s3fs
import shutil
from wget import download
import zarr

compressor = LZ4()

# default header locations for the FORGE data.
byte_locations={'RECTVD' : (41, 4, '>i'),
                'NSAMPTRC' : (115, 2, '>h'),
                'FIBREDIST' : (197, 4, '>i'),
                'RECMD' : (237, 4, '>i')}


def get_forge_root_s3(zarr_file):
    s3 = s3fs.S3FileSystem()
    store = s3fs.S3Map(root=zarr_file, s3=s3, check=False)
    root = zarr.group(store)
    return root

def make_forge_zarr(root, **config):
    num_traces, ns, num_lines = config['num_traces'], config['ns'], config['num_lines']
    
    das = root.zeros("seismic", shape=(num_traces, ns, num_lines), 
                        chunks=(num_traces, ns, 1), dtype=np.uint16, 
                            overwrite=True, compressor=compressor)
    
    scalers = root.zeros("scalers", shape=(num_lines, 2), dtype=np.float32, overwrite=True)
    commands = root.zeros("get_all_silixia", shape=(num_lines,), dtype='S108', overwrite=True)
    filenames = root.zeros("segy_filenames", shape=(num_lines,), dtype='S108', overwrite=True)
    
    # these are events we know about at the time of ingestion:
    num_events = config['num_events']    
    events = root.zeros("sample_events", shape=(num_events, 2), dtype=np.int, overwrite=True)

    return root


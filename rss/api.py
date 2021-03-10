
from collections import defaultdict
import ebcdic
from itertools import chain
from glob import glob
from ibm2ieee import ibm2float32
import json
from numcodecs import LZ4
import numpy as np
import os
import struct
import tqdm
import zarr

# SEGY defaults
headers_offset = 3600    
trace_header_size = 240
#FIXME - only supports 32-bit trace data
size_of_float = 4

segy_units = {0:'unknown', 1:'meters', 2:'feet'}

segy_format = {1 : '4-byte IBM floating-point',
               2 : '4-byte, twos complement integer',
               3 : '2-byte, twos complement integer',
               4 : '4-byte fixed-point with gain',
               5 : '4-byte IEEE floating-point',
               6 : 'unknown',
               7 : 'unknown',
               8 : '1-byte twos complement integer'}

byte_locations = {'inline' : (189, 4, '>i'),
                  'crossline' : (193, 4, '>i'),
                  'cdpx' : (181, 4, '>i'),
                  'cdpy' : (185, 4, '>i'),
                  'scalco' : (71, 2, '>h')}

default_inline_byte = 189
default_xline_byte = 193
default_cdpx_byte = 181
default_cdpy_byte = 185

# default compression
compressor = LZ4()

def parse_ebcdic(segy_file):
    with open(segy_file, 'rb') as fp:
        ebcdic_bytes = fp.read(3200).decode('cp1140')
    ebcdic_header = ""
    for i in range(40):
        ebcdic_header += ebcdic_bytes[i*80:(i+1)*80] + "\n"
    return ebcdic_header

def parse_binary_header(segy_file):
    with open(segy_file, 'rb') as fp:
        fp.seek(3200)
        binary_header = fp.read(400)
        
    sample_rate_ms = struct.unpack('>H', binary_header[16:18])[0]/1000.
    ns = struct.unpack('>H', binary_header[20:22])[0]
    float_format = struct.unpack('>H', binary_header[24:26])[0]
    spatial_units = struct.unpack('>H', binary_header[54:56])[0]

    if (float_format not in (1,5)):
        fmt = segy_format[float_format]
        raise RuntimeError(f'binary format {fmt} not supported.')
    
    size_of_trace = ns * 4 + 240
    
    file_size = os.path.getsize(segy_file)
    
    if((file_size - 3600) % size_of_trace):
        raise RuntimeError('Variable trace length not supported.')

    return {'sample_rate_ms' : sample_rate_ms, 
            'ns' : ns,
            'float_format' : float_format,
            'units' : segy_units[spatial_units],
            'size_of_trace' : size_of_trace,
            'num_traces' : (file_size - 3600)//size_of_trace}

def read_trace_data(segy_file,
                    binary_header,
                    byte_locations=byte_locations,
                    scalco=None):
    filename = os.path.splitext(os.path.basename(segy_file))[0]

    folder = f'{filename}'
    if(not os.path.exists(folder)):
        os.makedirs(folder)
    
    with open(f'{filename}/binary_header.json', 'w') as fp:
        fp.write(json.dumps(binary_header))
   
    trace_size = binary_header['size_of_trace']
    
    def to_coord(x, scal):
        if(scal > 0):
            return x * scal
        else:
            return x/abs(scal)
    
    def header_to_int(trace_as_bytes, scalco):
        hdr = trace_as_bytes[:trace_header_size]
        val = byte_locations['inline']
        hdr = [struct.unpack(val[2], hdr[val[0]-1:val[0]-1+val[1]])[0] for key,val in byte_locations.items()]

        if (scalco is None):            
            scalco = hdr[-1]
        # convert to scale
        hdr[2] = to_coord(hdr[2], scalco)
        hdr[3] = to_coord(hdr[3], scalco)
        return hdr[:-1]
        
    def traces_to_float(trace_as_bytes, binary_format):
        trace_data = trace_as_bytes[trace_header_size:]

        if binary_format == 1:
            trace_data = ibm2float32(np.frombuffer(trace_data, dtype='>u4'))
        elif binary_format == 5:
            trace_data = np.frombuffer(trace_data, dtype='>f')
            trace_data.byteswap(inplace=True)
        else:
            fmt = segy_format[float_format]
            raise RuntimeError(f'binary format {fmt} not supported.')        
        return trace_data


    crossline_compr = defaultdict(list)
    inline_compr = defaultdict(list)
    
    crossline_coords = defaultdict(list)
    inline_coords = defaultdict(list)

    inlines = np.zeros(binary_header['num_traces'], dtype=int)
    crosslines = np.zeros(binary_header['num_traces'], dtype=int)

    with open(segy_file, 'rb') as fp:  
        fp.seek(3600)
        for trac in tqdm.tqdm(range(0, binary_header['num_traces'])):
            raw_bytes = fp.read(binary_header['size_of_trace'])

            hdr = header_to_int(raw_bytes, scalco)
            trace = traces_to_float(raw_bytes, binary_header['float_format'])

            # Dumbest possible impl
            folder = '{}/inlines/{}'.format(filename, hdr[0])
            if(not os.path.exists(folder)):
                os.makedirs(folder)
            with open('{}/inlines/{}/traces.bin'.format(filename, hdr[0]), 'ba') as gp:
                trace.tofile(gp)
            crossline_coords[hdr[0]].append(hdr[1])
            
            # save all the inlines
            inlines[trac] = hdr[0]
            crosslines[trac] = hdr[1]

    np.save(f'{filename}/inlines.npy', inlines)
    np.save(f'{filename}/crosslines.npy', crosslines)
    
    inlines = np.unique(inlines)
    crosslines = np.unique(crosslines)
    
    min_inline = inlines[inlines > 0].min()
    max_inline = inlines[inlines > 0].max()

    min_crossline = crosslines[crosslines > 0].min()
    max_crossline = crosslines[crosslines > 0].max()
    
    num_inlines = len(inlines)
    num_crosslines = len(crosslines)

    # seems backwards, but the "crosslines" are in the "inline" image
    # i.e. the inline number is constant and vice versa:
    for key, val in crossline_coords.items():
        indx = np.array(val) - min_crossline
        valid_crosslines = np.zeros(num_crosslines, dtype=bool)
        valid_crosslines[indx] = 1
        with open(f'{filename}/inlines/{key}/index.bin', 'ba') as gp:
            valid_crosslines.tofile(gp)
            
    
def compressed_zarr(segy_file, chunks=[-1,100,100]):
    filename = os.path.splitext(os.path.basename(segy_file))[0]

    with open(f'{filename}/binary_header.json', 'r') as fp:
        binary_header = json.loads(fp.read())

    inlines = np.load(f'{filename}/inlines.npy')
    inlines = np.unique(inlines)

    min_inline = inlines.min()
    max_inline = inlines.max()

    crosslines = np.load(f'{filename}/crosslines.npy')
    crosslines = np.unique(crosslines)
    min_crossline = crosslines.min()
    max_crossline = crosslines.max()

    # always read whole traces:
    chunks[0] = binary_header['ns']

    #folder = f'{filename}/traces.zarr'
    store = zarr.DirectoryStore(f'{filename}')
    root = zarr.group(store)
    
    seismic = root.zeros("seismic",
                         shape=(binary_header['ns'],
                                max_crossline - min_crossline + 1,
                                max_inline - min_inline + 1),
                         chunks=chunks,
                         compressor=compressor,
                         dtype=np.uint16,
                         overwrite=True)

    folder = f'{filename}/inlines/*'

    #scalers = np.zeros((max_inline - min_inline + 1, 2), dtype=int)

    scalers = root.zeros("scalers", shape=(max_inline - min_inline + 1, 2), dtype=int)
    bounds = root.create_dataset("bounds", data=[int(min_inline),
                                                 int(min_crossline),
                                                 int(max_inline),
                                                 int(max_crossline)], dtype=int)
                                                  
    
    for line in tqdm.tqdm(glob(folder)):
        line_number = int(os.path.basename(line))

        # FIXME rerun little endian
        traces = np.fromfile(f'{filename}/inlines/{line_number}/traces.bin', 
                             dtype='<f4')
        indx = np.fromfile(f'{filename}/inlines/{line_number}/index.bin', 
                           dtype=bool)

        min_val = traces.min()
        traces -= (min_val)

        max_val = traces.max()
        traces = (65535-1)*traces/max_val

        traces += 1
    
        traces.shape = (-1, binary_header['ns'])
    
        _traces = np.zeros((binary_header['ns'],
                            max_crossline - min_crossline + 1),
                           dtype=np.uint16)
        _traces[:,np.where(indx)[0]] = traces.T

        seismic[..., line_number - min_inline] = _traces
        
        scalers[line_number - min_inline,:] = [min_val, max_val]
    
    np.save(f'{filename}/scalers.npy', scalers)
    
    #zarr.save(f'{filename}/seismic.zarr', seismic)
    """
    with open(f'{filename}/offset.json', 'w') as fp:
        fp.write(json.dumps({'min_inline' : int(min_inline), 
                             'min_crossline' : int(min_crossline),
                             'max_inline' : int(max_inline),
                             'max_crossline' : int(max_crossline)}))
    """

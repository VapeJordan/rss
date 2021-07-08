import argparse
from numcodecs import LZ4, JSON
import numpy as np
import os
import s3fs
import shutil
from tqdm import tqdm
from wget import download
import zarr

from rss.api import (parse_ebcdic, 
                         parse_binary_header, 
                             read_trace_data_unstructured)
from rss.forge_api import (get_forge_root_s3, make_forge_zarr)
from rss.forge_api import byte_locations as forge_byte_locations

forge_headers = ['RECTVD', 'NSAMPTRC', 'FIBREDIST', 'RECMD']

# fingers crossed the filesizes are the same
config = {
    'sample_rate_ms' : 0.5,
    'ns' : 30000,
    'float_format' : 5,
    'units' : 'feet',
    'size_of_trace' : 120240,
    'num_traces' : 1280,
    'num_lines' : 71881,
    'num_events' : 111,
    'strlen' : 108}

compressor = LZ4()

def to_uint16(traces):
    min_val = traces.min()
    traces -= min_val

    max_val = traces.max()
    if max_val != 0:
        # could the entire line be zero?
        traces = (65535 - 1) * traces / max_val

    # zero isn't an invalid number
    traces += 1
    
    return traces.astype(np.uint16), (min_val, max_val)


def parse_silxia_name(line):
    url = line.split(" ")[-1].rstrip()
    segy_file = os.path.basename(url)
    return url, segy_file


def extract_line(segy_file, binary_header, byte_locations=forge_byte_locations):
    filename = os.path.splitext(segy_file)[0]
    if not os.path.exists(os.path.join(filename, "data", "traces.bin")):
        read_trace_data_unstructured(segy_file, binary_header, 
                                         byte_locations=forge_byte_locations)
    data = np.fromfile(f'{filename}/data/traces.bin', dtype=np.float32)
    data.shape = (-1, binary_header['ns'])    

    headers = {key:np.load(f'{filename}/{key}.npy') for 
                key in forge_headers}

    return data, headers


def cleanup(line):
    url, segy_file = parse_silxia_name(line)    
    filename = os.path.splitext(segy_file)[0]
    os.remove(segy_file)
    shutil.rmtree(filename)


def ingest_line(das, iline, line):
    url, segy_file = parse_silxia_name(line)

    if not os.path.exists(segy_file):
        download(url)
    
    data, headers = extract_line(segy_file, config, 
                                byte_locations=forge_byte_locations)
    traces, scalers = to_uint16(data)
    for key,val in headers.items():
        das.create_dataset(key, data=val, overwrite=True)

    assert(traces.shape[0] == config['num_traces'])
    assert(traces.shape[1] == config['ns'])

    das["seismic"][..., iline] = traces
    das["scalers"][iline, :] = scalers

    
def log_success(line):
    with open("success.txt", 'a') as fp:
        fp.write(f"{line}\n")


def log_error(line):
    with open("errors.txt", 'a') as fp:
        fp.write(f"{line}\n")
        
        
if __name__ == "__main__":
    """ usage:
    python ingestion-forge.py --min_line=38427 --max_line=38428 --init_zarr=True --zarr_out=s3://gsh-competition-data/FORGE-DAS/das.zarr
    """
    parser = argparse.ArgumentParser()

    parser.add_argument('--min_line', nargs='?', type=int, 
                            help='minimum file to load (in get_all_silxia.sh.')
    
    parser.add_argument('--max_line', nargs='?', type=int,
                            help='maximum file to load (in get_all_silxia.sh.')

    parser.add_argument('--zarr_out', nargs='?', type=str,
                            help='The file to write the DAS data too.')

    parser.add_argument('--init_zarr', nargs='?', type=bool, default=False,
                            help='The file to write the DAS data too.')

    parser.add_argument('--init_events', nargs='?', type=bool, default=True,
                            help='Preload the files with events in them.')
    
    args = parser.parse_args()
    
    if args.init_zarr:
        print ("Initializing ZArr.")
        # the FORGE data is way too big to ingest locally:        
        s3 = s3fs.S3FileSystem()
        store = s3fs.S3Map(root=args.zarr_out, s3=s3, check=False)
        root = zarr.group(store)
    
        das = make_forge_zarr(root, **config)
        with open('get_all_silixa.sh', 'r') as fp:
            lines = fp.readlines()
        das['get_all_silixa'] = lines 

        das['segy_filenames'] = [parse_silxia_name(i)[1] for i in lines]

        with open('FORGE-Microseismic-Lookup.txt', 'r') as fp:
            lines = fp.readlines()
        for i, line in enumerate(lines[1:]):
            _, it, iset = line.split(" ")
            it, iset = int(it), int(iset)
            das['sample_events'][i,:] = (int(it), int(iset))
        
        # There's 70,000+ binary headers, so use a global definition and 
        # just hope for the best:
        das.empty('binary_header', shape=len(config), 
                      dtype=object, object_codec=JSON(),
                          overwrite=True)
        for i, keyval in enumerate(config.items()):
            key, val = keyval
            das['binary_header'][i] = {key : val}
    else:
        das = get_forge_root_s3(args.zarr_out)    

    if args.init_events:
        load_lines = np.unique(das['sample_events'][:,1])
    else:
        load_lines = range(args.min_line, args.max_line)
        
    lines = das['get_all_silixa']
    for il in tqdm(load_lines):
        try:
            ingest_line(das, il, lines[il])
            cleanup(lines[il])
            log_success(lines[il])
        except:
            log_error(lines[il])

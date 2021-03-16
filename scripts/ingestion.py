import argparse
import os
import shutil

from rss.api import (byte_locations, compressed_zarr,
                     parse_ebcdic, parse_binary_header, read_trace_data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('segy_file', type=str,
                        help='segy file to ingest')
    
    parser.add_argument('--inline', nargs='?', type=str, default='189-192',
                        help='inline byte location.')
    
    parser.add_argument('--crossline', nargs='?', type=str, default='193-196',
                        help='crossline byte location.')

    parser.add_argument('--cdpx', nargs='?', type=str, default='181-184',
                        help='cdpx byte location.')

    parser.add_argument('--cdpy', nargs='?', type=str, default='185-188',
                        help='cdpy byte location.')
    
    parser.add_argument('--override_scalco', nargs='?', type=int,
                        help='overrider coords scalar.')

    parser.add_argument('--sort_order', nargs='?', type=str, default='inline',
                        help='sort order of zarr.')
    
    args = parser.parse_args()

    def to_bytes(x):
        mn, mx = x.split('-')
        
        mn = int(mn)
        mx = int(mx)
        
        nbytes = mx - mn + 1
        if nbytes == 4:
            return (mn, nbytes, '>i')
        elif nbytes == 2:
            return (mn, nbytes, '>h')
        else:
            raise RuntimeError("byte {x} length should be 2 or 4.")

    byte_locations['inline'] = to_bytes(args.inline)
    byte_locations['crossline'] = to_bytes(args.crossline)
    byte_locations['cdpx'] = to_bytes(args.cdpx)
    byte_locations['cdpy'] = to_bytes(args.cdpy)
 
    ebcdic = parse_ebcdic(args.segy_file)
    print (ebcdic)
    
    binary_header = parse_binary_header(args.segy_file)    
    print ("")
    print ("Binary Header info:")
    print (binary_header)
    print ("")

    read_trace_data(args.segy_file,
                    binary_header,
                    sort_order=args.sort_order,
                    scalco=args.override_scalco,
                    byte_locations=byte_locations)

    compressed_zarr(args.segy_file, sort_order=args.sort_order)
   
    path = os.path.splitext(os.path.basename(args.segy_file))[0]    
    shutil.rmtree(os.path.join(path, '{sort_order}s'))

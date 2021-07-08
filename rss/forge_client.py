from IPython.display import clear_output
import matplotlib
import matplotlib.pylab as plt
import numpy as np
import s3fs
import os
from scipy.signal import butter, lfilter, medfilt
import zarr

def parse_silxia_name(line):
    url = line.split(" ")[-1].rstrip()
    segy_file = os.path.basename(url)
    return url, segy_file

def butter_bandpass(lowcut, highcut, fs, order=5):
    """license: see scipy-cookbook-notice.txt"""
    nyq = 0.5 * fs
    low = lowcut / nyq
    high = highcut / nyq
    b, a = butter(order, [low, high], btype='band')
    return b, a


def butter_bandpass_filter(data, lowcut, highcut, fs, order=5):
    """license: see scipy-cookbook-notice.txt"""    
    b, a = butter_bandpass(lowcut, highcut, fs, order=order)
    y = lfilter(b, a, data)
    return y


def from_uint16(traces, scalers):
    """ Converts a das data back into float format."""
    mask = traces < 1
    min_val, max_val = scalers
    traces -= 1
    traces = traces.astype(float) * max_val / (65535 - 1)
    traces += min_val
    return traces, mask


def load_das(das, iline):
    traces = das['seismic'][..., iline]
    scalers = das['scalers'][iline, :]
    return from_uint16(traces, scalers)


def load_meta(das):
    meta_data = das['binary_header']
    meta_data = {key:val for i in meta_data for key,val in i.items()}
    recmd = das['RECMD'][:]/1000.    
    time_seconds = np.arange(meta_data['ns']) * meta_data['sample_rate_ms']/1000.
    return meta_data, recmd, time_seconds


def plot(data, time=None, depth=None, 
         crop=None, figsize=(20,20), title='FORGE DAS', 
         cmap='seismic', scalers=None):
    """ Without some processing/clipping it will be hard to see the 
        microseismic events in the data.        
    """
    font = {'family' : 'DejaVu Sans',
            'weight' : 'normal',
            'size'   : 22}

    matplotlib.rc('font', **font)
    
    if depth is None:
        depth = np.arange(data.shape[0])
        
    if time is None:
        time = np.arange(data.shape[1])
        
    delta = (np.percentile(data[crop[0]:crop[1],:], 68) - 
                                 np.percentile(data[crop[0]:crop[1],:], 16))
    # very heavy tailed
    delta *= 3
    
    time = np.arange(data.shape[1]) * 0.5/1000.
    if crop:
        depth = depth[crop[0]:crop[1]] 
        time = time[crop[2]:crop[3]]
        data = data[crop[0]:crop[1], crop[2]:crop[3]]   
         
    extent = (time[0], time[-1], depth[-1], depth[0])
    
    plt.figure(figsize=figsize)
    plt.imshow(data, cmap=cmap, extent=extent, origin='upper',
                   aspect='auto', vmin=-delta, vmax=delta,
                       interpolation='bicubic')
    plt.xlabel('time (s)')
    plt.ylabel('depth (ft)')
    plt.title(title)


def process(inp):
    """ Processing worklflow loosely adapted from:
        Low-magnitude Seismicity with a Downhole Distributed Acoustic Sensing Array 
            -- examples from the FORGE Geothermal Experiment
        A. Lellouch et~al.
        https://arxiv.org/abs/2006.15197
    """
    #median
    outp = inp - medfilt(inp,(21,1))
    #
    outp = butter_bandpass_filter(outp, 5, 250, 2000.)
    
    outp = np.array([i/np.linalg.norm(i) for i in outp])
    return outp

class rssFORGEClient:
    def __init__(self, store, cache_size=128 * (1024 ** 2)):
        # don't cache meta-data read once
        self.cache = zarr.LRUStoreCache(store, max_size=cache_size)

        self.root = zarr.open(self.cache, mode="r")

        meta_data, recmd, time_seconds = load_meta(self.root)
        
        self.depth = recmd
        self.time_seconds = time_seconds
        self.sample_events = self.root["sample_events"][:]
        self.segy_filenames = self.root["segy_filenames"][:]
        
    def line(self, line_number):
        return load_das(self.root, line_number)
    
    def get_sample_events(self):
        """ Returns a the time of the event (in samples), and the index 
            of the event or (line number).
            
            usage:
            sample_events = client.get_sample_events()
            filenames, events = client.get_sample_events()
            
            # choose an event
            it, iset = events[42,:]
            
            data = client.linee(iset)
            # e.g. use the custom plot from the client library
            from rss.forge_client import plot

            plot(data, time=client.time_seconds, depth=client.depth, 
                        crop=(250, 1100, it-500, it+500), 
                            title=client.segy_filenames[42],
                                cmap='gray', figsize=(20,20))                        
        """        
        return self.sample_events[:]
        
        
class rssFORGEFromS3(rssFORGEClient):
    def __init__(
        self, filename, client_kwargs=None, cache_size=128 * (1024 ** 2)
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

        if client_kwargs is None:
            s3 = s3fs.S3FileSystem()
        else:
            s3 = s3fs.S3FileSystem(client_kwargs=client_kwargs)
            
        store = s3fs.S3Map(root=filename, s3=s3, check=False)

        super().__init__(store, cache_size=cache_size)
  
        
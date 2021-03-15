# rss - Real Simple Seismic

Forget about SEGY - watch you seismic like frames in a movie.

rss is a real simple way to ingest and access stacked 3d seismic data. Once ingested, 
the seismic data is access slices, inlines, crosslines, time slices (todo) or in 
3D chunks (todo). 

The data can be sourced locally or from your favourite cloud provider (AWS s3 currently supported).

rss = RSSFromS3(path_to_object, client_kwargs)

inline, mask = rss.line(1112, sort_order='inline')\
crossline, mask = rss.line(2111, sort_order='crossline')

The data will be padded (with nans) to a consistent 3d volume. The mask in this example is a 2x2
boolean array the shape of the lines wich tells you where are the dead traces.

## Installation 
git clone ...
python setup.py install

pip installation (todo).



# rss - Real Simple Seismic

Forget about SEGY - watch your seismic like frames in a movie.

rss is a real simple way to ingest and access stacked 3d seismic data. Once ingested, 
the seismic data is access slices, inlines, crosslines, time slices (todo) or in 
3D chunks (todo). 

rss provides a command-line tool for ingesting SEGY data into rss format. The output 
is a directory that can be kept on a local file system or places on in blob storage. 

rss supports serverless access to data on blob storage, currently only AWS S3 is supported, 
but support for other cloud solutions is on the product roadmap. 

## Installation
Right now rss needs to be installed from source:

git clone https://github.com/VapeJordan/rss.git

python setup.py install

However we are planning to release to pip shorty:\
pip installation (todo).

## Usage - Access data from a local file system

Create the rss object:

rss = rssFromFile(path_to_rss_data)

inline = rss.line(line_number, sort_order='inline')\
crossline = rss.line(line_number, sort_order='crossline')

The resulting inline, crossline are numpy array, with a regular shape across the survey.
Any dead traces are padded with NaN's. You can also map between the x,y coordinates of the survey and the inline/crossline 
coordinates using the query method:

rss.query_by_xy(xy, k=4)

Where x/y are eastings and northings. The variable "k" returns the k-nearest inline/crossline
coordinate to that x/y point. 


## Usage - Access data from AWS S3

Access to data on S3 is provided by the rssFromS3 object.

### Example: Access data from a public bucket

If the data resides on a bucket that support anonymous access to data, 
then all you need to do is provide object uri (its path on s3).

rss = rssFromS3(object_uri, cache_size=100*1024*2)

Reads from a remote blob store are relatively expensive, so this object supports 
a (LRU) least recently used cache. Speficy the max size of this cache in bytes as 
an optional argument (otherwise it defaults to 256Mb).

### Example: Access data from a private bucket

For a private bucket you will need to set AWS credentials and specify them 
in a dictionary.

client_kwargs = {'aws_access_key_id':'XYZ.....', 'aws_secret_access_key':'ABC.....'}\
rss = rssFromS3(object-uri, client_kwargs=client_kwargs)

## Usage - Access data from AWS S3

The ingestion script will need you to configure byte locations, typically you can read these
from the ebcdic header in the file, options are:

--inline
--crossline
--cdpx
--cdpy

You can also force the "scalar to be applied to all coordinates" to a constant value. 
--override_scalco

Finally, the layout is optimized for sort order, specify this as one of inline or crossline.
--sort_order='inline'

python ingestion.py psdn11_TbsdmF_full_w_AGC_Nov11.segy --inline='5-8' --crossline='21-24' --override_scalco=100  --sort_order='inline'

python ingestion.py psdn11_TbsdmF_full_w_AGC_Nov11.segy --inline='5-8' --crossline='21-24' --override_scalco=100  --sort_order='crossline'

Warning: Ingestion of large data can be time consuming, this volume takes 1 hour to complete ingestion.

The output will be a directory named after the SEGY filename, in this example, it will be psdn11_TbsdmF_full_w_AGC_Nov11.
This directory can be kept for access on a local disk or moved to an s3 bucket to support remote access that way.







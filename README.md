# rss - Real Simple Seismic

Forget about SEGY - watch your seismic like frames in a movie.

## (Update) FORGE Geothermal DAS 

rss adds access to the Utah FORGE: High-Resolution DAS Microseismic Data from Well 78-32.
FORGE Geothermal DAS dataset, provide by the [Geothermal Data Repository](https://gdr.openei.org/submissions/1185) 
and used under a CC-BY 4.0 license.

"DAS" is a recording of seismicity, which in this case is made with fibre optic cable which is permanently cemented 
in a FORGE monitoring well 78-32, which is a vertical well, 3274.78 feet deep. Having sensors located "downhole" 
means that the system is expected to be highly sensitive to microseismicity resulting from the enhanced geothermal
stimulation.

This DAS data consists of 71,880 (segy) DAS recordings. Of these large data, 111 recordings
the location of which has been kindly provided by [ariellellouch](https://github.com/ariellellouch/FORGE/blob/master/DAS_Microseis_Catalog).

Why should you care? Because geothermal is so hot right now. Certainly there's applications for 
automated microseismic event detection in carbon capture.


### Example Usage:

Use the client to establish a connection with the dataset:
```
client = rssFORGEFromS3('[email Vape for Access]')
```

You're not generally going to see anything much in the data, there's only 111 microseismic events 
in all of the 71,880 recordings. The client has a list of ariellellouch's events (time and dataset id), 
you might use these to get started:
```
it, iset = client.sample_events[34,:]
```

The FORGE DAS data has been indexed by its order in the "get_all_silixa.sh" script 
that is provided to access to segy. It's possible that not all the data is loaded, 
check the mask array, where it's "True" the data is missing:
```
data, mask = client.line(iset)
```

There's a plot and process (not a good one) functions to help see the events, they maybe had to
see through the noise otherwise:
```
from rss.forge_client import plot, process, rssFORGEFromS3
plot(data, time=client.time_seconds, depth=client.depth, 
            crop=(250, 1100, it-1000, it+1500), 
                title=client.segy_filenames[iset],
                    cmap='gray', figsize=(20,20))
```

![GitHub Logo](/data/FORGE-Example-Event.png)


## Poststack Seismic Data

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

rss = rssFromS3(object_uri, cache_size=100)

Reads from a remote blob store are relatively expensive, so this object supports 
a (LRU) least recently used cache. Speficy the max size of this cache in bytes as 
an optional argument (otherwise it defaults to 256Mb).

### Example: Access data from a private bucket

For a private bucket you will need to set AWS credentials and specify them 
in a dictionary.

client_kwargs = {'aws_access_key_id':'XYZ.....', 'aws_secret_access_key':'ABC.....'}\
rss = rssFromS3(object-uri, client_kwargs=client_kwargs)

## Usage - Ingesting SEGY data to rss

The ingestion script will need you to configure byte locations, typically you can read these
from the ebcdic header in the file, options are:

--inline\
--crossline\
--cdpx\
--cdpy

You can also force the "scalar to be applied to all coordinates" to a constant value.\
--override_scalco

Finally, the layout is optimized for sort order, specify this as one of inline or crossline.\
--sort_order

python ingestion.py psdn11_TbsdmF_full_w_AGC_Nov11.segy --inline='5-8' --crossline='21-24' --override_scalco=100  --sort_order='inline'

python ingestion.py psdn11_TbsdmF_full_w_AGC_Nov11.segy --inline='5-8' --crossline='21-24' --override_scalco=100  --sort_order='crossline'

Warning: Ingestion of large data can be time consuming, this volume takes 1 hour to complete ingestion.

The output will be a directory named after the SEGY filename, in this example, it will be psdn11_TbsdmF_full_w_AGC_Nov11.
This directory can be kept for access on a local disk or moved to an s3 bucket to support remote access that way.







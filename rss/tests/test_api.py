import os
import shutil
import unittest

from rss import api

test_header = """C01 CLIENT    : WOODSIDE ENERGY LTD                                             
C02 SURVEY    : ENFIELD 2010 4D MONITOR 5, BLOCK WA-28-L, NW AUSTRALIAN         
C03 DATASET   : FINAL FAR ANGLE (30-41) STACK OF MONITOR 5 IN TIME DOMAIN       
C04 INLINES   : 1000-1253 (INCR 1), CROSSLINES: 1430-2521 (INCR 1)              
C05 SRATE     : 4MS, TMAX : 4000MS                                              
C06 GRID      : 12.5 X 25 M                                                     
C07 TAPELABEL : GU5133                                                          
C08 RECORDING PARAMETERS:                                                       
C09 SHOT INTERVAL : 18.75M  RCV INTERVAL    : 12.50M                            
C10 CABLE LENGTH  : 2800M   RCVS PER CABLE  : 224 GUNS/CABLES : 2/8             
C11 GUN SEPARATION: 50.0M   CABLE SEPARATION: 100.0M                            
C12 MIN OFFSET: 580 M       MAX OFFSET: 3380 M                                  
C13 NOTE: THIS IS A 4D PUSH-REVERSE SURVEY WITH SOURCE BOAT BEHIND TAILBUOYS    
C14 DATA ORDERED BY FIELDRECORD AND CHANNEL NUMBER-CHANNEL 1 IS FAR OFFSET CHAN 
C15 SPHEROID OF REF: GDA94  UTM ZONE:50S COORDINATE UNITS: METERS               
C16 PROCESSED DATUM: MSL SAMPLE RATE: 2MS  MAXTIME: 4096MS                      
C17 PROCESSING SEQUENCE:(WESTERN)                                               
C18 CONVERT SEGD TO WESTERN FORMAT, 2MS SAMPLE RATE, 4096MS TRACE LENGTH        
C19 FLAG FIELD EDITS, SEISMIC-NAVIGATION MERGE,UPDATE TRACE HEADERS,SEGY OUTPUT 
C20 PROCESSING SEQUENCE:(CGGVERITAS)                                            
C21 REFORMAT FROM SEGY TO CGGVERITAS INTERNAL FORMAT                            
C22 WOODSIDE SUPPLIED ANTI ALIAS FILTER AND RESAMPLE TO 4 MS                    
C23 ONE SAMPLE TIME SHIFT (4MS SHIFT COMPARING WITH WESTERN FORMAT)             
C24 ZERO PHASE LOW-CUT FILTER (3HZ/18DB) AND ZERO PHASE DESIGNATURE             
C25 GUN/CABLE STATICS CORRECTION (5M/7M, VELOCITY: 1500 M/S) & TIDAL STATICS    
C26 SCALE, TIME & PHASE SHIFT CORRECTION (SCALAR:1.4,SHIFT:6.799MS/-12.326DEG)  
C27 CUT OFFSET INTO RANGE 587-3213 (BIN CENTER 625-3175)                        
C28 FIRST PASS SWELL NOISE ATTENUATION & LINEAR NOISE REMOVAL                   
C29 SECOND PASS SWELL NOISE ATTENUATION & EXTRA DENOISE FOR 16 VERY NOISY LINES 
C30 3D SRME, RANDOMISED GATHER DENOISE, RADON DEMULTIPLE & INVERSE Q            
C31 SHOT & CHANNEL SCALING, RMC & WATER COLUMN STATICS CORRECTION               
C32 GLOBAL MATCH, MERGE WITH BASE & MISSING TRACE INTERPOLATION                 
C33 4D DSRNRMS BINNING (6.25M X 25M GRID),DEBUBBLE,PSDM & ADJACENT BINS SUM     
C34 RESIDUAL MOVEOUT CORRECTION & STRETCH TO TIME WITH SMOOTH VELOCITY          
C35 RADON DEMULTIPLE,OFFSET DEPENDENT SPECTRAL BALANCE,TRIM STATICS,DENOISE     
C36 WOODSIDE SPECTRAL BALANCE, ANGLE MUTE (30-41), STACK AND WAVELET DENOISE    
C37 GLOBAL MATCH WITH BASE,FULL FOLD POLYGON CUT & OUTPUT IN SEGY FORMAT        
C38 CDP       :BYTE 21-24    TRACE SEQ :BYTE 25-28    ARCHV NO:BYTE 173-174     
C39 CDPX(M)   :BYTE 181-184  CDPY(M)   :BYTE 185-188 (MULTIPLIED BY 100)        
C40 INLINE    :BYTE 189-192  CROSSLINE :BYTE 193-196                            
"""

binary_meta = {
    "sample_rate_ms": 4.0,
    "ns": 1001,
    "float_format": 1,
    "units": "meters",
    "num_traces": 1,
    "size_of_trace": 4244,
}


class TestAPI(unittest.TestCase):
    def setUp(self):
        self.segy_file = "data/enfield_test_data.sgy"
        self.ieee_data = "data/psdn_test_data.segy"

    def test_ebcdic(self):
        from rss.api import parse_ebcdic

        # just print for QC
        result = parse_ebcdic(self.segy_file)
        self.assertEqual(test_header, result)

    def test_parse_binary_header(self):
        from rss.api import parse_binary_header

        binary_header = parse_binary_header(self.segy_file)
        for key, val in binary_header.items():
            self.assertEqual(binary_meta[key], val)

    def test_read_trace_data_unstructured(self):    
        from ibm2ieee import ibm2float32
        import numpy as np
        import os

        from rss.api import read_trace_data_unstructured
        
        binary_meta = {
            "sample_rate_ms": 1.0,
            "ns": 1501,
            "float_format": 5,
            "units": "meters",
            "num_traces": 120,
            "size_of_trace": 240 + 4 * 1501,
        }
        ieee_data = "../data/psdn_test_data.segy"

        read_trace_data_unstructured(self.ieee_data, binary_meta)

        output_file = os.path.join(os.path.splitext(
            os.path.basename(self.ieee_data))[0], "inline.npy")
        inlines = np.load(output_file)

        assert(inlines[0] == 983)

        read_size = binary_meta["size_of_trace"] * binary_meta["num_traces"]
        with open(self.ieee_data, "rb") as fp:
            fp.seek(3600)
            traces = np.frombuffer(fp.read(read_size), dtype=">f")
        traces.shape = (120, -1)
        traces = traces[:, 60:]

        output_file = os.path.join(
            os.path.splitext(os.path.basename(self.ieee_data))[0],
            "data",
            "traces.bin")

        with open(output_file, "rb") as fp:
            test_traces = np.frombuffer(fp.read(1501 * 4 * 120), "<f")
        test_traces.shape = (120, -1)

        np.testing.assert_array_equal(traces, test_traces)            
            
    def test_read_trace_data_ieee(self):
        import numpy as np
        from rss.api import read_trace_data

        binary_meta = {
            "sample_rate_ms": 1.0,
            "ns": 1501,
            "float_format": 5,
            "units": "meters",
            "num_traces": 120,
            "size_of_trace": 240 + 4 * 1501,
        }

        read_size = binary_meta["size_of_trace"] * binary_meta["num_traces"]
        with open(self.ieee_data, "rb") as fp:
            fp.seek(3600)
            traces = np.frombuffer(fp.read(read_size), ">f")

        traces.shape = (120, -1)
        traces = traces[:, 60:]

        read_trace_data(self.ieee_data, binary_meta)

        output_file = os.path.join(
            os.path.splitext(os.path.basename(self.ieee_data))[0],
            "inlines",
            "983",
            "traces.bin",
        )

        with open(output_file, "rb") as fp:
            test_traces = np.frombuffer(fp.read(1501 * 4 * 120), "<f")
        test_traces.shape = (120, -1)

        try:
            np.testing.assert_array_equal(traces, test_traces)
            shutil.rmtree(
                os.path.splitext(os.path.basename(self.ieee_data))[0]
            )
        except:
            shutil.rmtree(
                os.path.splitext(os.path.basename(self.ieee_data))[0]
            )
            raise

    def test_read_trace_data_ibm(self):
        from ibm2ieee import ibm2float32
        import numpy as np

        from rss.api import read_trace_data

        with open(self.segy_file, "rb") as fp:
            fp.seek(3600 + 240)
            trace = ibm2float32(np.frombuffer(fp.read(1001 * 4), dtype=">u4"))

        read_trace_data(self.segy_file, binary_meta, scalco=-100)

        output_file = os.path.join(
            os.path.splitext(os.path.basename(self.segy_file))[0],
            "inlines",
            "1253",
            "traces.bin",
        )

        with open(output_file, "rb") as fp:
            test_trace = np.frombuffer(fp.read(1001 * 4), "<f")

        np.testing.assert_array_equal(trace, test_trace)

        try:
            np.testing.assert_array_equal(trace, test_trace)
            shutil.rmtree(
                os.path.splitext(os.path.basename(self.segy_file))[0]
            )
        except:
            shutil.rmtree(
                os.path.splitext(os.path.basename(self.segy_file))[0]
            )
            raise

  
    def test_compressed_zarr(self):
        from ibm2ieee import ibm2float32
        import numpy as np
        from rss.api import compressed_zarr, read_trace_data
        from rss.client import rssFromFile

        binary_meta = {
            "sample_rate_ms": 1.0,
            "ns": 1501,
            "float_format": 5,
            "units": "meters",
            "num_traces": 120,
            "size_of_trace": 240 + 4 * 1501,
        }

        read_trace_data(self.ieee_data, binary_meta, sort_order="inline")
        read_trace_data(self.ieee_data, binary_meta, sort_order="crossline")

        compressed_zarr(self.ieee_data, sort_order="inline")
        compressed_zarr(self.ieee_data, sort_order="crossline")

        # test the client too
        rss = rssFromFile("psdn_test_data")

        traces, _ = rss.line(983, sort_order="inline")

        np.testing.assert_array_equal(traces.shape, (1501, 120))
        # crossline will be the same because we only have one trace

        with open(self.ieee_data, "rb") as fp:
            fp.seek(3600)
            read_size = (
                binary_meta["size_of_trace"] * binary_meta["num_traces"]
            )
            test_traces = np.frombuffer(fp.read(read_size), dtype=">f4")

        test_traces.shape = (120, -1)
        test_traces = test_traces[:, 60:].T

        # test ranges:
        np.testing.assert_allclose(traces.max(), test_traces.max(), rtol=1e-4)

        np.testing.assert_allclose(traces.min(), test_traces.min(), rtol=1e-4)

        # the largest relative error is where the amplitude is smallest
        dynamic_range = test_traces.max() - test_traces.min()
        delta = np.abs((traces - test_traces) / dynamic_range)
        self.assertTrue(delta.max() < 1e-4)

        shutil.rmtree(os.path.splitext(os.path.basename(self.ieee_data))[0])

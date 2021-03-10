import os
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

binary_meta = {'sample_rate_ms': 4.0, 'ns': 1001, 'float_format': 1,
               'units': 'meters', 'num_traces' : 227545, 'size_of_trace' : 4244}

class TestAPI(unittest.TestCase):
    def setUp(self):
        #self.segy_file = 'data/ENFIELD_2010_4D_FAR_ANGLE.sgy'
        self.segy_file = 'data/ENFIELD_2010_4D_FULLSTACK.sgy'
        
    def _test_ebcdic(self):
        from rss.api import parse_ebcdic
        # just print for QC
        result = parse_ebcdic(self.segy_file)
        self.assertEqual(test_header, result)

    def _test_parse_binary_header(self):
        from rss.api import parse_binary_header
        binary_header = parse_binary_header(self.segy_file)
        for key, val in binary_header.items():
            self.assertEqual(binary_meta[key], val)
            
    def _test_read_trace_data(self):
        from time import time
        from rss.api import read_trace_data
        st = time()
        read_trace_data(self.segy_file, binary_meta, scalco=-100)
        print ("Elapsed time : ", time() - st)

    def test_compressed_zarr(self):
        from time import time
        from rss.api import compressed_zarr
        st = time()
        compressed_zarr(self.segy_file)
        print ("Elapsed time : ", time() - st)

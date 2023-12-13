import pyuff
import re
import numpy as np
import pandas as pd

FREQ_TYPES_58 = [2, 3, 4, 5, 6, 7, 8, 9, 10, 12]    # `func_types` corresponding to frequency domain data for record type 58
# TODO: What domain type is "Probability Density Function?" 

def read_uff(uff_path, time_pattern=".*", freq_pattern=".*", header_keys=("id1")):
    """Reads Universal file (UFF). Returns time and/or frequency domain data as DataFrame(s)"""

    if (time_pattern is None) and (freq_pattern is None):
        raise ValueError("Need to specify pattern for time or frequency domain")

    if time_pattern is not None:
        time_series = []    # Returns pd.Series objects for time domain return values
        time_headers = []   # Meta data to store in returned df_time column headers
        tpat = re.compile(time_pattern)
        returning_time = True
    else:
        returning_time = False
    
    if freq_pattern is not None:
        freq_series = []    # Stores pd.Series objects for frequency domain return values]
        freq_headers = []   # Meta data to store in returned df_freq column headers
        fpat = re.compile(freq_pattern)
        returning_freq = True
    else:
        returning_freq = False

    uff = pyuff.UFF(uff_path)
    all_sets = uff.read_sets()

    if type(all_sets) is dict:
        # Only one set found in file. Put in list and go about process as usual.
        all_sets = [all_sets]
    
    for item in all_sets:
        id1 = item["id1"]
        format_type = int(item["type"])
        if format_type != 58:
            print(f"WARNING: {id1} in file is format type {format_type}, though type 58 is expected.")
        
        func_type = item["func_type"]
        # From https://community.sw.siemens.com/s/article/Which-information-can-I-find-in-a-Universal-File
        # For type 58
        # 0	    General or Unknown
        # 1	    Time Response
        # 2	    Auto Spectrum
        # 3	    Cross Spectrum
        # 4	    Frequency Response Function
        # 5	    Transmissibility
        # 6	    Coherence
        # 7	    Cross Correlation
        # 9	    Power Spectral Density (PSD)
        # 10	Energy Spectral Density (ESD)
        # 11	Probability Density Function
        # 12	Spectrum

        if returning_freq and (func_type in FREQ_TYPES_58) and fpat.match(id1):
            # Assume abscissa values may be different between frequency domain series',
            # Create pd.Series so abscissa-to-ordinate values are correctly mapped in df_freq
            freq_series.append(pd.Series(data=item["data"], index=item["x"]))
            item_header = tuple([item[key] for key in header_keys])
            freq_headers.append(item_header)
        elif returning_time and func_type==1 and tpat.match(id1):
            # Actually, let's not assume the abscissa values are consistent between time domain data either
            time_series.append(pd.Series(data=item["data"], index=item["x"]))
            item_header = tuple([item[key] for key in header_keys])
            time_headers.append(item_header)
        elif func_type==0:
            print(f"WARNING: Data {id1} has func_type of 'General or Unknown' and will not be included in either return DataFrame.")
        elif func_type==11:
            print(f"WARNING: Data {id1} has func_type of 'Probability Density Function' and will not be included in either return DataFrame.")
        
    dfs = []
    if returning_time:
        if len(time_series) > 0:
            df_time = pd.concat(time_series, axis=1)
            df_time.columns = pd.MultiIndex.from_tuples(time_headers)
        else:
            df_time = pd.DataFrame()
            
        dfs.append(df_time)

    if returning_freq:
        if len(freq_series) > 0:
            df_freq = pd.concat(freq_series, axis=1)
            df_freq.columns = pd.MultiIndex.from_tuples(freq_headers) 
        else:           
            df_freq = pd.DataFrame()

        dfs.append(df_freq)
    
    return dfs

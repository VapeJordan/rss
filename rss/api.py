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

# SEGY definitions
headers_offset = 3600
trace_header_size = 240
size_of_float = 4

segy_units = {0: "unknown", 1: "meters", 2: "feet"}

segy_format = {
    1: "4-byte IBM floating-point",
    2: "4-byte, twos complement integer",
    3: "2-byte, twos complement integer",
    4: "4-byte fixed-point with gain",
    5: "4-byte IEEE floating-point",
    6: "unknown",
    7: "unknown",
    8: "1-byte twos complement integer",
}

byte_locations = {
    "inline": (189, 4, ">i"),
    "crossline": (193, 4, ">i"),
    "cdpx": (181, 4, ">i"),
    "cdpy": (185, 4, ">i"),
    "scalco": (71, 2, ">h"),
}
apply_spatial_scalar_to = ["cdpx", "cdpy"]

# default compression
compressor = LZ4()


def parse_ebcdic(segy_file):
    with open(segy_file, "rb") as fp:
        ebcdic_bytes = fp.read(3200).decode("cp1140")
    ebcdic_header = ""
    for i in range(40):
        ebcdic_header += ebcdic_bytes[i * 80 : (i + 1) * 80] + "\n"
    return ebcdic_header


def parse_binary_header(segy_file):
    with open(segy_file, "rb") as fp:
        fp.seek(3200)
        binary_header = fp.read(400)

    sample_rate_ms = struct.unpack(">H", binary_header[16:18])[0] / 1000.0
    ns = struct.unpack(">H", binary_header[20:22])[0]
    float_format = struct.unpack(">H", binary_header[24:26])[0]
    spatial_units = struct.unpack(">H", binary_header[54:56])[0]

    if float_format not in (1, 5):
        fmt = segy_format[float_format]
        raise RuntimeError(f"binary format {fmt} not supported.")

    size_of_trace = ns * 4 + 240

    file_size = os.path.getsize(segy_file)

    if (file_size - 3600) % size_of_trace:
        raise RuntimeError("Variable trace length not supported.")

    return {
        "sample_rate_ms": sample_rate_ms,
        "ns": ns,
        "float_format": float_format,
        "units": segy_units[spatial_units],
        "size_of_trace": size_of_trace,
        "num_traces": (file_size - 3600) // size_of_trace,
    }


def to_coord(x, scal):
    if scal > 0:
        return x * scal
    else:
        return x / abs(scal)


def parse_header(trace_as_bytes, 
                 scalco=None, 
                 byte_locations=byte_locations,
                 apply_spatial_scalar_to=apply_spatial_scalar_to):
    hdr = trace_as_bytes[:trace_header_size]

    if "scalco" not in byte_locations.keys():
        byte_locations["scalco"] = (71, 2, ">h")
    
    hdr = {
        key: struct.unpack(val[2], hdr[val[0] - 1 : val[0] - 1 + val[1]])[0]
        for key, val in byte_locations.items()
    }

    # sometimes you have to override this:
    if scalco is None:
        scalco = hdr["scalco"]
        
    for key in apply_spatial_scalar_to:
        if key in hdr.keys():
            hdr[key] = to_coord(hdr[key], scalco)
            
    return hdr


def parse_trace(trace_as_bytes, binary_format, override_byteswap=False):
    trace_data = trace_as_bytes[trace_header_size:]

    if binary_format == 1:
        trace_data = ibm2float32(np.frombuffer(trace_data, dtype=">u4"))
    elif binary_format == 5:
        trace_data = np.frombuffer(trace_data, dtype=">f")
        if not override_byteswap:
            trace_data = trace_data.byteswap()
    else:
        fmt = segy_format[float_format]
        raise RuntimeError(f"binary format {fmt} not supported.")
    return trace_data


def read_trace_data(
    segy_file,
    binary_header,
    byte_locations=byte_locations,
    apply_spatial_scalar_to=apply_spatial_scalar_to,
    scalco=None,
    sort_order="inline",
):
    sort_order = sort_order.lower()
    if sort_order not in ("inline", "crossline"):
        raise RuntimeError(
            f"{sort_order} not supported, sort order should be on of inline or crossline."
        )

    if sort_order == "inline":
        orthogonal_line = "crossline"
    else:
        orthogonal_line = "inline"

    filename = os.path.splitext(os.path.basename(segy_file))[0]

    folder = f"{filename}"
    if not os.path.exists(folder):
        os.makedirs(folder)

    with open(os.path.join(filename, "binary_header.json"), "w") as fp:
        fp.write(json.dumps(binary_header))

    trace_size = binary_header["size_of_trace"]

    line_coords = defaultdict(list)

    inlines = np.zeros(binary_header["num_traces"], dtype=int)
    crosslines = np.zeros(binary_header["num_traces"], dtype=int)

    cdpx = np.zeros(binary_header["num_traces"], dtype=int)
    cdpy = np.zeros(binary_header["num_traces"], dtype=int)

    with open(segy_file, "rb") as fp:
        fp.seek(3600)
        for trac in tqdm.tqdm(range(0, binary_header["num_traces"])):
            raw_bytes = fp.read(binary_header["size_of_trace"])

            hdr = parse_header(raw_bytes, 
                               scalco, 
                               byte_locations=byte_locations,
                               apply_spatial_scalar_to=apply_spatial_scalar_to)
            
            trace = parse_trace(raw_bytes, binary_header["float_format"])

            # Dumbest possible impl
            line_number = hdr[sort_order]
            folder = os.path.join(filename, f"{sort_order}s", f"{line_number}")
            if not os.path.exists(folder):
                os.makedirs(folder)
            with open(os.path.join(folder, "traces.bin"), "ba") as gp:
                trace.tofile(gp)
            line_coords[hdr[sort_order]].append(hdr[orthogonal_line])

            # save all the inlines
            inlines[trac] = hdr["inline"]
            crosslines[trac] = hdr["crossline"]

            cdpx[trac] = hdr["cdpx"]
            cdpy[trac] = hdr["cdpy"]

    np.save(os.path.join(filename, "inlines.npy"), inlines)
    np.save(os.path.join(filename, "crosslines.npy"), crosslines)

    np.save(os.path.join(filename, "cdpx.npy"), cdpx)
    np.save(os.path.join(filename, "cdpy.npy"), cdpy)

    inlines = np.unique(inlines)
    crosslines = np.unique(crosslines)

    min_inline = inlines[inlines > 0].min()
    max_inline = inlines[inlines > 0].max()

    min_crossline = crosslines[crosslines > 0].min()
    max_crossline = crosslines[crosslines > 0].max()

    num_inlines = len(inlines)
    num_crosslines = len(crosslines)

    if sort_order == "inline":
        min_orth_line = min_crossline
        num_orth_lines = max_crossline - min_crossline + 1
    else:
        min_orth_line = min_inline
        num_orth_lines = max_inline - min_inline + 1

    # seems backwards, but the "crosslines" are in the "inline" image
    # i.e. the inline number is constant and vice versa:
    for key, val in line_coords.items():
        indx = np.array(val) - min_orth_line
        valid_crosslines = np.zeros(num_orth_lines, dtype=bool)
        valid_crosslines[indx] = 1

        output_file = os.path.join(
            filename, f"{sort_order}s", f"{key}", "index.bin"
        )
        with open(output_file, "ba") as gp:
            valid_crosslines.tofile(gp)

def read_trace_data_unstructured(segy_file, binary_header, 
    byte_locations=byte_locations, override_byteswap=False):
    """ Read all the data in the file but dont assume structure."""

    filename = os.path.splitext(os.path.basename(segy_file))[0]

    folder = f"{filename}"
    if not os.path.exists(folder):
        os.makedirs(folder)

    with open(os.path.join(filename, "binary_header.json"), "w") as fp:
        fp.write(json.dumps(binary_header))

    trace_size = binary_header["size_of_trace"]

    header_values = {key : np.zeros(binary_header["num_traces"], dtype=int) for 
                        key in byte_locations.keys()}

    with open(segy_file, "rb") as fp:
        fp.seek(3600)
        for trac in tqdm.tqdm(range(0, binary_header["num_traces"])):
            raw_bytes = fp.read(binary_header["size_of_trace"])

            hdr = parse_header(raw_bytes, 
                               byte_locations=byte_locations, 
                               apply_spatial_scalar_to=[])

            trace = parse_trace(raw_bytes, binary_header["float_format"], 
                                    override_byteswap=override_byteswap)
            # Dumbest possible impl
            folder = os.path.join(filename, "data")
            if not os.path.exists(folder):
                os.makedirs(folder)

            with open(os.path.join(folder, "traces.bin"), "ba") as gp:
                trace.tofile(gp)

            for key in header_values.keys():
                header_values[key][trac] = hdr[key]

    for key in header_values.keys():
        np.save(os.path.join(filename, f"{key}.npy"), header_values[key])


def compressed_zarr(segy_file, sort_order="inline"):
    sort_order = sort_order.lower()
    if sort_order not in ("inline", "crossline"):
        raise RuntimeError(
            f"{sort_order} not supported, sort order should be on of inline or crossline."
        )

    if sort_order == "inline":
        orthogonal_line = "crossline"
    else:
        orthogonal_line = "inline"

    filename = os.path.splitext(os.path.basename(segy_file))[0]

    with open(os.path.join(filename, "binary_header.json"), "r") as fp:
        binary_header = json.loads(fp.read())

    cdpx = np.load(os.path.join(filename, "cdpx.npy"))
    cdpy = np.load(os.path.join(filename, "cdpy.npy"))

    inlines = np.load(os.path.join(filename, "inlines.npy"))
    crosslines = np.load(os.path.join(filename, "crosslines.npy"))

    if sort_order == "inline":
        min_orth_line = crosslines.min()
        max_orth_line = crosslines.max()

        min_line = inlines.min()
        max_line = inlines.max()
    else:
        min_orth_line = inlines.min()
        max_orth_line = inlines.max()

        min_line = crosslines.min()
        max_line = crosslines.max()

    # always read whole traces:

    chunks = [
        int(binary_header["ns"]),
        int(max_orth_line - min_orth_line + 1),
        1,
    ]

    # folder = f'{filename}/traces.zarr'
    store = zarr.DirectoryStore(f"{filename}")
    root = zarr.group(store)

    bounds = root.create_dataset(
        "bounds",
        data=[
            int(inlines.min()),
            int(crosslines.min()),
            int(inlines.max()),
            int(crosslines.max()),
        ],
        dtype=int,
        overwrite=True,
    )

    coords_root = root.create_group("coords", overwrite=True)

    inline_coord = coords_root.create_dataset(
        "inlines", data=inlines, dtype=int, overwrite=True
    )
    crossline_coord = coords_root.create_dataset(
        "crosslines", data=crosslines, dtype=int, overwrite=True
    )
    cdpx_coord = coords_root.create_dataset(
        "cdpx", data=cdpx, dtype=float, overwrite=True
    )
    cdpy_coord = coords_root.create_dataset(
        "cdpy", data=cdpy, dtype=float, overwrite=True
    )

    line_root = root.create_group(sort_order, overwrite=True)

    seismic = line_root.zeros(
        "seismic",
        shape=(
            binary_header["ns"],
            max_orth_line - min_orth_line + 1,
            max_line - min_line + 1,
        ),
        chunks=chunks,
        compressor=compressor,
        dtype=np.uint16,
        overwrite=True,
    )

    scalers = line_root.zeros(
        "scalers", shape=(max_line - min_line + 1, 2), dtype=int
    )

    folder = os.path.join(filename, f"{sort_order}s", "*")
    for line in tqdm.tqdm(glob(folder)):
        line_number = int(os.path.basename(line))

        # FIXME rerun little endian
        traces = np.fromfile(
            os.path.join(
                filename, f"{sort_order}s", f"{line_number}", "traces.bin"
            ),
            dtype="<f4",
        )
        indx = np.fromfile(
            os.path.join(
                filename, f"{sort_order}s", f"{line_number}", "index.bin"
            ),
            dtype=bool,
        )

        min_val = traces.min()
        traces -= min_val

        max_val = traces.max()
        if max_val != 0:
            # could the entire line be zero?
            traces = (65535 - 1) * traces / max_val

        # zero isn't an invalid number
        traces += 1

        traces.shape = (-1, binary_header["ns"])

        _traces = np.zeros(
            (binary_header["ns"], max_orth_line - min_orth_line + 1),
            dtype=np.uint16,
        )

        _traces[:, np.where(indx)[0]] = traces.T

        seismic[..., line_number - min_line] = _traces

        scalers[line_number - min_line, :] = [min_val, max_val]

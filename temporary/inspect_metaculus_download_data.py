import io
import struct
import zipfile
import zlib
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from src.common.http import get_metaculus_client
from src.metaculus.grabber import MetaculusGrabber


def main() -> None:
    grabber = MetaculusGrabber()
    posts = grabber.fetch_posts(limit=50)
    target_post = None
    for post in posts:
        if (post.get("forecasts_count") or 0) > 10:
            target_post = post
            break

    if not target_post:
        print("No post with forecasts_count > 10 found.")
        return

    post_id = target_post["id"]
    print(f"Using post_id={post_id} forecasts_count={target_post.get('forecasts_count')}")

    client = get_metaculus_client()
    response = client.get(
        f"/api/posts/{post_id}/download-data/",
        params={"aggregation_methods": ["recency_weighted"]},
    )
    response.raise_for_status()
    content_type = response.headers.get("Content-Type")
    content_length = response.headers.get("Content-Length")
    content_encoding = response.headers.get("Content-Encoding")
    transfer_encoding = response.headers.get("Transfer-Encoding")
    disposition = response.headers.get("Content-Disposition")
    print(f"Content-Type: {content_type}")
    print(f"Content-Length header: {content_length}")
    print(f"Content-Encoding: {content_encoding}")
    print(f"Transfer-Encoding: {transfer_encoding}")
    print(f"Content-Disposition: {disposition}")
    content = response.content
    print(f"Downloaded bytes: {len(content)}")
    tail = content[-22:]
    tail_hex = " ".join(f"{b:02x}" for b in tail)
    print(f"Zip tail (22 bytes): {tail_hex}")
    eocd_sig = b"PK\x05\x06"
    eocd_index = content.rfind(eocd_sig)
    print(f"EOCD signature index: {eocd_index}")

    print(f"is_zipfile: {zipfile.is_zipfile(io.BytesIO(content))}")
    try:
        zf = zipfile.ZipFile(io.BytesIO(content))
        print("Zip files:", zf.namelist())
        for name in zf.namelist():
            with zf.open(name) as f:
                preview = f.read(300).decode("utf-8", errors="replace")
                safe_preview = preview.encode("ascii", errors="replace").decode("ascii")
                print(f"\n--- {name} preview ---")
                print(safe_preview)
        return
    except zipfile.BadZipFile:
        preview = response.content[:300].decode("utf-8", errors="replace")
        safe_preview = preview.encode("ascii", errors="replace").decode("ascii")
        print("Bad zip response preview:")
        print(safe_preview)

    print("Attempting manual local-file parsing...")
    files = {}
    offset = 0
    signature = b"PK\x03\x04"
    while True:
        sig_index = content.find(signature, offset)
        if sig_index == -1 or sig_index + 30 > len(content):
            break
        header = content[sig_index + 4:sig_index + 30]
        if offset == 0:
            header_hex = " ".join(f"{b:02x}" for b in header)
            print(f"Header bytes: {header_hex}")
        (
            _version,
            flags,
            compression,
            _mod_time,
            _mod_date,
            _crc,
            comp_size,
            _uncomp_size,
            name_len,
            extra_len,
        ) = struct.unpack("<HHHHHIIIHH", header)
        if offset == 0:
            print(
                f"Header debug: flags={flags} compression={compression} "
                f"comp_size={comp_size} name_len={name_len} extra_len={extra_len}"
            )
        name_start = sig_index + 30
        name_end = name_start + name_len
        filename = content[name_start:name_end].decode("utf-8", errors="replace")
        data_start = name_end + extra_len
        data_end = data_start + comp_size
        if comp_size == 0 or (flags & 0x08):
            next_sig = content.find(signature, data_start)
            central_sig = content.find(b"PK\x01\x02", data_start)
            candidates = [pos for pos in [next_sig, central_sig] if pos != -1]
            if candidates:
                data_end = min(candidates)
        file_data = content[data_start:data_end]
        if compression == 0:
            raw = file_data
        elif compression == 8:
            try:
                raw = zlib.decompress(file_data, -zlib.MAX_WBITS)
            except zlib.error:
                raw = b""
        else:
            raw = b""
        if filename:
            files[filename] = raw
        offset = data_end

    safe_files = [
        name.encode("ascii", errors="replace").decode("ascii")
        for name in files.keys()
    ]
    print("Manual files:", safe_files)
    for name, raw in files.items():
        if "forecast_data.csv" in name or "question_data.csv" in name:
            preview = raw[:300].decode("utf-8", errors="replace")
            safe_preview = preview.encode("ascii", errors="replace").decode("ascii")
            safe_name = name.encode("ascii", errors="replace").decode("ascii")
            print(f"\n--- {safe_name} preview (manual) ---")
            print(safe_preview)

    marker = b"forecast_data.csvQuestion ID"
    marker_index = content.find(marker)
    if marker_index != -1:
        data_start = marker_index + len(b"forecast_data.csv")
        next_local = content.find(b"PK\x03\x04", data_start)
        next_central = content.find(b"PK\x01\x02", data_start)
        candidates = [pos for pos in [next_local, next_central] if pos != -1]
        data_end = min(candidates) if candidates else len(content)
        csv_bytes = content[data_start:data_end]
        preview = csv_bytes[:300].decode("utf-8", errors="replace")
        safe_preview = preview.encode("ascii", errors="replace").decode("ascii")
        print("\n--- forecast_data.csv preview (marker) ---")
        print(safe_preview)


if __name__ == "__main__":
    main()

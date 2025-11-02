import os
import random
import logging
import sys
from typing import List, Tuple, Iterator
import argparse  # kept unused; safe to remove if desired

from dotenv import load_dotenv
from botocore.exceptions import ClientError

from s3_client_wrapper import S3ClientWrapper


BUCKET_NAME = "mikro"  # Keep in sync with s3_image_uploader.py


def setup_logging(verbose: bool = False, log_to_file: str | None = None) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    fmt = '%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s'
    datefmt = '%Y-%m-%d %H:%M:%S'

    # Clear existing handlers to avoid duplicate logs when re-running
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)

    root.setLevel(level)
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(level)
    sh.setFormatter(formatter)
    root.addHandler(sh)

    if log_to_file:
        try:
            fh = logging.FileHandler(log_to_file, encoding='utf-8')
            fh.setLevel(level)
            fh.setFormatter(formatter)
            root.addHandler(fh)
        except Exception:
            # If file handler fails (e.g., permissions), continue with console logging
            pass

    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def iter_files(root_dir: str, recursive: bool = True) -> List[str]:
    files: List[str] = []
    if recursive:
        for base, _dirs, fnames in os.walk(root_dir):
            for f in fnames:
                full = os.path.join(base, f)
                if os.path.isfile(full):
                    files.append(full)
    else:
        try:
            for f in os.listdir(root_dir):
                full = os.path.join(root_dir, f)
                if os.path.isfile(full):
                    files.append(full)
        except FileNotFoundError:
            pass
    return files


def choose_random(files: List[str], n: int) -> List[str]:
    if not files:
        return []
    if n >= len(files):
        return files
    return random.sample(files, n)


def key_for_local_path(local_path: str) -> str:
    # Must match uploader logic: s3_path = local_path.lstrip('/')
    return local_path.lstrip('/')


def check_exists(s3_client, bucket: str, key: str) -> Tuple[bool, str]:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True, ""
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code')
        if code == '404':
            return False, "Not found (404)"
        return False, f"ClientError: {code}"
    except Exception as e:
        return False, f"Error: {e}"


def is_tiff(filename: str) -> bool:
    name = filename.lower()
    return name.endswith('.tif') or name.endswith('.tiff')


def find_random_tiff_in_tree(root_dir: str,
                             max_depth: int = 10,
                             max_scandir_per_dir: int = 1000) -> str:
    """
    Descend randomly from root_dir, scanning at most `max_scandir_per_dir` entries per directory.
    If a directory contains any TIFF files (within the cap), pick one randomly and return it.
    Returns empty string if none found within the constraints.
    """
    current = root_dir
    logging.debug(f"Descend: start at {current}, max_depth={max_depth}, max_scandir_per_dir={max_scandir_per_dir}")

    for _ in range(max_depth):
        tiffs: List[str] = []
        subdirs: List[str] = []
        scanned = 0
        try:
            with os.scandir(current) as it:
                for entry in it:
                    try:
                        if entry.is_file(follow_symlinks=False) and is_tiff(entry.name):
                            tiffs.append(entry.path)
                            if len(tiffs) >= 16:  # cap choices for speed
                                break
                        elif entry.is_dir(follow_symlinks=False):
                            subdirs.append(entry.path)
                    except OSError:
                        # Skip entries we cannot stat
                        continue
                    scanned += 1
                    if scanned >= max_scandir_per_dir:
                        break
        except (FileNotFoundError, NotADirectoryError, PermissionError):
            return ""

        logging.debug(f"Descend: scanned ~{scanned} entries in {current}, tiffs={len(tiffs)}, subdirs={len(subdirs)}")

        if tiffs:
            choice = random.choice(tiffs)
            logging.debug(f"Descend: found TIFF in {current}: {choice}")
            return choice

        if not subdirs:
            return ""

        current = random.choice(subdirs)
        logging.debug(f"Descend: continue into subdir {current}")

    return ""


def sample_random_tiffs(root_dir: str,
                        n: int,
                        max_attempts: int = 5000,
                        max_depth: int = 10,
                        max_scandir_per_dir: int = 1000) -> List[str]:
    """
    Attempts random descents to collect up to N unique TIFFs without walking the full tree.
    Limits total attempts to avoid long runtimes on sparse trees.
    """
    chosen: List[str] = []
    seen: set = set()
    attempts = 0

    logging.info(f"Sampling TIFFs: target={n}, max_attempts={max_attempts}, max_depth={max_depth}, per_dir_cap={max_scandir_per_dir}")
    while len(chosen) < n and attempts < max_attempts:
        attempts += 1
        found = find_random_tiff_in_tree(root_dir, max_depth=max_depth, max_scandir_per_dir=max_scandir_per_dir)
        if not found:
            if attempts % 100 == 0:
                logging.info(f"Sampling progress: attempts={attempts}, found={len(chosen)}/{n}")
            continue
        if found in seen:
            logging.debug(f"Duplicate candidate skipped: {found}")
            continue
        seen.add(found)
        chosen.append(found)
        logging.info(f"Sampling progress: selected {len(chosen)}/{n} -> {found}")
    return chosen


def yield_random_tiffs(root_dir: str,
                       n: int,
                       max_attempts: int = 5000,
                       max_depth: int = 10,
                       max_scandir_per_dir: int = 1000) -> Iterator[str]:
    """
    Generator version: yields up to N unique TIFF file paths as they are found
    via repeated random descents. Stops early if attempts are exhausted.

    This allows immediate verification/processing per file without collecting
    the full sample first.
    """
    seen: set = set()
    attempts = 0
    emitted = 0

    logging.info(
        f"Sampling (streaming): target={n}, max_attempts={max_attempts}, max_depth={max_depth}, per_dir_cap={max_scandir_per_dir}"
    )

    while emitted < n and attempts < max_attempts:
        attempts += 1
        found = find_random_tiff_in_tree(root_dir, max_depth=max_depth, max_scandir_per_dir=max_scandir_per_dir)
        if not found:
            if attempts % 100 == 0:
                logging.info(f"Streaming progress: attempts={attempts}, yielded={emitted}/{n}")
            continue
        if found in seen:
            logging.debug(f"Duplicate candidate skipped: {found}")
            continue
        seen.add(found)
        emitted += 1
        logging.info(f"Streaming progress: yielded {emitted}/{n} -> {found}")
        yield found


def main() -> int:
    # Hardcoded configuration (edit here as needed)
    ROOT_DIR = "/share/mikro3/"
    N = 10000
    # random-descent parameters (tune for performance):
    MAX_DEPTH = 12
    MAX_SCANDIR_PER_DIR = 2000
    SEED = None  # e.g., 123 for reproducibility
    VERBOSE = False
    LOG_FILE = "verifier.log"  # logs details of what happens during verification

    setup_logging(VERBOSE, log_to_file=LOG_FILE)

    if SEED is not None:
        random.seed(SEED)

    root_dir = ROOT_DIR

    if not os.path.isdir(root_dir):
        logging.error(f"Directory not found: {root_dir}")
        return 2

    load_dotenv()
    endpoint_url = os.getenv('ENDPOINT_URL')
    if not endpoint_url:
        logging.error("ENDPOINT_URL env var not set. Export it or add to .env")
        return 3

    s3_wrapper = S3ClientWrapper(endpoint_url=endpoint_url, region=os.getenv('AWS_REGION'))
    s3 = s3_wrapper.get_fresh_s3_client()

    logging.info(
        f"Starting verification with ROOT_DIR={root_dir}, N={N}, MAX_DEPTH={MAX_DEPTH}, PER_DIR_CAP={MAX_SCANDIR_PER_DIR}, SEED={SEED}"
    )

    hits = 0
    misses = 0
    errors = 0
    missing_examples = []

    # Output files for continuous logging
    FOUND_OUTFILE = "verifier_found.txt"
    MISSING_OUTFILE = "verifier_missing.txt"

    total = 0
    stream = yield_random_tiffs(
        root_dir,
        N,
        max_attempts=max(2000, N * 200),
        max_depth=MAX_DEPTH,
        max_scandir_per_dir=MAX_SCANDIR_PER_DIR,
    )

    # Open output files in line-buffered append mode
    with open(FOUND_OUTFILE, "a", encoding="utf-8") as f_found, \
         open(MISSING_OUTFILE, "a", encoding="utf-8") as f_missing:
        for idx, local_path in enumerate(stream, start=1):
            total = idx
            key = key_for_local_path(local_path)
            logging.info(f"[{idx}/{N}] Checking S3 existence: s3://{BUCKET_NAME}/{key}")
            exists, err = check_exists(s3, BUCKET_NAME, key)
            if exists:
                logging.info(f"FOUND: s3://{BUCKET_NAME}/{key}")
                hits += 1
                try:
                    f_found.write(f"{local_path}\t s3://{BUCKET_NAME}/{key}\n")
                    f_found.flush()
                except Exception:
                    pass
            else:
                if err.startswith("ClientError") or err.startswith("Not found"):
                    logging.info(f"MISSING: s3://{BUCKET_NAME}/{key} ({err})")
                    misses += 1
                    if len(missing_examples) < 10:
                        missing_examples.append((local_path, key, err))
                    try:
                        f_missing.write(f"{local_path}\t s3://{BUCKET_NAME}/{key}\t {err}\n")
                        f_missing.flush()
                    except Exception:
                        pass
                else:
                    logging.warning(f"ERROR: s3://{BUCKET_NAME}/{key} -> {err}")
                    errors += 1

    if total == 0:
        logging.warning(f"Could not locate any TIFFs under {root_dir} within attempt limits")
        return 0

    print("")
    print("Verification summary:")
    print(f"  Checked: {total}")
    print(f"  Found in S3: {hits}")
    print(f"  Missing in S3: {misses}")
    print(f"  Errors: {errors}")

    if missing_examples:
        print("")
        print("Missing examples (up to 10):")
        for lp, k, e in missing_examples:
            print(f"  local={lp} -> s3://{BUCKET_NAME}/{k} :: {e}")

    return 0 if (misses == 0 and errors == 0) else 1


if __name__ == "__main__":
    raise SystemExit(main())

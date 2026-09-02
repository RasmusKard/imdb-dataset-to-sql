from os import path, remove
import os
import os.path
import tempfile
from urllib.parse import urlsplit
from urllib.request import urlretrieve
import uuid
import gzip
from shutil import copyfileobj

_IMDB_DATASET_HOST = "datasets.imdbws.com"


def _confined(directory):
    """Normalize `directory` and require it to sit inside the system temp dir.

    Returns the normalized directory, raises for anything else. commonpath is the
    confinement test: unlike a startswith string check it cannot be fooled by a sibling
    directory whose name shares a prefix, and realpath has already collapsed any `..`.
    """
    root = os.path.realpath(tempfile.gettempdir())
    resolved = os.path.realpath(directory)
    if os.path.commonpath([resolved, root]) != root or ".." in directory.split(os.sep):
        raise ValueError(f"path is not under the temp dir: {directory!r}")
    return resolved


def join_path_with_random_uuid(directory):
    """A uuid-named scratch path under `directory`.

    Every caller passes a TemporaryDirectory; the result is always the confined,
    normalized directory plus a name generated here, so it cannot point anywhere else
    however the caller was reached.
    """
    return os.path.join(_confined(directory), str(uuid.uuid4()))


def download_imdb_dataset(url, directory):
    """Download and gunzip an IMDb dataset dump into `directory`; returns the path.

    `directory` must normalize to inside the system temp dir (every caller passes a
    TemporaryDirectory), the URL must be an https dataset URL on IMDb's host, and both
    scratch files are created by mkstemp inside that directory - so neither the fetch
    nor the writes can be steered anywhere else, and neither file can be preempted by a
    symlink planted in the temp dir.
    """
    parts = urlsplit(url)
    if parts.scheme != "https" or parts.netloc != _IMDB_DATASET_HOST:
        raise ValueError(f"not an IMDb dataset URL: {url!r}")

    scratch_dir = _confined(directory)

    gz_fd, gz_file_path = tempfile.mkstemp(dir=scratch_dir, suffix=".tsv.gz")
    os.close(gz_fd)
    urlretrieve(url, gz_file_path)

    out_fd, output_path = tempfile.mkstemp(dir=scratch_dir, suffix=".tsv")
    try:
        with gzip.open(gz_file_path, "rb") as f_in:
            with os.fdopen(out_fd, "wb") as f_out:
                copyfileobj(f_in, f_out)
    finally:
        if path.exists(gz_file_path):
            remove(gz_file_path)

    return output_path

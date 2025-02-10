from os import path, remove
import os.path
from urllib.request import urlretrieve
import uuid
import gzip
from shutil import copyfileobj
from sqlalchemy.dialects import registry


def join_path_with_random_uuid(path):
    return os.path.join(path, str(uuid.uuid4()))


def download_imdb_dataset(url, output_path):
    gz_file_path = output_path + ".gz"
    urlretrieve(url, gz_file_path)

    with gzip.open(gz_file_path, "rb") as f_in:
        with open(output_path, "wb") as f_out:
            copyfileobj(f_in, f_out)

    if path.exists(gz_file_path):
        remove(gz_file_path)

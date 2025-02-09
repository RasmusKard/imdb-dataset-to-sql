import os.path
import uuid


def join_path_with_random_uuid(path):
    return os.path.join(path, str(uuid.uuid4()))

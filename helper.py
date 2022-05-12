from google.cloud import storage


# Object's metadata

def get_object_metadata(bucket_name, blob_name):
    """
    This function will retrieve the metadata of the object

    :param:
        bucket_name: (str) - name of the gcs bucket
        blob_name: (str) - name of the object for which metadata is to be retrieved

    :return:
        metadata of object in dict form """

    # Instantiates the storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name=bucket_name)

    # Retrieve a blob, and its metadata, from Google Cloud Storage.
    blob = bucket.get_blob(blob_name=blob_name)
    metadata = blob.metadata  # type - dict
    # log.info(f"Metadata: {metadata}")

    return metadata


# Object's creation time (ctime)

def get_object_ctime(bucket_name, blob_name):
    """
    This function will retrieve the ctime of the object

    :param:
        bucket_name: (str) - name of the gcs bucket
        blob_name: (str) - name of the object for which ctime is to be retrieved

    :return:
        ctime of the object in datetime format """

    # Instantiates the storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name=bucket_name)

    # Retrieve a blob, and its metadata, from Google Cloud Storage.
    blob = bucket.get_blob(blob_name=blob_name)
    ctime = blob.time_created  # type - datetime
    # log.info(f"ctime : {ctime}")

    return ctime


# Object's modification time (mtime)

def get_object_mtime(bucket_name, blob_name):
    """
    This function will retrieve the mtime of the object

    :param:
        bucket_name: (str) - name of the gcs bucket
        blob_name: (str) - name of the object for which mtime is to be retrieved

    :return:
        mtime of the object in datetime format """

    # Instantiates the storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name=bucket_name)

    # Retrieve a blob, and its metadata, from Google Cloud Storage.
    blob = bucket.get_blob(blob_name=blob_name)
    mtime = blob.updated  # type - datetime
    # log.info(f"mtime : {mtime}")

    return mtime


# Hash of the object - crc32c

def get_object_crc32(bucket_name, blob_name):
    """
    This function will retrieve the crc32c hash of the object

    :param:
        bucket_name: (str) - name of the gcs bucket
        blob_name: (str) - name of the object for which crc32c is to be retrieved

    :return:
        crc32c value of the object in str """

    # Instantiates the storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name=bucket_name)

    # Retrieve a blob, and its metadata, from Google Cloud Storage.
    blob = bucket.get_blob(blob_name=blob_name)
    crc32c = blob.crc32c  # type - str
    # log.info(f"crc32c_val: {crc32c}")

    return crc32c


# Object's size in bytes

def get_object_size(bucket_name, blob_name):
    """
    This function will retrieve the size of the object

    :param:
        bucket_name: (str) - name of the gcs bucket
        blob_name: (str) - name of the object for which size is to be retrieved

    :return:
        size of the object in bytes - type int """

    # Instantiates the storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name=bucket_name)

    # Retrieve a blob, and its metadata, from Google Cloud Storage.
    blob = bucket.get_blob(blob_name=blob_name)
    size = blob.size  # type - int
    size_in_gb = size / pow(10, 9)
    # log.info(f"size_in_gb : {size_in_gb}")

    return size_in_gb


# Buckets label

def get_bucket_labels(bucket_name):
    """
    This function will retrieve the labels of the bucket

    :param:
        bucket_name: (str) - name of the gcs bucket

     :return:
        labels as dict {Key:Value} """

    # Instantiates the storage client

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    # Bucket labels
    bucket_labels = bucket.labels
    # log.info(f"bucket_labels: {bucket_labels}")

    return bucket_labels


# Bucket creation time

def get_bucket_ctime(bucket_name):
    """
    This function will retrieve the ctime of the bucket

    :param:
        bucket_name: (str) - name of the gcs bucket

     :return:
        ctime as datetime """

    # Instantiates the storage client

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    # Bucket labels
    bucket_ctime = bucket.time_created
    # log.info(f"bucket_labels: {bucket_labels}")

    return bucket_ctime


# Set metadata

def set_object_metadata(bucket_name, blob_name, metadata: dict):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.get_blob(blob_name)
    blob.metadata = metadata
    blob.patch()


def get_object_acl_source(source_bucket_name, blob_name):
    """Prints out a blob's access control list."""

    storage_client = storage.Client()
    bucket = storage_client.bucket(source_bucket_name)
    blob = bucket.blob(blob_name)
    object_acl_source = blob.acl
    source_acl_list = []

    for entry in object_acl_source:
        source_acl_list.append(entry)

    return source_acl_list


def check_and_set_acl_to_dest(dest_bucket_name, blob_name, source_acl_list):
    storage_client = storage.Client()
    bucket = storage_client.bucket(dest_bucket_name)
    blob = bucket.blob(blob_name)
    object_acl_dest = blob.acl
    dest_acl_list = []

    for entry in object_acl_dest:
        dest_acl_list.append(entry)

    if source_acl_list != dest_acl_list:
        object_acl_dest.save(acl=source_acl_list)  # saves the source acl to destination
        return True
    else:
        return False


def set_acl_to_dest(dest_bucket_name, blob_name, source_acl_list):
    storage_client = storage.Client()
    bucket = storage_client.bucket(dest_bucket_name)
    blob = bucket.blob(blob_name)
    object_acl = blob.acl
    object_acl.save(acl=source_acl_list)





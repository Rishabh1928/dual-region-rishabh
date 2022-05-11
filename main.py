import os,json
from flask import Flask, request
import subprocess
import base64
import logging as log
import google.cloud.logging as logging
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


def get_object_acl(bucket_name, blob_name):
    """Prints out a blob's access control list."""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    acl_list = []

    for entry in blob.acl:
        acl_list.append(entry)
        # print("{}: {}".format(entry["role"], entry["entity"]))

    return acl_list



app = Flask(__name__)

@app.route("/create", methods=['POST'])
def create():
    logging_client = logging.Client()
    logging_client.setup_logging()
    data = request.get_json()
    log.info("create method called")
    wholedata = f"wholedata : {data} ::end of data"
    log.info(wholedata)
    prt = "methodName:"+data['protoPayload']['methodName']
    #log.info(prt)
    prt = prt+" -- resourceName:"+data['protoPayload']['resourceName']
    #log.info(prt)
    prt = prt+" -- bucket_name:"+data['resource']['labels']['bucket_name']
    #log.info(prt)
    prt = prt+" -- location:"+data['resource']['labels']['location']
    log.info(prt)
    scr_bucket = data['resource']['labels']['bucket_name']
    obj_name = data['protoPayload']['resourceName'].split("/objects/")[1]
    source = "gs://" + scr_bucket
    sp = subprocess.Popen(["gsutil","label","get",source],stdout=subprocess.PIPE)
    out = sp.stdout.read()
    if "no label configuration" not in str(out,"utf-8"):
        #log.info(type(out))
        #log.info(out)
        try:
            dr_flg = json.loads(str(out,"utf-8"))
        except:
            log.info("not a dual-region bucket, event skipped")
            return ("OK",200)
        if "dual-region" in dr_flg.keys():
            if dr_flg["dual-region"] != "true":
                log.info("bucket is not dual region, event skipped")
                return ("ok",200)
        else:
            log.info("bucket is not dual region, event skipped")
            return ("ok",200)
    else:
        log.info("bucket is not dual region, event skipped")
        return ("ok",200)
    if obj_name[-1] == '/':
        log.info("folder created..., event skipped")
        return('ok',200)
    source = "gs://" + scr_bucket + "/" + obj_name
    dest_bucket = scr_bucket + "-delhi-backup/"
    dest = "gs://" + dest_bucket + obj_name
    proc = subprocess.Popen(["gsutil", "-m", "cp", "-r", "-p", source, dest])
    try:
        outs, errs = proc.communicate()
        return ('OK', 200)
    except Exception as e:
        log.info(e)
        proc.kill()
        outs, errs = proc.communicate()
        return ('NOT OK', 400)

@app.route("/update", methods=['POST'])
def update():
    logging_client = logging.Client()
    logging_client.setup_logging()
    data = request.get_json()
    #log.info("root method called...")
    wholedata = f"wholedata : {data} ::end of data"
    #log.info(wholedata)
    prt = "data['protoPayload']['methodName']:"+data['protoPayload']['methodName']
    #log.info(prt)
    prt = prt+" -- data['protoPayload']['resourceName']:"+data['protoPayload']['resourceName']
    #log.info(prt)
    prt = prt+ " -- data['resource']['labels']['bucket_name']:"+data['resource']['labels']['bucket_name']
    #log.info(prt)
    prt = prt + "-- data['resource']['labels']['location']:"+data['resource']['labels']['location']
    log.info(prt)
    scr_bucket = data['resource']['labels']['bucket_name']
    obj_name = data['protoPayload']['resourceName'].split("/objects/")[1]
    source = "gs://" + scr_bucket
    sp = subprocess.Popen(["gsutil","label","get",source],stdout=subprocess.PIPE)
    out = sp.stdout.read()
    if "no label configuration" not in str(out,"utf-8"):
        #log.info(type(out))
        #log.info(out)
        try:
            dr_flg = json.loads(str(out,"utf-8"))
        except:
            log.info("not a dual-region bucket, event skipped")
            return ("OK",200)
        if "dual-region" in dr_flg.keys():
            if dr_flg["dual-region"] != "true":
                log.info("bucket is not dual region, event skipped")
                return ("ok",200)
        else:
            log.info("bucket is not dual region, event skipped")
            return ("ok",200)
    else:
        log.info("bucket is not dual region, event skipped")
        return ("ok",200)
    if obj_name[-1] == '/':
        log.info("folder created..., event skipped")
        return('ok',200)
    source = "gs://" + scr_bucket + "/" + obj_name
    dest_bucket = scr_bucket + "-delhi-backup/"
    dest = "gs://" + dest_bucket + obj_name
    cmd = "gsutil"+" acl"+" get "+ source + " > /acl.txt "
    sp = os.popen(cmd)
    out = sp.read()
    cmd = "gsutil"+" acl"+" set"+" /acl.txt "+ dest
    sp = os.popen(cmd)
    outs = sp.read()
    cmd = "gsutil"+" stat "+ source
    sp = os.popen(cmd)
    outs = sp.read()
    a=outs.split("Metadata:")[1].split("Hash")[0].split('\n')[1:-1]
    b={}
    cmd = "gsutil setmeta "
    for x in range(0,len(a)):
        tmp = ' '.join(a[x].split())
        b["x-goog-meta-"+tmp.split(': ')[0]] = tmp.split(': ')[1]
        cmd = cmd + '-h "' + "x-goog-meta-"+tmp.split(': ')[0] + ":" + tmp.split(': ')[1] + '" '
    log.info(b)
    cmd = cmd + dest
    sp = os.popen(cmd)
    log.info(sp.read())
    return ('OK', 200)

@app.route("/", methods=['POST'])
def main():
    log.info("root method call")
    return ('OK', 200)

@app.route("/delete", methods=['POST'])
def delete():
    logging_client = logging.Client()
    logging_client.setup_logging()
    data = request.get_json()
    #log.info("root method called...")
    wholedata = f"wholedata : {data} ::end of data"
    #log.info(wholedata)
    prt = "data['protoPayload']['methodName']:"+data['protoPayload']['methodName']
    #log.info(prt)
    prt = prt+" -- data['protoPayload']['resourceName']:"+data['protoPayload']['resourceName']
    #log.info(prt)
    prt = prt+ " -- data['resource']['labels']['bucket_name']:"+data['resource']['labels']['bucket_name']
    #log.info(prt)
    prt = prt + "-- data['resource']['labels']['location']:"+data['resource']['labels']['location']
    log.info(prt)
    scr_bucket = data['resource']['labels']['bucket_name']
    obj_name = data['protoPayload']['resourceName'].split("/objects/")[1]
    source = "gs://" + scr_bucket
    sp = subprocess.Popen(["gsutil","label","get",source],stdout=subprocess.PIPE)
    out = sp.stdout.read()
    if "no label configuration" not in str(out,"utf-8"):
        #log.info(type(out))
        #log.info(out)
        try:
            dr_flg = json.loads(str(out,"utf-8"))
        except:
            log.info("not a dual-region bucket, event skipped")
            return ("OK",200)
        if "dual-region" in dr_flg.keys():
            if dr_flg["dual-region"] != "true":
                log.info("bucket is not dual region, event skipped")
                return ("ok",200)
        else:
            log.info("bucket is not dual region, event skipped")
            return ("ok",200)
    else:
        log.info("bucket is not dual region, event skipped")
        return ("ok",200)
    source = "gs://" + scr_bucket + "/" + obj_name
    dest_bucket = scr_bucket + "-delhi-backup/"
    dest = "gs://" + dest_bucket + obj_name
    cmd = "gsutil" + " rm" + " -r " + '"' + dest + '"'
    sp = os.popen(cmd)
    log.info(sp.read())
    return('OK',200)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

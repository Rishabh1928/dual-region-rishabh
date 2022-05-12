import os,json
from flask import Flask, request
import subprocess
import base64
import logging as log
import google.cloud.logging as logging
from google.cloud import storage
from helper import get_bucket_labels, get_object_ctime, get_object_mtime, get_object_size, get_object_crc32, \
    get_object_metadata, set_object_metadata, get_object_acl_source, check_and_set_acl_to_dest, set_acl_to_dest


app = Flask(__name__)

@app.route("/create", methods=['POST'])
def create():
    """Creates the same object in the destination bucket."""

    # Instantiates a logging client
    logging_client = logging.Client()
    logging_client.setup_logging()

    # Get the data from request
    data = request.get_json()

    # Complete data in the request
    complete_data = f"Start: {data} : End"
    log.info(complete_data)

    # Extract relevant data
    source_bucket_name = data["message"]["attributes"]["bucketId"]
    object_name = data["message"]["attributes"]["objectId"]

    # Relevant data combined
    relevant_data = f"Bucket-Name: {source_bucket_name}, Object-Name: {object_name}"
    log.info(relevant_data)

    # Derived fields
    gs_source_bucket = f"gs://{source_bucket_name}"

    # Check if a folder is created or an object
    if object_name[-1] == "/":
        log.info("Folder created, event skipped..")
        return "OK", 200

    gs_source_object_url = f"{gs_source_bucket}/{object_name}"

    # Get labels attached to the bucket for the dual region check
    bucket_labels = get_bucket_labels(source_bucket_name)

    if bucket_labels:  # means labels are there
        if "dual-region" in bucket_labels.keys():
            if bucket_labels["dual-region"] != "rishabh-true":
                log.info("Dual-region is not enabled for this bucket. Event Skipped..")
                return "OK", 200
        else:
            log.info("Dual-region is not enabled for this bucket. Event Skipped..")
            return "OK", 200
    else:
        log.info("Dual-region is not enabled for this bucket. Event Skipped..")
        return "OK", 200

    dest_bucket_name = source_bucket_name + "-delhi-dr-backup"
    gs_dest_bucket = f"gs://{dest_bucket_name}"
    gs_dest_object_url = f"{gs_dest_bucket}/{object_name}"

    # Object's existence check in destination
    sub_proc_object_check = subprocess.Popen(["gsutil", "-q", "stat", gs_dest_object_url], stdout=subprocess.PIPE)
    stream_data = sub_proc_object_check.communicate()[0]
    exit_code = sub_proc_object_check.returncode

    # Get different info about the object

    ctime_source = get_object_ctime(source_bucket_name, object_name)
    pre_copy_mtime_source = get_object_mtime(source_bucket_name, object_name)
    crc32c_source = get_object_crc32(source_bucket_name, object_name)
    object_size_gb = get_object_size(source_bucket_name, object_name)

    # Check if objects exists & size of object is greater than 10 GB

    if exit_code != 0:
        log.info("Object doesn't exists in destination.")
        if object_size_gb < 10:
            log.info("Object size is less than 10GB. Copying started via cloud run...")
            # Copying using gsutil
            sp_copy = subprocess.Popen(["gsutil", "-m", "cp", "-r", "-p", gs_source_object_url, gs_dest_object_url])
            try:
                out, errs = sp_copy.communicate()
            except Exception as e:
                log.info(e)
                sp_copy.kill()
                out, errs = sp_copy.communicate()

            post_copy_mtime_source = get_object_mtime(source_bucket_name, object_name)

            if pre_copy_mtime_source != post_copy_mtime_source:
                meta_source = get_object_metadata(source_bucket_name, object_name)
                set_object_metadata(dest_bucket_name, object_name, metadata=meta_source)
                log.info("The metadata for the destination object is set")

                # update acl
                return "OK", 200

            else:
                log.info("Object copied successfully to destination..")
                return "OK", 200
        else:
            log.info("Object size is greater than 10 GB, copying will get started via GKE..")
            # gke function will be called here
            return "OK", 200
    else:
        log.info("Object exists in destination. Performing checks!")

        # Get different info about the object in the destination

        ctime_dest = get_object_ctime(dest_bucket_name, object_name)
        pre_copy_mtime_dest = get_object_mtime(dest_bucket_name, object_name)
        crc32c_dest = get_object_crc32(dest_bucket_name, object_name)

        # Checks

        if crc32c_source != crc32c_dest:
            if not (ctime_source < ctime_dest):
                if object_size_gb < 10:
                    log.info("Object size is less than 10GB. Copying started via cloud run...")
                    # Copying using gsutil
                    sp_copy = subprocess.Popen(
                        ["gsutil", "-m", "cp", "-r", "-p", gs_source_object_url, gs_dest_object_url])
                    try:
                        out, errs = sp_copy.communicate()
                    except Exception as e:
                        log.info(e)
                        sp_copy.kill()
                        out, errs = sp_copy.communicate()

                    post_copy_mtime_source = get_object_mtime(source_bucket_name, object_name)

                    if pre_copy_mtime_source != post_copy_mtime_source:
                        # update meta
                        meta_source = get_object_metadata(source_bucket_name, object_name)
                        try:
                            set_object_metadata(dest_bucket_name, object_name, metadata=meta_source)
                            log.info("Source metadata successfully set to destination..")
                        except Exception as e:
                            log.info(e)

                        # update acl
                        source_acl_list = get_object_acl_source(source_bucket_name, object_name)
                        try:
                            set_acl_to_dest(dest_bucket_name, object_name, source_acl_list)
                            log.info("Source ACL successfully set to destination..")
                        except Exception as e:
                            log.info(e)

                        return "OK", 200

                    else:
                        log.info("Object copied successfully to destination..")
                        return "OK", 200
                else:
                    log.info("Object size is greater than 10 GB, copying will get started via GKE..")
                    # gke function will be called here
                    return "OK", 200
            else:
                log.info("No need to copy object. Checking ACL & Meta...")
        else:
            log.info("No need to copy object. Checking ACL & Meta...")

            # Get & check source and destination metadata

            meta_source = get_object_metadata(source_bucket_name, object_name)
            meta_dest = get_object_metadata(dest_bucket_name, object_name)

            if meta_source != meta_dest:  # order of keys doesn't matter
                try:
                    set_object_metadata(dest_bucket_name, object_name, metadata=meta_source)
                    log.info("Source metadata is set to destination..")
                except Exception as e:
                    log.info(e)
            else:
                log.info("Metadata is same for both source & destination..")

            # Get & check source & destination acl

            source_acl_list = get_object_acl_source(source_bucket_name, object_name)
            check_set_acl = check_and_set_acl_to_dest(dest_bucket_name, object_name, source_acl_list)

            if check_set_acl:  # true - source acl is not equal to dest & source acl is set to dest
                log.info("Source ACL is set to destination..")
            else:
                log.info("ACL is same for both source & destination..")

            return "OK", 200
        
        
        
        
            
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

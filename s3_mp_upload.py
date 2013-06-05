#!/usr/bin/env python

"""
https://github.com/mumrah/s3-multipart

"""

from cStringIO import StringIO
import logging
from math import ceil
from multiprocessing import Pool
import sys
import time
import urlparse

import boto

import os, sys, time, subprocess, datetime
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from housepy import log, config


def do_part_upload(args):
    """
    Upload a part of a MultiPartUpload

    Open the target file and read in a chunk. Since we can't pickle
    S3Connection or MultiPartUpload objects, we have to reconnect and lookup
    the MPU object with each part upload.

    :type args: tuple of (string, string, string, int, int, int)
    :param args: The actual arguments of this method. Due to lameness of
                 multiprocessing, we have to extract these outside of the
                 function definition.

                 The arguments are: S3 Bucket name, MultiPartUpload id, file
                 name, the part number, part offset, part size
    """
    # Multiprocessing args lameness
    bucket_name, mpu_id, fname, i, start, size = args
    log.debug("do_part_upload got args: %s" % (args,))

    # Connect to S3, get the MultiPartUpload
    s3 = boto.connect_s3(config['aws']['access_key_id'], config['aws']['secret_access_key'])
    bucket = s3.lookup(bucket_name)
    mpu = None
    for mp in bucket.list_multipart_uploads():
        if mp.id == mpu_id:
            mpu = mp
            break
    if mpu is None:
        raise Exception("Could not find MultiPartUpload %s" % mpu_id)

    # Read the chunk from the file
    fp = open(fname, 'rb')
    fp.seek(start)
    data = fp.read(size)
    fp.close()
    if not data:
        raise Exception("Unexpectedly tried to read an empty chunk")

    def progress(x,y):
        log.info("Part %d: %0.2f%%" % (i+1, 1.*x/y))

    # Do the upload
    t1 = time.time()
    mpu.upload_part_from_file(StringIO(data), i+1, cb=progress)

    # Print some timings
    t2 = time.time() - t1
    s = len(data)/1024./1024.
    log.info("Uploaded part %s (%0.2fM) in %0.2fs at %0.2fMbps" % (i+1, s, t2, s/t2))

def main(src, dest, num_processes=2, split=50, force=False, reduced_redundancy=False, verbose=False):
    # Check that dest is a valid S3 url

    src = open(src)
    s3 = boto.connect_s3(config['aws']['access_key_id'], config['aws']['secret_access_key'])
    bucket = s3.lookup(config['aws']['bucket'])
    key = bucket.get_key(dest)
    # See if we're overwriting an existing key
    if key is not None:
        if not force:
            raise ValueError("'%s' already exists. Specify -f to overwrite it" % dest)

    # Determine the splits
    part_size = max(5*1024*1024, 1024*1024*split)
    src.seek(0,2)
    size = src.tell()
    num_parts = int(ceil(size / part_size))

    # If file is less than 5M, just upload it directly
    if size < 5*1024*1024:
        src.seek(0)
        t1 = time.time()
        k = boto.s3.key.Key(bucket,dest)
        k.set_contents_from_file(src)
        t2 = time.time() - t1
        s = size/1024./1024.
        log.info("Finished uploading %0.2fM in %0.2fs (%0.2fMbps)" % (s, t2, s/t2))
        return

    # Create the multi-part upload object
    mpu = bucket.initiate_multipart_upload(dest, reduced_redundancy=reduced_redundancy)
    log.info("Initialized upload: %s" % mpu.id)

    # Generate arguments for invocations of do_part_upload
    def gen_args(num_parts, fold_last):
        for i in range(num_parts+1):
            part_start = part_size*i
            if i == (num_parts-1) and fold_last is True:
                yield (bucket.name, mpu.id, src.name, i, part_start, part_size*2)
                break
            else:
                yield (bucket.name, mpu.id, src.name, i, part_start, part_size)


    # If the last part is less than 5M, just fold it into the previous part
    fold_last = ((size % part_size) < 5*1024*1024)

    # Do the thing
    try:
        # Create a pool of workers
        pool = Pool(processes=num_processes)
        t1 = time.time()
        pool.map_async(do_part_upload, gen_args(num_parts, fold_last)).get(9999999)
        # Print out some timings
        t2 = time.time() - t1
        s = size/1024./1024.
        # Finalize
        src.close()
        mpu.complete_upload()
        log.info("Finished uploading %0.2fM in %0.2fs (%0.2fMbps)" % (s, t2, s/t2))
    except KeyboardInterrupt:
        log.warning("Received KeyboardInterrupt, canceling upload")
        pool.terminate()
        mpu.cancel_upload()
    except Exception, err:
        log.error("Encountered an error, canceling upload")
        log.error(err)
        mpu.cancel_upload()

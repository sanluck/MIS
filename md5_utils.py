#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# md5_utils.py - расчет md5 файла
#

import os
import sys
import hashlib
import logging

if __name__ == "__main__":
    LOG_FILENAME = '_md5.out'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

FNAME = "150420003.xml"
FPATH = "/home/gnv/MIS/LIS/exchange.1/"

def md5_file(file_path):

    if os.path.exists(file_path) == False:
        return None

    md5 = hashlib.md5(open(file_path).read()).hexdigest()

    return md5

def write_md5(file_path):

    fmd5 = md5_file(file_path)
    fname = os.path.basename(ffname)
    sout = "MD5 ({0}) = {1}".format(fname, fmd5)
    fileName, fileExtension = os.path.splitext(file_path)
    fname_md5 = file_path.replace(fileExtension, ".md5")
    file_md5 = open(fname_md5, "w")
    file_md5.write(sout)
    file_md5.close()

if __name__ == "__main__":

    ffname = FPATH + FNAME
    sout = "file: {0}".format(ffname)
    log.info(sout)

    fmd5 = md5_file(ffname)
    #fmd5 = hashlib.md5(open(ffname).read()).hexdigest()
    fname = os.path.basename(ffname)
    sout = "MD5 ({0}) = {1}".format(fname, fmd5)
    log.info(sout)
    fileName, fileExtension = os.path.splitext(ffname)
    sout = "fileExtension: {0}".format(fileExtension)
    log.info(sout)
    write_md5(ffname)

    sys.exit(0)

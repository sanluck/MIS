#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# copyf-2.py - read input file line by line
#              in case last character is <;> delete it
#              write it to output file
#
#

import logging
import sys, codecs
from cStringIO import StringIO

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

LOG_FILENAME = '_copyf_cad.out'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

IN_PATH        = "./FIN"
OUT_PATH       = "./FOUT"

SM_CH          = u';'
SM_COUNT       = 22

CHECK_DATES    = True
i_dates        = [7,11,17]

CHECK_MOTIVE   = True
i_motive_att   = 15

CHECK_SNILS    = True
i_snils        = 13

CHECK_ENP      = True
i_enp          = 3


def get_fnames(path = IN_PATH, file_ext = '.csv'):
# get file names
    
    import os    
    
    fnames = []
    for subdir, dirs, files in os.walk(path):
        for fname in files:
            if fname.find(file_ext) > 1:
                log.info(fname)
                fnames.append(fname)
    
    return fnames    

def get_st(fname):
# read file line by line into arry    

    ins = open( fname, "r" )

    array = []
    for line in ins:
        u_line = line.decode('cp1251')
        l_line = len(line)
        l1 = l_line - 1
        l2 = l_line - 2
        
        if (l2 >= 0) and (u_line[l2] in ('\r','\n')):
            u_line = u_line[:l2]
        elif (l1>= 0) and (u_line[l1] in ('\r','\n')):
            u_line = u_line[:l1]
        
        array.append( u_line )
    
    ins.close()    
    
    return array

def write_st(ar, fout):
# write lines to output file line by line
    from datetime import date
    
    d_now = date.today()
    y_now = d_now.year
    
    fo = open(fout, "wb")
    
    nnn = len(ar)
    i   = 0
    for line in ar:
        i += 1

        a_l = line.split(SM_CH)
        sm_count = len(a_l) - 1
        if sm_count <> SM_COUNT:
            sout = "Wrong SM_COUNT in the line:"
            log.warn( sout )
            sout = "   {0}".format(line.encode('utf-8'))
            log.warn( sout )
            continue

        if CHECK_DATES:
            l_wrong = False
            sd_w    = ''
            for i_d in i_dates:
                sd = a_l[i_d]
                if len(sd) == 0: continue
                if sd[:3] not in (u'"19', u'"20'):
                    l_wrong = True
                    sd_w = sd
                    break
                sy  = sd[1:5]
                nsy = int(sy)
                if nsy > y_now:
                    l_wrong = True
                    sd_w = sd
                    break
            
            if l_wrong:
                sout = "Wrong Date ({0}) in the line:".format(sd)
                log.warn( sout )
                sout = "   {0}".format(line.encode('utf-8'))
                log.warn( sout )
                continue
        if CHECK_MOTIVE:
            s_motive = a_l[i_motive_att]
            if len(s_motive) == 0:
                a_l[i_motive_att] = '"0"'
            elif s_motive not in (u'"0"', u'"1"', u'"2"'):
                sout = "Wrong Motive_Att ({0}) in the line:".format(s_motive)
                log.warn( sout )
                sout = "   {0}".format(line.encode('utf-8'))
                log.warn( sout )
                a_l[i_motive_att] = '"2"'
                
        if CHECK_SNILS:
            SNILS = a_l[i_snils]
            if len(SNILS) > 13:
                SNILS = SNILS.replace(u" ", "")
                SNILS = SNILS.replace(u"-", "")
            if len(SNILS) <> 13:
                SNILS = ''
            a_l[i_snils] = SNILS

        if CHECK_ENP:
            ENP = a_l[i_enp]
            l_enp = len(ENP)
            if l_enp not in (0, 18):
                ENP = ''
                a_l[i_enp] = ENP
        
        lll = SM_CH.join(a_l)    
        if i == nnn: 
            lout = lll.upper().encode('cp1251')
        else:
            lout = lll.upper().encode('cp1251')+"\r\n"
        
        fo.write(lout)
                 
    fo.close()
    
    
if __name__ == "__main__":
    
    import os, shutil
    import time

    log.info("======================= COPYF-CAD ===========================================")
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Copying and processing of files. Start {0}'.format(localtime))

    fnames = get_fnames()
    n_fnames = len(fnames)
    sout = "Totally {0} files has been found".format(n_fnames)
    log.info( sout )
    
    for fname in fnames:

        mcod = fname[3:9]
        f_fname = IN_PATH + "/" + fname
        sout = "Input file: {0} mcod: {1}".format(f_fname, mcod)
        log.info(sout)
    
        ar = get_st(f_fname)
        l_ar = len(ar)
        sout = "File has got {0} lines".format(l_ar)
        log.info( sout )
        
        fout_name = fname
        ar2 = ar
        l_ar = len(ar2)
        sout = "Totally {0} lines to be processed".format(l_ar)
        log.info( sout )

        destination = OUT_PATH + "/" + fout_name
        
        write_st(ar2, destination)
        
        sout = "Output file: {0}".format(destination)
        log.info(sout)
    
    localtime = time.asctime( time.localtime(time.time()) )
    log.info('Copying and processing of files. Finish  '+localtime)  
    
    sys.exit(0)

#!/usr/bin/env python
# encoding: utf8


class InsorgInfoItem:
    def __init__(self, code, name, inn = None, ogrn = None, okato = None, mcod = None):
        self.code  = code
        self.name  = name
        self.inn   = inn
        self.ogrn  = ogrn
        self.okato = okato
        self.mcod  = mcod
        
    def __unicode__(self):
        return u"InsorgInfo({0}/{1},{2},{3})".format( self.code, self.mcod, self.name, self.ogrn)

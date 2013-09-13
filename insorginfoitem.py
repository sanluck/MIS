#!/usr/bin/env python
# encoding: utf8


class InsorgInfoItem:
    def __init__(self, code, name, inn = None, ogrn = None, okato = None):
        self.code  = code
        self.name  = name
        self.inn   = inn
        self.ogrn  = ogrn
        self.okato = okato
        
    def __unicode__(self):
        return u"InsorgInfo({0},{1},{2})".format( self.code, self.name, self.ogrn)
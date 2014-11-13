#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# ConfigSection.py - формирования файла ПН для выгрузки в Минздрав
#

def ConfigSectionMap(Config, Section):
    dict1 = {}
    options = Config.options(Section)
    for option in options:
        try:
            dict1[option] = Config.get(Section, option)
            if dict1[option] == -1:
                log.debug("skip: %s" % option)
        except:
            log.warn("exception on %s!" % option)
            dict1[option] = None
    return dict1
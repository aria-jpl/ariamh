#! /usr/bin/env python3
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Author: Giangi Sacco
# Copyright 2013, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import json
_contextFile = '_context.json'
def toContext(process,exit,message):
    contex = None
    with open(_contextFile,'r') as fp:
        context = json.load(fp)
    if context:
        if 'processes' in context:
            context['processes'].update({process:{'returnStatus':{'value':exit,'description':message}}})
        else:
            context.update({'processes':{process:{'returnStatus':{'value':exit,'description':message}}}})
        with open(_contextFile,'w') as fp:
            json.dump(context,fp,indent=4)

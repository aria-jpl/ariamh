#!/usr/bin/env python3

import numpy as np
import xml.etree.ElementTree as ET
import sarSetup as SS
import xmlSetup as XS
import os


def makeXML(master=None, slave=None, dem=None, dirname='.', raw=True):
    '''Creates a default insarApp.xml file.'''
   
    if master is None:
        raise ValueError('Master catalog is not provided.')
    if slave is None:
        raise ValueError('Slave catalog is not provided.')

    insar = {}
    insar['posting'] = 20
    insar['unwrap'] = True
    insar['filter strength'] = 0.5
    insar['unwrapper name'] = 'snaphu_mcf'
    if raw:
        insar['Sensor name'] = 'COSMO_SKYMED'
    else:
        insar['Sensor name'] = 'COSMO_SKYMED_SLC'
        insar['doppler method'] = 'useDOPCSKSLC'

    insar['master.catalog'] = master
    insar['slave.catalog'] = slave 
    if dem is not None:
        insar['Dem.catalog'] = dem 

    root = ET.Element('insarApp')
    root.append(XS.XMLFromDict(insar, name="insar"))


    XS.indentXML(root)
    fid = open(os.path.join(dirname, 'insarApp.xml'), 'w')
    XS.writeXML(fid, root)
    fid.close()

if __name__ == '__main__':

    f1 = SS.geth5names('../h5/20130531')
    f2 = SS.geth5names('../h5/20130718')

    sxml = SS.sarCatalogXML(f1)
    mxml = SS.sarCatalogXML(f2)

    makeXML(master=mxml, slave=sxml)

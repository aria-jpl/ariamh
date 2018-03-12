import tsinsar as ts
import argparse
import numpy as np

def parse():
    parser= argparse.ArgumentParser(description='Preparation of XML files'+ 
            'for setting up the processing chain. Check tsinsar/tsxml.py' + 
            'for details on the parameters.')
    parser.parse_args()


parse()
g = ts.TSXML('data')
g.prepare_data_xml('example',xlim=[0,$width],ylim=[0,$length],
        rxlim=[$rx0,$rx1],rylim=[$ry0,$ry1],
        latfile='lat.flt',lonfile='lon.flt',hgtfile='',
    mask='../mask.flt',
        inc=$inc,cohth=0.3,chgendian='False',
        masktype='f4',unwfmt='RMG',corfmt='FLT',
        demfmt='FLT',)
g.writexml('data.xml')


g = ts.TSXML('params')
g.prepare_sbas_xml(nvalid=$nvalid,netramp=True,
        atmos='',demerr=False,
        uwcheck=False,regu=True,
        filt=$filt, gpsvert=True,
    gpsramp=False, gpstype='sopac',
    stntype=False, stnlist='../stationlist',
    gpspath='../neu', gpsmodel=False
    )
g.writexml('sbas.xml')


#g = ts.TSXML('params')
#g.prepare_mints_xml(netramp=True,atmos='',demerr=False,uwcheck=False,regu=True,masterdate='19920604')
#g.writexml('mints.xml')


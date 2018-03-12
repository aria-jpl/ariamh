#!/usr/bin/env python3

import argparse
import os
def createPegFile(sensor,project,tracks,dire,latStart,latEnd,lon):
    #need only approximate longitude
    for sen in sensor:
        fp = open('pegfile_' + sen.lower() + '_' + project.lower(),'w')
        fp.write('PegBandIndx\tPathNo\t\tDirection\tLatStart\tLatEnd\tPegLat\tPegLon\tPegHeading\n')
        for di,tr,ls,le,lo in zip(dire,tracks,latStart,latEnd,lon):
            fp.write(sen  + '\t\t' + str(tr) + '\t\t' + di + '\t\t' + str(ls) +'\t\t' + str(le) + '\t' + str((ls+le)/2.) + '\t' + str(lo) + '\t\t0\n')
        fp.close()

def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-t','--tracks',dest='tracks',nargs='*',type=int,help='the tracks number') 
    parser.add_argument('-s','--start',dest='latStart',nargs='*',type=float,help='peg region starting latitude') 
    parser.add_argument('-e','--end',dest='latEnd',nargs='*',type=float,help='peg region ending latitude') 
    parser.add_argument('-l','--longitude',dest='lon',nargs='*',type=float,help='peg region longitude') 
    parser.add_argument('-d','--direction',dest='direction',nargs='*',type=str,help='satellite direction') 
    parser.add_argument('-p','--project',dest='project',type=str,help='Project that belongs too')
    parser.add_argument('-n','--sensor',dest='sensor',nargs='*',type=str,help='Sensor')                                                                                                                                        
                                                                                                                                        
    args = parser.parse_args()
    createPegFile(args.sensor,args.project,args.tracks,args.direction,args.latStart,args.latEnd,args.lon)

if __name__ == '__main__':
    import sys
    sys.exit(main())

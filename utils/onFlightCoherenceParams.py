#!/usr/bin/env python3

import os
import argparse
import json
def createCoherenceFile(args):
    #need only approximate longitude
    #remember that for dsc latStart > latEnd
  
    fp = open('coherenceParams_' + args.project.lower() + '.json','w')
    json.dump({'bCrit':args.bCrit,'doppler':args.doppler,'tau':args.tau,'coherenceThreshold':args.cThrs},fp,indent=4)
    fp.close()

def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-b','--bCrit',dest='bCrit',default=400,type=float,help='Critical perpendicular baseline') 
    parser.add_argument('-d','--doppler',dest='doppler',default=0.4,type=float,help='Doppler') 
    parser.add_argument('-t','--tau',dest='tau',default=180,type=float,help='Critical temporal baseline') 
    parser.add_argument('-p','--project',dest='project',type=str,help='Project that belongs too')
    parser.add_argument('-c','--cThrs',dest='cThrs',default=0.3,type=float,help='Critical temporal baseline') 
                                                                                                                                   
    args = parser.parse_args()
    createCoherenceFile(args)

if __name__ == '__main__':
    import sys
    sys.exit(main())

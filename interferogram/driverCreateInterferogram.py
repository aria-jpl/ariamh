#! /usr/bin/env python 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Author: Giangi Sacco
# Copyright 2012, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import sys
import os
def main(filename):
    
    if not os.path.exists('context.json'):
        command = 'echo {\\"id\\":1} > context.json'
        print command
        os.system(command)
    path_dir = os.path.dirname(os.path.abspath(__file__))
    os.system(os.path.join(path_dir,'createInterferogram.py ') + '--inputFile ' + filename + ' --stage 0')
    #the executable is in tropmap
    error  = os.system('pyAPSCorrect.py --model NARR --pickle insar.cpk')
    #error = 0
    print("Error val = ",error)
    os.system(os.path.join(path_dir,'createInterferogram.py ') +  '--inputFile ' + filename + ' --stage 1 --errorCode ' + str(error))

if __name__ == '__main__':
    sys.exit(main(sys.argv[1]))

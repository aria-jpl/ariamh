#!/usr/bin/env python3
import os
import argparse
cpend = '.cp'
tabs = ' '*8
def fix(filein):
    fp = open(filein)
    fp1 = open(filein + '.out','w')
    allL = fp.readlines()
    for l in allL:
        if l.split():
            nl = l.replace('\t',tabs)
        else:
            nl = l
        fp1.write(nl)
    fp.close()
    fp1.close()
    import os
    os.system('mv ' + filein + ' ' + filein + cpend)
    os.system('mv ' + filein + '.out ' + filein)

def clean(fl):
    os.system('rm -rf ' + fl)
    
def main(topDir,action,endw):
    ls = os.listdir(topDir)
    for fl in ls:
        if(os.path.isdir(fl)):
            cwd = os.getcwd()
            os.chdir(fl)
            main('./',action,endw)
            os.chdir(cwd)
        else:
            if(fl.endswith(endw) and not fl.count('fixTabs')):
                action(fl)
if __name__ == '__main__':
    import sys
    parser = argparse.ArgumentParser(description='Retab py scripts')
    parser.add_argument('-a','--action', dest='action', type=str, required=True,
            help='What to do: "retab" files or "clean" backup files or "single" file')
    parser.add_argument('-d','--dir', dest='dir', type=str, required=False,default='./',
            help='Directory where to start recursion.')
    parser.add_argument('-f','--file', dest='file', type=str, required=False,default='',
            help='Filename when running on single file')
    
    parser = parser.parse_args()
    if(parser.action == 'retab'):
        endw = '.py'
        action = fix
    elif(parser.action == 'clean'):
        endw = cpend
        action = clean
    elif(parser.action == 'single'):
        fix(parser.file)
        sys.exit(0)
    else:
        print('Unrecognized action',parser.action)
    sys.exit(main(parser.dir,action,endw))

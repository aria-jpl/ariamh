#!/usr/bin/env python
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Author: Piyush Agram
# Copyright 2013, by the California Institute of Technology. ALL RIGHTS RESERVED.
# United States Government Sponsorship acknowledged.
# Any commercial use must be negotiated with the Office of Technology Transfer at
# the California Institute of Technology.
# This software may be subject to U.S. export control laws.
# By accepting this software, the user agrees to comply with all applicable U.S.
# export laws and regulations. User has the responsibility to obtain export licenses,
# or other export authority as may be required before exporting such information to
# foreign countries or providing access to foreign persons.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


import argparse
import symtable
import math
import numpy as np
from numpy.lib.stride_tricks import as_strided
import logging
import os
import sys


helpStr = """

ISCE Band image with imageMath.py 

Examples:
*********

    1) imageMath.py -e='a*exp(-1.0*J*arg(b))' -o test.int -t cfloat  --a=resampOnlyImage.int --b=topophase.mph
       This uses phase from topophase.mph to correct topophase from the interferograms

    2) imageMath.py -e='a_0;a_1' --a=resampOnlyImage.amp -o test.amp -s BIL
       This converts a BIP image to a BIL image

    3) imageMath.py -e="abs(a);sqrt(b_0**2 + b_1**2)" --a=topophase.flat --b="topophase.mph;3419;float;2;BIP" -o test.mag -s BIL
        This should produce a BIL (RMG) image where both channels are equal. Input the correct width before testing this.

Rules:
******

    0) Input math expressions should be valid python expressions.

    1) A math expression for every band of output image is needed. For a multi-band output image, these expressions are separated by a ;. 
       Example: See Example 2  above.

    2) All variable names in the math expressions need to be lower case, single character.  Capital characters and multi-char names are reserved for constants and functions respectively.

    3) The band of multi-band input images are represented by adding  _i  to the variable name, where "i" is the band number. All indices are zero-based (C and python). 
       Example : a_0 represents the first band of the image represented by variable "a".

    4) For a single band image, the _0 band notation is optional. 
       Example: a_0 and a are equivalent for a single band image.

    5) For every lower case variable in the equations, another input "--varname" is needed. Example shown above where --a and --b are defined.

    6) Variables  can be defined in two ways:
        a) File name (assuming an ISCE .xml file also exists). 
           Example --a=resamp.int

        b) Image grammar:  "Filename;width;datatype;bands;scheme"
           Example --a="resamp.int;3200;cfloat;1;BSQ"

                -  Default value for datatype=float
                -  Default value for bands = 1
                -  Default value for scheme = BSQ

        c) In the image grammar: Single character codes for datatypes are case sensitive (Numpy convention) whereas multi-character codes are case-insensitive. Internally, everything is translated to numpy convention by the code before processing.
"""


#######Current list of supported unitary functions - f(x)
fnDict = { 'cos':       np.cos,
           'sin':       np.sin,
           'exp':       np.exp,
           'log':       np.log,
           'log2':      np.log2,
           'log10':     np.log10,
           'tan' :      np.tan,
           'asin':      np.arcsin,
           'acos':      np.arccos,
           'atan':      np.arctan,
           'arg':       np.angle,
           'conj':      np.conj,
           'abs' :      np.abs,
           'round' :    np.round,
           'ceil' :     np.ceil,
           'floor' :    np.floor,
           'real'  :    np.real,
           'imag' :     np.imag,
           'rad':       np.radians,
           'deg':       np.degrees,
           'sqrt':      np.sqrt
         }

#######Current list of constants
constDict = { "PI"  : np.pi,
              "J"   : np.complex(0.0, 1.0),
              "I"   : np.complex(0.0, 1.0),
              "E"   : np.exp(1.0),
              "NAN" : np.nan
            }


#####Dictionary of global parameters
iMath = {
          'outFile' : None,     ####Output file name
          'outBands' : [],      ####List of out band mmaps
          'outScheme' : 'BSQ',  ####Output scheme
          'equations' : [],     #####List of math equations
          'outType' : 'f',      ####Output datatype
          'width'   : None,     ####Width of images
          'length'  : None,     ####Length of images
          'inBands' : {},       ####Dictionary of input band mmaps
          'inFiles' : {}        ####Dictionary input file mmaps
        }



######To deal with data types
'''
    Translation between user inputs and numpy types.

    Single char codes are case sensitive (Numpy convention).

    Multiple char codes are case insensitive.
'''

####Signed byte
byte_tuple = ('b', 'byte', 'b8', 'b1')

####Unsigned byte
ubyte_tuple = ('B', 'ubyte', 'ub8', 'ub1')

####Short int
short_tuple = ('h', 'i2', 'short', 'int2', 'int16')

####Unsigned short int
ushort_tuple = ('H', 'ui2', 'ushort', 'uint2', 'uint16')

####Integer
int_tuple = ('i', 'i4', 'i32', 'int', 'int32','intc')

####Unsigned int 
uint_tuple = ('I', 'ui4', 'ui32', 'uint', 'uint32', 'uintc')

####Long int
long_tuple = ('l', 'l8', 'l64', 'long', 'long64', 'longc',
            'intpy', 'pyint', 'int64')

####Unsigned long int 
ulong_tuple = ('L', 'ul8', 'ul64', 'ulong', 'ulong64', 'ulongc',
            'uintpy', 'pyuint', 'uint64')

######Float 
float_tuple =('f', 'float', 'single', 'float32', 'real4', 'r4')

######Complex float 
cfloat_tuple = ('F', 'c8','complex','complex64','cfloat')

#####Double
double_tuple = ('d', 'double', 'real8', 'r8', 'float64',
        'floatpy', 'pyfloat')

######Complex Double
cdouble_tuple=('D', 'c16', 'complex128', 'cdouble')

####Mapping to numpy data type
typeDict = {}

for dtuple in (byte_tuple, ubyte_tuple,
              short_tuple, short_tuple,
              int_tuple, uint_tuple,
              long_tuple, ulong_tuple,
              float_tuple, cfloat_tuple,
              double_tuple, cdouble_tuple):

    for dtype in dtuple:
        typeDict[dtype] = dtuple[0]


def NUMPY_type(instr):
    '''
    Translates a given string into a numpy data type string.
    '''

    tstr = instr.strip()

    if len(tstr) == 1:
        key = tstr
    else:
        key = tstr.lower()
   
    try:
        npType = typeDict[key]
    except:
        raise ValueError('Unknown data type provided : %s '%(instr))

    return npType


isceTypeDict = { 
                    "f" : "FLOAT",
                    "F" : "CFLOAT",
                    "d" : "DOUBLE",
                    "h" : "SHORT",
                    "i" : "INT",
                    "l" : "LONG",
               }


def printNUMPYMap():
    import json
    return json.dumps(typeDict, indent=4, sort_keys=True)

#########Classes and utils to deal with strings ###############
def isNumeric(s):
    '''
    Determine if a string is a number.
    '''
    try:
        i = float(s)
        return True
    except ValueError, TypeError:
        return False

class NumericStringParser(object):
    '''
    Parse the input expression using Python's inbuilt parser.
    '''
    def __init__(self, num_string):
        '''
        Create a parser object with input string.
        '''
        self.string = num_string
        self._restricted = fnDict.keys() + constDict.keys()

    def parse(self):
        '''
        Parse the input expression to get list of identifiers.
        '''

        try:
            symTable = symtable.symtable(self.string, 'string', 'eval')
        except:
            raise IOError('Not a valid python math expression \n' + 
                    self.string)

        idents = symTable.get_identifiers()

        known = []
        unknown = []
        for ident in idents:
            if ident not in self._restricted:
                unknown.append(ident)
            else:
                known.append(ident)


        for val in unknown:
            band = val.split('_')[0]
            if len(band)!=1:
                raise IOError('Multi character variables in input expressions represent functions or constants. Unknown function or constant : %s'%(val))

            elif (band.lower() != band):
                raise IOError('Single character upper case letters are used for constant. No available constant named %s'%(val))

        return unknown, known

def uniqueList(seq):
    '''
    Returns a list with unique elements in a list.
    '''
    seen = set()
    seen_add = seen.add
    return [ x for x in seq if x not in seen and not seen_add(x)]

def bandsToFiles(bandList, logger):
    '''
    Take a list of band names and convert it to file names.
    '''
    flist = []
    for band in bandList:
        names = band.split('_')
        if len(names) > 2:
            logger.error('Invalid band name: %s'%band)
        
        if names[0] not in flist:
            flist.append(names[0])

    logger.debug('Number of input files : %d'%len(flist))
    logger.debug('Input files: ' + str(flist))
    return flist


#######Create the logger for the application
def createLogger(debug):
    '''
    Creates an appopriate logger.
    '''
#    logging.basicConfig()
    logger = logging.getLogger('imageMath')
    consoleHandler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name) s - %(levelname)s\n%(message)s')
    consoleHandler.setFormatter(formatter)
    if args.debug:
        logger.setLevel(logging.DEBUG)
        consoleHandler.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
        consoleHandler.setLevel(logging.INFO)

    logger.addHandler(consoleHandler)

    return logger


##########Classes and utils for memory maps
class memmap(object):
    '''Create the memap object.'''
    def __init__(self,fname, mode='readonly', nchannels=1, nxx=None, nyy=None, scheme='BSQ', dataType='f'):
        '''Init function.'''

        fsize = np.zeros(1, dtype=dataType).itemsize

        if nxx is None:
            raise ValueError('Undefined file width for : %s'%(fname))

        if mode=='write':
            if nyy is None:
                raise ValueError('Undefined file length for opening file: %s in write mode.'%(fname))
        else:
            try:
                nbytes = os.path.getsize(fname)
            except:
                raise ValueError('Non-existent file : %s'%(fname))

            if nyy is None:
                nyy = nbytes/(fsize*nchannels*nxx)

                if (nxx*nyy*fsize*nchannels) != nbytes:
                    raise ValueError('File size mismatch for %s. Fractional number of lines'(fname))
            elif (nxx*nyy*fsize*nchannels) > nbytes:
                    raise ValueError('File size mismatch for %s. Number of bytes expected: %d'%(nbytes))
             

        self.name = fname
        self.width = nxx
        self.length = nyy

        ####List of memmap objects
        acc = []

        ####Create the memmap for the full file
        nshape = nchannels*nyy*nxx
        omap = np.memmap(fname, dtype=dataType, mode=mode, 
                shape = (nshape,))

        if scheme.upper() == 'BIL':
            nstrides = (nchannels*nxx*fsize, fsize)

            for band in xrange(nchannels):
                ###Starting offset
                noffset = band*nxx

                ###Temporary view
                tmap = omap[noffset:]

                ####Trick it into creating a 2D array
                fmap = as_strided(tmap, shape=(nyy,nxx), strides=nstrides)

                ###Add to list of objects
                acc.append(fmap)

        elif scheme.upper() == 'BSQ':
            nstrides = (fsize, fsize)

            for band in xrange(nchannels):
                ###Starting offset
                noffset = band*nxx*nyy

                ###Temporary view
                tmap = omap[noffset:noffset+nxx*nyy]

                ####Reshape into 2D array
                fmap = as_strided(tmap, shape=(nyy,nxx))

                ###Add to lits of objects
                acc.append(fmap)

        elif scheme.upper() == 'BIP':
            nstrides = (nchannels*nxx*fsize,nchannels*fsize)

            for band in xrange(nchannels):
                ####Starting offset
                noffset = band

                ####Temporary view
                tmap = omap[noffset:]

                ####Trick it into interpreting ot as a 2D array
                fmap = as_strided(tmap, shape=(nyy,nxx), strides=nstrides)

                ####Add to the list of objects
                acc.append(fmap)

        else:
            raise ValueError('Unknown file scheme: %s for file %s'%(scheme,fname))

        ######Assigning list of objects to self.bands
        self.bands = acc


def mmapFromISCE(fname, logger):
    '''
    Create a file mmap object using information in an ISCE XML.
    '''
    try:
        import isce
        import iscesys
        from iscesys.Parsers.FileParserFactory import createFileParser
    except:
        raise ImportError('ISCE has not been installed or is not importable')

    if not fname.endswith('.xml'):
        dataName = fname
        metaName = fname + '.xml'
    else:
        metaName = fname
        dataName = os.path.splitext(fname)[0]

    parser = createFileParser('xml')
    prop, fac, misc = parser.parse(metaName)

    logger.debug('Creating readonly ISCE mmap with \n' +
            'file = %s \n'%(dataName) + 
            'bands = %d \n'%(prop['number_bands']) + 
            'width = %d \n'%(prop['width']) + 
            'length = %d \n'%(prop['length'])+
            'scheme = %s \n'%(prop['scheme']) +
            'dtype = %s \n'%(prop['data_type']))

    mObj = memmap(dataName, nchannels=prop['number_bands'],
            nxx=prop['width'], nyy=prop['length'], scheme=prop['scheme'],
            dataType=NUMPY_type(prop['data_type']))

    return mObj

def mmapFromStr(fstr, logger):
    '''
    Create a file mmap object using information provided on command line.

    Grammar = 'filename;width;datatype;bands;scheme'
    '''
    def grammarError():
        raise SyntaxError("Undefined image : %s \n" +
                "Grammar='filename;width;datatype;bands;scheme'"%(fstr))

    parms = fstr.split(';')
    logger.debug('Input string: ' + str(parms))
    if len(parms) < 2:
        grammarError()

    try:
        fname = parms[0]
        width = int(parms[1])
        if len(parms)>2:
            datatype = NUMPY_type(parms[2])
        else:
            datatype='f'

        if len(parms)>3:
            bands = int(parms[3])
        else:
            bands = 1

        if len(parms)>4:
            scheme = parms[4].upper()
        else:
            scheme = 'BSQ'

        if scheme not in ['BIL', 'BIP', 'BSQ']:
            raise IOError('Invalid file interleaving scheme: %s'%scheme)
    except:
        grammarError()

    logger.debug('Creating readonly mmap from string with \n' +
            'file = %s \n'%(fname) + 
            'bands = %d \n'%(bands) + 
            'width = %d \n'%(width) + 
            'scheme = %s \n'%(scheme) +
            'dtype = %s \n'%(datatype))


    mObj = memmap(fname, nchannels=bands, nxx=width,
            scheme=scheme, dataType=datatype)

    return mObj

    pass

#######ISCE XML rendering
def renderISCEXML(fname, bands, nyy, nxx, datatype, scheme, descr):
    '''
    Renders an ISCE XML with the right information.
    '''
    
    try:
        import isce
        import isceobj
    except:
        raise ImportError('ISCE has not been installed or is not importable.')

    
    img = isceobj.createImage()
    img.filename = fname
    img.scheme = scheme
    img.width=nxx
    img.length = nyy
    try:
        img.dataType = isceTypeDict[datatype]
    except:
        try:
            img.dataType = isceTypeDict[NUMPY_type(datatype)]
        except:
            raise Exception('Processing complete but ISCE XML not written as the data type is currently not supported by ISCE Image Api')

    img.addDescription(descr)
    img.bands = bands
    img.setAccessMode('read')
    img.createImage()
    img.renderHdr()
    img.finalizeImage()


#######Command line parsing
def detailedHelp():
    '''
    Return the detailed help message.
    '''
    msg = helpStr + '\n\n'+ \
              'Available Functions \n' + \
              '********************\n' + \
              str(fnDict.keys()) + '\n\n' + \
              'Available Constants \n' + \
              '********************\n' + \
              str(constDict.keys()) + '\n\n' + \
              'Available DataTypes -> numpy code mapping  \n' + \
              '*****************************************  \n'+ \
              printNUMPYMap() + '\n'

    return msg

class customArgparseFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter):
    pass

class customArgparseAction(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        '''
        The action to be performed.
        '''
        print detailedHelp()
        parser.print_help()
        parser.exit()


def firstPassCommandLine():
    '''
    Take a first parse at command line parsing.
    Read only the basic required fields
    '''

    #####Create the generic parser to get equation and output format first
    parser = argparse.ArgumentParser(description='ISCE Band math calculator.',
            formatter_class=customArgparseFormatter)

#    help_parser = subparser.add_
    parser.add_argument('-H','--hh', nargs=0, action=customArgparseAction,
            help='Display detailed help information.')
    parser.add_argument('-e','--eval', type=str, required=True, action='store',
            help='Expression to evaluate.', dest='equation')
    parser.add_argument('-o','--out', type=str, default=None, action='store',
            help='Name of the output file', dest='out')
    parser.add_argument('-s','--scheme',type=str, default='BSQ', action='store', 
            help='Output file format.', dest='scheme')
    parser.add_argument('-t','--type', type=str, default='float', action='store',
            help='Output data type.', dest='dtype')
    parser.add_argument('-d','--debug', action='store_true', default=False,
            help='Print debugging statements', dest='debug')
    parser.add_argument('-n','--noxml', action='store_true', default=False,
            help='Do not create an ISCE XML file for the output.', dest='noxml')

    #######Parse equation and output format first
    args, files = parser.parse_known_args()


    #####Check the output scheme for errors
    if args.scheme.upper() not in ['BSQ', 'BIL', 'BIP']:
        raise IOError('Unknown output scheme: %s'%(args.scheme))
    iMath['outScheme'] = args.scheme.upper()

    npType = NUMPY_type(args.dtype)
    iMath['outType'] = npType

    return args, files

class customArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise Exception(message)

def parseInputFile(varname, args):
    '''
    Get the input string corresponding to given variable name.
    '''

    inarg = varname.strip()
    ####Keyname corresponds to specific 
    key = '--' + inarg

    if len(varname.strip()) > 1:
        raise IOError('Input variable names should be single characters.\n' +
                'Invalid variable name: %s'%varname)

    if (inarg != inarg.lower()):
        raise IOError('Input variable names should be lower case. \n' +
                'Invalud variable name: %s'%varname)

    #####Create a simple parser
    parser = customArgumentParser(description='Parser for band math.',
            add_help=False)
    parser.add_argument(key, type=str, required=True, action='store',
            help='Input string for a particular variable.', dest='instr')

    try:
        infile, rest = parser.parse_known_args(args)
    except:
        raise SyntaxError('Input file : "%s" not defined on command line'%varname)
    return infile.instr, rest


#######The main driver that puts everything together
if __name__ == '__main__':

    args, files = firstPassCommandLine()

    #######Set up logger appropriately
    logger = createLogger(args.debug)
    logger.debug('Known: '+ str(args))
    logger.debug('Optional: '+ str(files))


    #######Determine number of input and output bands
    bandList = []
    for ii,expr in enumerate(args.equation.split(';')):

        #####Now parse the equation to get the file names used
        nsp = NumericStringParser(expr)
        logger.debug('Input Expression: %d : %s'%(ii, expr))
        bands, known = nsp.parse()
        logger.debug('Unknown variables: ' + str(bands))
        logger.debug('Known variables: ' + str(known))
       
        iMath['equations'].append(expr)
        bandList = bandList + bands 

    bandList = uniqueList(bandList)
    
    numOutBands = len(iMath['equations'])
    logger.debug('Number of output bands = %d'%(numOutBands))
    logger.debug('Number of input bands used = %d'%(len(bandList)))
    logger.debug('Input bands used = ' + str(bandList))


    #####Determine unique images from the bandList
    fileList = bandsToFiles(bandList, logger)


    ######Create input memmaps
    for ii,infile in enumerate(fileList):
        fstr, files = parseInputFile(infile, files)
        logger.debug('Input string for File %d: %s: %s'%(ii, infile, fstr))

        if len(fstr.split(';')) > 1:
            fmap = mmapFromStr(fstr, logger)
        else:
            fmap = mmapFromISCE(fstr, logger)

        iMath['inFiles'][infile] = fmap

        if len(fmap.bands) == 1:
            iMath['inBands'][infile] = fmap.bands[0]

        for ii in xrange(len(fmap.bands)):
            iMath['inBands']['%s_%d'%(infile, ii)] = fmap.bands[ii]

    if len(files):
        raise IOError('Unused input variables set:\n'+ ' '.join(files))

    #######Some debugging
    logger.debug('List of available bands: ' + str(iMath['inBands'].keys()))
    
    ####If used in calculator mode.
    if len(bandList) == 0:
        dataDict=dict(fnDict.items() + constDict.items())
        logger.info('Calculator mode. No output files created')
        for ii, equation in enumerate(iMath['equations']):
            res=eval(expr, dataDict)
            logger.info('Output Band %d : %f '%(ii, res))

        sys.exit(0)
    else:
        if args.out is None:
            raise IOError('Output file has not been defined.')

    #####Check if all bands in bandList have been accounted for
    for band in bandList:
        if band not in iMath['inBands'].keys():
            raise ValueError('Undefined band : %s '%(band))

    ######Check if all the widths match
    widths = [img.width for var,img in iMath['inFiles'].iteritems() ]
    if len(widths) != widths.count(widths[0]):
        logger.debug('Widths of images: ' + 
                str([(var, img.name, img.width) for var,img in iMath['inFiles'].iteritems()]))
        raise IOError('Input images are not of same width')

    iMath['width'] = widths[0]
    logger.debug('Output Width =  %d'%(iMath['width']))

    #######Check if all the lengths match
    lengths=[img.length for var,img in iMath['inFiles'].iteritems()]
    if len(lengths) != lengths.count(lengths[0]):
        logger.debug('Lengths of images: ' +
             str([(var, img.name, img.length) for var,img in iMath['inFiles'].iteritems()]))

        raise IOError('Input images are not of the same length')

    iMath['length'] = lengths[0]
    logger.debug('Output Length = %d'%(iMath['length']))

    #####Now create the output file
    outmap = memmap(args.out, mode='write', nchannels=numOutBands,
            nxx=iMath['width'], nyy=iMath['length'], scheme=iMath['outScheme'],
            dataType=iMath['outType'])

    logger.debug('Creating output ISCE mmap with \n' +
            'file = %s \n'%(args.out) + 
            'bands = %d \n'%(numOutBands) + 
            'width = %d \n'%(iMath['width']) + 
            'length = %d \n'%(iMath['length'])+
            'scheme = %s \n'%(iMath['outScheme']) +
            'dtype = %s \n'%(iMath['outType']))

    iMath['outBands'] = outmap.bands

    #####Start evaluating the expressions

    ####Set up the name space to use
    dataDict=dict(fnDict.items() + constDict.items())
    bands = iMath['inBands']
    outBands = iMath['outBands']

    #####Replace ^ by ** 
    for lineno in xrange(iMath['length']):

        ####Load one line from each of the the bands
        for band in bandList:  #iMath['inBands'].iteritems():
            dataDict[band] = bands[band][lineno,:]

        ####For each output band
        for kk,expr in enumerate(iMath['equations']):
            res = eval(expr, dataDict)
            outBands[kk][lineno,:] = res

    
    ######Render ISCE XML if needed
    if not args.noxml:
        renderISCEXML(args.out, numOutBands, iMath['length'], iMath['width'],
                iMath['outType'], iMath['outScheme'], ' '.join(sys.argv))

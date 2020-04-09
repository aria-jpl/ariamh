#!/usr/bin/env python
###This script lists baselines and dopplers for a set of hdf5 files

from __future__ import print_function
from __future__ import division
from builtins import range
from builtins import object
from past.utils import old_div
import argparse
import isce
import numpy as np
import h5py
import os
import itertools
from isceobj.Sensor import createSensor
from isceobj.Orbit.Orbit import Orbit, StateVector
from iscesys.StdOEL.StdOELPy import create_writer
import stdproc
import datetime
import sys

import matplotlib.pyplot as plt
import matplotlib.dates as mdates 
import matplotlib


stdWriter = create_writer("log", "", True, filename="prepareStack.log")


class orbit_info(object):
    def __init__(self, sar, fname ,fd):
        '''Initialize with a sarProc object and corresponding XML file name'''
        self.planet = sar.frame.getInstrument().getPlatform().planet
        self.orbit = sar.frame.getOrbit()
        self.dt = sar.frame.sensingMid
        self.prf = sar.frame.getInstrument().getPulseRepetitionFrequency()
        self.fd = fd
        self.rds = None
        self.rng = sar.frame.startingRange
        self.clook = None
        self.slook = None
        self.peg = None
        self.hgt = None
        self.filename = fname

        stateVec = orbit_info.getStateVector(self.orbit, self.dt)
        self.pos = stateVec.getPosition()
        self.vel = stateVec.getVelocity()
        self.getPeg()
        self.computeLookAngle()

    @staticmethod
    def getStateVector(orbit, time):
        return orbit.interpolateOrbit(time, method='hermite')

    def getPeg(self):

        shortOrb = Orbit()
        for i in range(-10,10):
            time = self.dt + datetime.timedelta(seconds=(old_div(i,self.prf)))
            sv = self.orbit.interpolateOrbit(time, method='hermite')
            shortOrb.addStateVector(sv)

        objPeg = stdproc.createGetpeg()
        objPeg.wireInputPort(name='planet', object=self.planet)
        objPeg.wireInputPort(name='Orbit', object=shortOrb)

        stdWriter.setFileTag("getpeg", "log")
        stdWriter.setFileTag("getpeg", "err")
        stdWriter.setFileTag("getpeg", "out")
        objPeg.setStdWriter(stdWriter)
        objPeg.estimatePeg()

        self.peg = objPeg.getPeg()
        self.hgt = objPeg.getAverageHeight()
        self.rds = self.peg.getRadiusOfCurvature()


    def computeLookAngle(self):
        self.clook = old_div((2*self.hgt*self.rds+self.hgt**2+self.rng**2),(2*self.rng*(self.rds+self.hgt)))
        self.slook = np.sqrt(1-self.clook**2)
#        print('Estimated Look Angle: %3.2f degrees'%(np.arccos(self.clook)*180.0/np.pi))

    def getBaseline(self, slave):
        '''Compute baseline between current object and another orbit object.'''
        
        mpos = np.array(self.pos)
        mvel = np.array(self.vel)

        #######From the ROI-PAC scripts
        rvec = old_div(mpos,np.linalg.norm(mpos))
        crp = old_div(np.cross(rvec, mvel),np.linalg.norm(mvel))
        crp = old_div(crp,np.linalg.norm(crp))
        vvec = np.cross(crp, rvec)
        mvel = np.linalg.norm(mvel)

        spos = np.array(slave.pos)
        svel = np.array(slave.vel)
        svel = np.linalg.norm(svel)

        dx = spos - mpos;
        z_offset = old_div(slave.prf*np.dot(dx, vvec),mvel)

        slave_dt = slave.dt - datetime.timedelta(seconds=old_div(z_offset,slave.prf))
        try:
            svector = orbit_info.getStateVector(slave.orbit, slave_dt)
        except:
            raise ValueError('Slave image probably not geolocated with master image. Orbit time out of bounds.')

        spos = np.array(svector.getPosition())
        svel = np.array(svector.getVelocity())
        svel = np.linalg.norm(svel)

        dx = spos-mpos
        hb = np.dot(dx, crp)
        vb = np.dot(dx, rvec)

        csb = -1.0*hb*self.clook + vb*self.slook

#        print('Estimated Baseline: %4.2f'%csb)
        return csb

def CSKmetadata(self):
    '''Populates the metadata section of the CSKobj.'''

    try:
        fp = h5py.File(self.hdf5, 'r')
    except h5py.h5e.H5Error as strerror:
        print("Error: %s \n" % strerror)
        return

    fd = fp.attrs['Centroid vs Range Time Polynomial']

    self.populateMetadata(file=fp)
    fp.close()
    return fd


def parse():
    '''
    Parse the command line to get the list of values.
    '''
    def Range(nmin, nmax):
        class RangeObj(argparse.Action):
            def __call__(self, parser, args, values, option_string=None):
                if not nmin <= values <= nmax:
                    msg = 'Argument "{f}" requires value between {nmin} and {nmax}'.format(f=self.dest, nmin=nmin, nmax=nmax)
                    raise argparse.ArgumentTypeError(msg)
                setattr(args, self.dest, values)

        return RangeObj

    #####Actual parser set up
    parser = argparse.ArgumentParser(description='Computes the baseline plot for given set of SAR images.')
    parser.add_argument('fnames', nargs='+', default=None, help = 'H5 files corresponding to the SAR scenes.')
    parser.add_argument('-plot', dest='plot', default=False, help = 'Include Baseline Plots.',action='store_true')
    parser.add_argument('-Bcrit', dest='Bcrit', default=600.0, help='Critical Geometric Baseline in meters [0., 10000.]', type=float, action=Range(0., 10000.))
    parser.add_argument('-Tau', dest='Tau', default=200.0, help='Temporal Decorrelation Time Constant in days [0., 3650.]', type=float, action=Range(0., 3650.))
    parser.add_argument('-dop', dest='dop', default=0.5, help='Critical Doppler difference in fraction of PRF', type=float, action=Range(0., 1.))
    parser.add_argument('-coh', dest='cThresh', default=0.3, help='Coherence Threshold to estimate viable interferograms. [0., 1.0]', type=float, action=Range(0., 1.))
    parser.add_argument('-raw', dest='raw', default=False, action='store_true',
            help='Set for raw data.')
    inps = parser.parse_args()

    return inps

def process(inps):
    '''
    Do the actual processing.
    '''
    nSar = len(inps.fnames)
    print(inps.fnames)
    print('Number of SAR Scenes = %d'%nSar)

    Orbits = []
    print('Reading in all the raw files and metadata.')
    for k in range(nSar):
        if inps.raw:
            sar = createSensor('COSMO_SKYMED')
        else:
            sar = createSensor('COSMO_SKYMED_SLC')
        sar.hdf5= inps.fnames[k]
        fd = CSKmetadata(sar)
        Orbits.append(orbit_info(sar, inps.fnames[k],fd[0]))

    ##########We now have all the pegpoints to start processing.
    Dopplers = np.zeros(nSar)
    Bperp    = np.zeros(nSar)
    Days     = np.zeros(nSar)

    #######Setting the first scene as temporary reference.
    master = Orbits[0]


    Dopplers[0] = master.fd
    Days[0] = master.dt.toordinal()
    for k in range(1,nSar):
        slave = Orbits[k]
        Bperp[k] = master.getBaseline(slave)
        Dopplers[k] = slave.fd
        Days[k]  = slave.dt.toordinal() 


    print("************************************")
    print("Index    Date       Bperp  Doppler")
    print("************************************")
    
    ### Plot
    if inps.plot:
            f=open("baseline.txt",'w')
            g=open("bplot.txt",'w')
            f.write("Index     Date       Bperp   Doppler \n")
    
    
    for k in range(nSar):
        print('{0:>3}    {1:>10} {2:4.2f}  {3:4.2f}'.format(k+1, Orbits[k].dt.strftime('%Y-%m-%d'), Bperp[k],Dopplers[k]))
	
        ### Plot
        if inps.plot:
                f.write('{0:>3}    {1:>10}    {2:4.2f}     {3:4.2f} \n'.format(k+1, Orbits[k].dt.strftime('%Y-%m-%d'), Bperp[k],Dopplers[k]))
                g.write('{0:>10}    {1:4.2f} \n'.format(Orbits[k].dt.strftime('%Y-%m-%d'), Bperp[k]))
	
        #### Looking at all possible pairs. Stop here if you just want to add
    ### 1 scene. If the first scene is the new scene, you have all reqd
    ### information at this stage.

    print("************************************")
    
    ### Plot
    if inps.plot:
        f.close()
        g.close()
        os.system('mkdir baselineInfos')
        os.system('mv baseline.txt bplot.txt baselineInfos')
    	
	
    geomRho = (1-np.clip(old_div(np.abs(Bperp[:,None]-Bperp[None,:]),inps.Bcrit), 0., 1.))
    tempRho = np.exp(old_div(-1.0*np.abs(Days[:,None]-Days[None,:]),inps.Tau))
    dopRho  = (old_div(np.abs(Dopplers[:,None] - Dopplers[None,:]), master.prf)) < inps.dop

    Rho = geomRho * tempRho * dopRho
    for kk in range(nSar):
        Rho[kk,kk] = 0.

    
    avgRho = old_div(np.mean(Rho, axis=1)*nSar,(nSar-1))
    numViable = np.sum((Rho> inps.cThresh), axis=1)

    ####Currently sorting on average coherence.

    masterChoice = np.argsort(-avgRho) #Descending order
    masterOrbit = Orbits[masterChoice[0]]
    masterBperp = Bperp[masterChoice[0]]


    print('*************************************')
    print('Ranking for Master Scene Selection: ')
    print('**************************************')
    print('Rank  Index      Date    nViable   Avg. Coh.' )
    for kk in range(nSar):
        ind = masterChoice[kk]
        print('%03d   %03d   %10s  %03d  %02.3f'%(kk+1, ind+1, Orbits[ind].dt.strftime('%Y-%m-%d'), numViable[ind], avgRho[ind]))

    print('***************************************')

    print('***************************************')
    print('List of Viable interferograms:')
    print('***************************************')

    [ii,jj] = np.where(Rho > inps.cThresh)

    pairList = []
    print('Master     Slave      Bperp      Deltat')
    if inps.plot:
        os.system('rm baselineInfos/InSAR_pairs.txt')
        f=open("baselineInfos/InSAR_pairs.txt",'w')
        f.write('Master     Slave      Bperp(m)      Deltat(days)     Doppler(Hz) \n')
        f.close()
        os.system('rm baselineInfos/InSAR_plot.txt')
        g=open("baselineInfos/InSAR_plot.txt",'w')
        g.close()

    for mind, sind in zip(ii,jj):
        master = Orbits[mind]
        slave = Orbits[sind]
	
        #Plot
        giorni = []
        BaseList = []
	
        if master.dt > slave.dt:
            print('{0:>10} {1:>10}  {2:>4.2f}   {3:>4.2f}'.format(master.dt.strftime('%Y-%m-%d'), slave.dt.strftime('%Y-%m-%d'), Bperp[mind]-Bperp[sind], Days[mind] - Days[sind]))
            pairList.append([master.dt.strftime('%Y%m%d'), slave.dt.strftime('%Y%m%d'), Bperp[mind] - Bperp[sind]])
	
        if inps.plot:
                #f=open("InSAR_plot.txt",'w')
                if master.dt > slave.dt:
                        f=open("baselineInfos/InSAR_pairs.txt",'a')
                        f.write('{0:>10} {1:>10}  {2:>4.2f}        {3:>4.2f}            {4:>4.2f} \n'.format(master.dt.strftime('%Y-%m-%d'), slave.dt.strftime('%Y-%m-%d'), Bperp[mind]-Bperp[sind], Days[mind] - Days[sind], Dopplers[mind] - Dopplers[sind]))
                        f.close()
                        g=open("baselineInfos/InSAR_plot.txt",'a')
                        g.write('{0:>10} {1:>10}  {2:>4.2f}   {3:>4.2f}       {4:>4.2f}   {5:>4.2f} \n'.format(master.dt.strftime('%Y-%m-%d'), slave.dt.strftime('%Y-%m-%d'), Bperp[mind], Bperp[sind], Dopplers[mind], Dopplers[sind]))
                        plt.plot_date([Days[mind], Days[sind]], [Bperp[mind], Bperp[sind]], 'r-', lw=1, xdate=True, ydate=False)
		
                                #f=open("InSAR_plot.txt",'a')
                                #f.write('{2:>4.2f} {2:>4.2f}     {3:>4.2f} {3:>4.2f} \n'.format(Bperp[mind], Bperp[sind], Days[mind], Days[sind]))
                                #f.close()
                #print(Bperp[mind], Days[mind], Bperp[sind] , Days[sind])
				
	    
	    
	   

    print('***************************************')

    #######Currently picks master peg point.
    print('***************************************')
    commonPeg = masterOrbit.peg
    print('Common peg point:                      ')
    print(commonPeg)
    print('Bperp Range:  [%f , %f] '%(Bperp.min()-masterBperp, Bperp.max()-masterBperp))

    ######Choose median doppler
    commonDop = np.median(Dopplers)
    maxDop   = np.max(Dopplers)
    minDop = np.min(Dopplers)
    varDop = old_div(np.max(np.abs(Dopplers-commonDop)),masterOrbit.prf)

    print('Common Doppler: ', commonDop)
    print('Doppler Range:  [%f, %f]'%(minDop, maxDop))
    print('MAx Doppler Variation = %f %%'%(varDop*100))
    print('******************************************')
    
    ### Plot
    if inps.plot:
        days, bperp = np.loadtxt("baselineInfos/bplot.txt", unpack=True, converters={ 0: mdates.strpdate2num('%Y-%m-%d')})
        plt.plot_date(x=days, y=bperp, xdate=True, ydate=False)
        date_span = 0.2*np.abs(days.max()-days.min())
        bperp_span = 0.2*np.abs(bperp.max()-bperp.min())
        plt.grid(True)
        plt.ylabel("Perpendicular Baseline (meters)")
        plt.xlabel("Time")
        plt.xlim([days.min()-date_span, days.max()+date_span])
        plt.ylim([bperp.min()-bperp_span, bperp.max()+bperp_span])
        plt.axes().set_aspect('auto','datalim')
        plt.savefig('baselineInfos/baseline.png')
        plt.show()
    
	
    return pairList

  
if __name__ == '__main__':
    inps = parse()
    process(inps)

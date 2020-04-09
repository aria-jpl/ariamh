from __future__ import division
from builtins import range
from builtins import object
from past.utils import old_div
import datetime 
from isceobj.Orbit.Orbit import Orbit, StateVector
from isceobj.Planet.Planet import Planet
import stdproc
from iscesys.StdOEL.StdOELPy import create_writer
import numpy


class OrbitInfo(object):
    '''Class for storing metadata about a SAR scene.'''
    def __init__(self, fm):
        '''Initialize with a FrameMetadata object'''
        self._lookDict = {'right': -1,
        'left' : 1}
        self.direction = fm.direction
        self.fd = fm.doppler
        self.tStart = fm.sensingStart
        self.tStop = fm.sensingStop
        self.lookSide = self._lookDict[fm.lookDirection]
        self.prf = fm.prf
        self.rng = fm.startingRange

        self.coherenceThreshold = 0.2
        self.orbVec = None 
        self.tMid = self.tStart +old_div(sum([self.tStop-self.tStart,
                    datetime.timedelta()],datetime.timedelta()),2)
        self.pos = None
        self.vel = None
        self.peg  = None
        self.rds = None
        self.hgt = None
        self.clook = None
        self.slook = None
        self.baseline = {'horz' : fm.horizontalBaseline,
                    'vert' : fm.verticalBaseline,
                    'total' : 0}
        self.coherence = None
        self.planet = Planet(pname='Earth')
        self.unpackOrbitVectors(fm.orbit)
        #self.computePeg()
        #self.computeLookAngle()

    
    def getBaseline(self):
        return self.baseline
    
    def getCoherence(self):
        return self.coherence

    def unpackOrbitVectors(self, orb):
        self.orbVec = Orbit(source='json', quality='good')
        self.orbVec._referenceFrame = 'WGS-84'
        relTims = orb[0]
        satPos = orb[1]
        satVel = orb[2]
        refTime = orb[3]

        for kk in range(len(satPos)):
            vecTime = refTime + datetime.timedelta(seconds = relTims[kk]) 
            tempVec = StateVector(time=vecTime,
                    position=satPos[kk],
                    velocity=satVel[kk])
            self.orbVec.addStateVector(tempVec)

        stateVec = self.orbVec.interpolateOrbit(self.tMid, 'hermite')
        self.pos = stateVec.getPosition()
        self.vel = stateVec.getVelocity()
        return


    def computeLookAngle(self):
        self.clook = old_div((2*self.hgt*self.rds+self.hgt**2+self.rng**2),(2*self.rng*(self.rds+self.hgt)))
        self.slook = numpy.sqrt(1-self.clook**2)
#        print('Estimated Look Angle: %3.2f degrees'%(np.arccos(self.clook)*180.0/np.pi))
        return

    def computePeg(self):

        shortOrb = Orbit()
        for i in range(-10,10):
            time = self.tMid + datetime.timedelta(seconds=(old_div(i,self.prf)))
            sv = self.orbVec.interpolateOrbit(time, method='hermite')
            shortOrb.addStateVector(sv)

        objPeg = stdproc.createGetpeg()
        objPeg.wireInputPort(name='planet', object=self.planet)
        objPeg.wireInputPort(name='Orbit', object=shortOrb)

        stdWriter = create_writer("log", "", True, filename="orbitInfo.log")
        stdWriter.setFileTag("getpeg", "log")
        stdWriter.setFileTag("getpeg", "err")
        stdWriter.setFileTag("getpeg", "log")

        objPeg.setStdWriter(stdWriter)
        objPeg.estimatePeg()
        
        self.peg = objPeg.getPeg()
        self.rds = objPeg.getPegRadiusOfCurvature()
        self.hgt = objPeg.getAverageHeight()
        return

    def computeBaseline(self, slave):
        '''
        Compute baseline between current object and another orbit object.
        This is meant to be used during data ingest.
        '''

        mpos = numpy.array(self.pos)
        mvel = numpy.array(self.vel)

        #######From the ROI-PAC scripts
        rvec = old_div(mpos,numpy.linalg.norm(mpos))
        crp = old_div(numpy.cross(rvec, mvel),numpy.linalg.norm(mvel))
        crp = old_div(crp,numpy.linalg.norm(crp))
        vvec = numpy.cross(crp, rvec)
        mvel = numpy.linalg.norm(mvel)

        spos = numpy.array(slave.pos)
        svel = numpy.array(slave.vel)
        svel = numpy.linalg.norm(svel)

        dx = spos - mpos;
        z_offset = old_div(slave.prf*numpy.dot(dx, vvec),mvel)

        slave_time = slave.tMid - datetime.timedelta(seconds=old_div(z_offset,slave.prf))

        ####Remove these checks to deal with scenes from same track but not exactly overlaid
#        if slave_time < slave.tStart:
#            raise Exception('Out of bounds. Try the previous frame in time.')
#        elif slave.tStop < slave_time:
#            raise Exception('Out of bounds. Try the next frame in time.')

        try:
            svector = slave.orbVec.interpolateOrbit(slave_time, method='hermite')
        except:
            raise Exception('Error in interpolating orbits. Possibly using non geo-located images.')

        spos = numpy.array(svector.getPosition())
        svel = numpy.array(svector.getVelocity())
        svel = numpy.linalg.norm(svel)

        dx = spos-mpos
        hb = numpy.dot(dx, crp)
        vb = numpy.dot(dx, rvec)

        csb = self.lookSide*hb*self.clook + vb*self.slook

        self.baseline = {'horz' : hb,
                    'vert' : vb,
                    'total' : csb}

   
    def computeCoherence(self, slave, Bcrit=400., Tau=180.0, Doppler=0.4):
        '''
        This is meant to be estimate the coherence. This is for estimating which pairs to process.

        I assume that baseline dict is already available in the json input. baseline dict is w.r.t master and slave baseline is already precomputed w.r.t master.

        Typically: process a pair if Rho > 0.3
        '''
        Bperp = numpy.abs(self.lookSide*(self.baseline['horz'] - slave.horizontalBaseline)*self.clook + (self.baseline['vert'] - slave.verticalBaseline)*self.slook)
        Btemp = numpy.abs(self.tStart.toordinal() - slave.sensingStart.toordinal()) * 1.0
        Bdop = numpy.abs(old_div((self.fd * self.prf - slave.doppler * slave.prf), self.prf))

        geomRho = (1-numpy.clip(old_div(Bperp,Bcrit), 0., 1.))
        tempRho = numpy.exp(old_div(-1.0*Btemp,Tau))
        dopRho  = Bdop < Doppler

        self.coherence = geomRho * tempRho * dopRho
    

    def computeCoherenceNoRef(self, slave, Bcrit=400., Tau=180.0, Doppler=0.4):
        '''
            This is meant to be estimate the coherence. This is for estimating which pairs to process.  Ignores baseline values in json and computes baseline between given pair. Master is not involved.

             Typically: process a pair if Rho > 0.3
        '''
        
        self.computeBaseline(OrbitInfo(slave))
        Bperp = numpy.abs(self.lookSide*self.baseline['horz']*self.clook + self.baseline['vert'] *self.slook)
        Btemp = numpy.abs(self.tStart.toordinal() - slave.sensingStart.toordinal()) * 1.0
        Bdop = numpy.abs(old_div((self.fd * self.prf - slave.doppler * slave.prf), self.prf))

        
        print(('Bperp: %f (m) , Btemp: %f days, Bdop:  %f (frac PRF)'%
                (Bperp,Btemp,Bdop))) 
        geomRho = (1-numpy.clip(old_div(Bperp,Bcrit), 0., 1.))
        tempRho = numpy.exp(old_div(-1.0*Btemp,Tau))
        dopRho  = Bdop < Doppler
        self.coherence = geomRho * tempRho * dopRho
        print(('Expected Coherence: %f'%(self.coherence)))

   
    def isCoherent(self,slave,Bcrit=400., Tau=180, Doppler = 0.4,threshold=0.3):
#### Change this line to self.computeCoherence to go back to original.
        self.computeCoherenceNoRef(slave,Bcrit,Tau,Doppler)

        ret = False
        if(self.coherence >= threshold):
            ret = True
        return ret

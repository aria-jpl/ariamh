from builtins import range
import numpy as np 
import os
import isceobj
import datetime
import logging
import types

logger = logging.getLogger('isce.topsinsar.topo')
      
def runTopo(self):
    from zerodop.topozero import createTopozero
    from isceobj.Planet.Planet import Planet

    swathList = self._insar.getValidSwathList(self.swaths)

    ####Catalog for logging
    catalog = isceobj.Catalog.createCatalog(self._insar.procDoc.name)

    ####Load in DEM
    demfilename = self.verifyDEM()
    catalog.addItem('Dem Used', demfilename, 'topo')

    boxes = []
    for swath in swathList:
        #####Load the master product
        master = self._insar.loadProduct( os.path.join(self._insar.masterSlcProduct,  'IW{0}.xml'.format(swath)))


        numCommon  = self._insar.numberOfCommonBursts[swath-1]
        startIndex = self._insar.commonBurstStartMasterIndex[swath-1]

        if numCommon > 0:
            catalog.addItem('Number of common bursts IW-{0}'.format(swath), self._insar.numberOfCommonBursts[swath-1], 'topo')

    
            ###Check if geometry directory already exists.
            dirname = os.path.join(self._insar.geometryDirname, 'IW{0}'.format(swath))

            if os.path.isdir(dirname):
                logger.info('Geometry directory {0} already exists.'.format(dirname))
            else:
                os.makedirs(dirname)


            ###For each burst
            for index in range(numCommon):
                ind = index + startIndex
                burst = master.bursts[ind]

                latname = os.path.join(dirname, 'lat_%02d.rdr'%(ind+1))
                lonname = os.path.join(dirname, 'lon_%02d.rdr'%(ind+1))
                hgtname = os.path.join(dirname, 'hgt_%02d.rdr'%(ind+1))
                losname = os.path.join(dirname, 'los_%02d.rdr'%(ind+1))
                incangname = os.path.join(dirname, 'incang_%02d.rdr'%(ind+1))

                demImage = isceobj.createDemImage()
                demImage.load(demfilename + '.xml')

                #####Run Topo
                planet = Planet(pname='Earth')
                topo = createTopozero()
                topo.slantRangePixelSpacing = burst.rangePixelSize
                topo.prf = 1.0/burst.azimuthTimeInterval
                topo.radarWavelength = burst.radarWavelength
                topo.orbit = burst.orbit
                topo.width = burst.numberOfSamples
                topo.length = burst.numberOfLines
                topo.wireInputPort(name='dem', object=demImage)
                topo.wireInputPort(name='planet', object=planet)
                topo.numberRangeLooks = 1
                topo.numberAzimuthLooks = 1
                topo.lookSide = -1
                topo.sensingStart = burst.sensingStart
                topo.rangeFirstSample = burst.startingRange
                topo.demInterpolationMethod='BIQUINTIC'
                topo.latFilename = latname
                topo.lonFilename = lonname
                topo.heightFilename = hgtname
                topo.losFilename = losname
                topo.incFilename = incangname
                topo.topo()

                bbox = [topo.minimumLatitude, topo.maximumLatitude, topo.minimumLongitude, topo.maximumLongitude]
                boxes.append(bbox)

                catalog.addItem('Number of lines for burst {0} - IW-{1}'.format(index,swath), burst.numberOfLines, 'topo')
                catalog.addItem('Number of pixels for bursts {0} - IW-{1}'.format(index,swath), burst.numberOfSamples, 'topo')
                catalog.addItem('Bounding box for burst {0} - IW-{1}'.format(index,swath), bbox, 'topo')

        else:
            print('Skipping Processing for Swath {0}'.format(swath))

        topo = None

    boxes = np.array(boxes)
    bbox = [np.min(boxes[:,0]), np.max(boxes[:,1]), np.min(boxes[:,2]), np.max(boxes[:,3])]
    catalog.addItem('Overall bounding box', bbox, 'topo')


    catalog.printToLog(logger, "runTopo")
    self._insar.procDoc.addAllFromCatalog(catalog)

    return
def createTopo(self):
    return types.MethodType(runTopo, self )  
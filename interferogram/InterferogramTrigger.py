#! /usr/bin/env python3 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Author: Giangi Sacco
# Copyright 2012, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import sys
from interferogram.Interferogram import Interferogram as InterferogramBase
from utils.contextUtils import toContext
from interferogram.createPrepareInterferogram import createPrepareInterferogram

class Interferogram(InterferogramBase):


    def __init__(self):
        super(Interferogram,self).__init__()

    



    def run(self,ops):
        filename = ops.inputFile
        self._productList.append(filename); 
        #try:
        
        process = 'InterferogramTrigger'
        try: 
            
            listMeta = self.createMetadata(filename)
            self._sensor = listMeta[0][0].spacecraftName
            #db start
            #self._sensor = 'CSKS4'
            #db end
            self._prepareInterferogram = createPrepareInterferogram(self._sensor)
            self._inputFile = self.createInputFile(listMeta)
            # hack to make isce believe this is the command line
            self._insar = self._insarClass(cmdline=self._inputFile)
            self._insar._insar.unwrappedIntFilename =  self._insar._insar.topophaseFlatFilename.replace('.flat','.unw')
            #these tow statements need to be here before configure in order to be set
            self._insar._insar.geocode_bbox = self.createGeoBBox(listMeta)
            self._insar._insar.geocode_list = self.createGeocodeList(self._insar._insar)
            self._insar._configure()
            self._insar.run()
            #here dump insar object
            # delete it and reload from file
            self.createPngList(self._insar._insar)
            self.createBrowseImages()
            self.createProductList()
            self.createProductJson(listMeta)
        except Exception as e:
            print(e)
            message = 'InterferogramTrigger.py: run failed  with exception ' + str(e)
            exit = 1
            toContext(process,exit,message)
            raise Exception
        exit = 0 
        message = 'InterferogramTrigger: completed'
        toContext(process,exit,message)

        return 0
    
   

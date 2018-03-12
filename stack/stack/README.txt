This directory contains scripts to launch stack processing on calimap / CSK datasets.

Preferred Directory structure:

Parent
|   
|-- dem       (Directory to store DEMs)
|-- h5        (Directory with all the HDF5 data organized by dates)
|   | 
|   `-- 20130531  (Directory with contiguous frames for a given data)
|       |-- CSKS1_RAW_B_HI_09_HH_RA_SF_20130531135659_20130531135705.h5
|       |-- CSKS1_RAW_B_HI_09_HH_RA_SF_20130531135703_20130531135710.h5
|       `-- CSKS1_RAW_B_HI_09_HH_RA_SF_20130531135708_20130531135715.h5
|
|-- insar     (Directory to store processed interferograms)
`-- GIAnT     (Directory for GIAnT runs)


Step 0 : Understand the scripts in the calimap directory. All of them have extensive argparse headers. "script_name.py -h" should print out help information.

Step 1:   Organize HDF5 data.
Organize all the HDF5 data as shown above

Step 2:   Create or Set up the DEM. 

        2a) If you have a DEM with a corresponding XML in ISCE format, just copy it or link it into the DEM directory.

        2b) If you want to automatically generate an SRTM dem:
        make_dem.py -i ./h5/20130531 -o ./dem
         This will figure out the bounding boxes for the frames for data on a given date and download required dem in the dem directory.

Step 3:  Set up dirs for viable interferograms
  
        stackSetup.py -i ./h5 -o./insar -d ./dem/demfile.dem 

        This prints a list of viable interferograms to screen and automatically creates directories in the insar directories for interferogram generation. Use other options to print only list of viable pairs or to include custom pairs.

Step 4:  Process the interferograms

        runInsarApp.py -d ./insar --end=uwrap

        This essentially lets you run insarApp.py in sequence on various pairs. Check out other options to force reprocessing or custom process some pairs.


Step 5:  Determine common bounding box for geocoding.

        editBbox.py -d ./insar

        If a file named bbox.snwe exists that is automatically used. If not the bounding boxes of all interferograms are analyzed and a file is created for you. The pickle files are modified with this common value. Check out other options.


Step 6:  Geocoding data.

        runInsarApp.py -d ./insar --start=geocode -f   

        This geocodes all the interferograms on a common grid. The "-f" option  requirement is a remnant and will soon be modified. Check other options.


Step 7:  Preparing data for ingestion into GIAnT.

        prepGIAnT.py -i ./insar -o ./GIAnT

        Currently a minimum set of inputs are read from the stacks and prepared for GIAnT analysis like in the Calimap project. To use your own templates - create a copy of the templates directory in the stackScripts directory and modify scripts accordingly.



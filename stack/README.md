Requirements:

1) ISCE python2 version
2) GIAnT
3) GMT


Step 1  (This would be obtained from the drill down):
-----------------------------------------------------

Get metadata of the interferograms you are interested in.

> getMetadata.py --track 118 --frame 350 --beam 21 --pass dsc 

Creates metadata.json with all information about inteferograms

Step 2 (This can be done by the ARIA system):
---------------------------------------------

Fetch and stage these interferograms in a local directory

> stageInterferograms.py --meta metadata.json

Creates ./insar directory with various interferograms

Note that the directory names used for staging the data are not the same as product names.

Step 3:
-------

Run QA to see what interferograms might be of interest.

> runQA.py --dir ./insar

Creates valid.list with list of viable interferograms.

Step 4:
-------

Get auxdata and GPS data

> getAuxData.py --dir ./insar

Creates land water mask and reference pixel file. Chooses best GPS station to use as reference for time-series generation.

Check the ref.in file and one of the interferograms to make sure reference pixel location is reasonable. Automatic estimation of reference pixel with GPS data is not a perfect process.


Inputs to this script can be modified to just generate the Land Water Mask and not do anything with GPS. 

Step 5:
-------

Prepare GIAnT run

> prepGIAnT_cali.py -i ./insar -o ./GIAnT --ilist valid.list --ref ref.in


Step 6:
-------

Run GIAnT analysis in the GIAnT sub-directory as usual.

1) python prepxml.py
2) PrepIgramStack.py
3) ProcessStack.py
#4) NSBASInvert.py
5) TimeFnInvert.py



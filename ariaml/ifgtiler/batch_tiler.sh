dryrun=0

platform=csk
ifgcachedir=../ifg_image_cache/$platform
tiledir=../tiles_${platform}
if [ $platform == 'csk' ]; then
    tiledim=192
    numtiles=25
elif [ $platform == 's1a' ]; then
    tiledim=256
    numtiles=50
fi
tilecmd="ifgtiler.py -v -m 0.4 -n ${numtiles} -d ${tiledim} -c -o ${tiledir}"
if [ $dryrun == 1 ]; then
    tilecmd="echo $tilecmd"
fi
for ifgbase in $(ls $ifgcachedir); do
    ifgpath=${ifgcachedir}/${ifgbase}
    unw=$ifgpath/filt_topophase.unw.geo.browse.png
    coh=$ifgpath/topophase_ph_only.cor.geo.browse.png    
    if [ -f $unw ] & [ -f $coh ]; then
	tilethisifg="$tilecmd $unw $coh"
	echo $tilethisifg
	echo "$($tilethisifg)" >> ${platform}_batch_tiler.log
    else
	echo "***missing files in $ifgpath***"
	echo $ifgpath >> ${platform}_missing_files.txt
    fi
    unset unw coh;
done

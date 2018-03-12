#!/usr/bin/env python
import sys
def createLines(csk,minMaxLon,minMaxLat,nFrames,dire,tr):
    #eps nearly one 10-th of a frame
    eps = (minMaxLat[1] - minMaxLat[0])/(nFrames*10)
    #make top and bot a bit smaller to make sure the boundaries are included
    top = minMaxLat[1] - eps
    bot = minMaxLat[0] + eps
    delta = []
    # 2 and 5 are special cases
    if  (nFrames == 2):
         nZones = 1 # one zone
         delta = [(top - bot)]
    elif (nFrames == 5):
        nZones = 2
        delta = [(top - bot)*3/5.0,(top - bot)*2/5.0]
    else:
        nZones = nFrames/3
        left = nFrames%3
        # 4 frames length
        l4 = (top - bot)/(left/4.0 + 3/4.0*nZones)
        # 3 frames length
        l3 = 3/4.0*l4
        # note tot length  (top - bot) = l4*left + l3*(nZones - left)
        for i in range(nZones):
            if i < left:
                delta.append(l4)
            else:
                delta.append(l3)
    
               
    if(dire == 'asc'):
        fact = +1
        start = bot
    else:
        fact = -1
        start = top
    ret = []
    for i in range(nZones):
        retN = '%s       %3d        %s       %.2f        %.2f       %.2f        %.2f        %d         %.2f       %.2f '\
        %(csk,int(tr),dire,start,start + fact*delta[i],start + fact*delta[i]/2,getLon(top,bot,minMaxLon[1],minMaxLon[0],start + fact*delta[i]/2),nFrames,getLon(top,bot,minMaxLon[1],minMaxLon[0],start),getLon(top,bot,minMaxLon[1],minMaxLon[0],start + fact*delta[i]))         
        ret.append(retN)
        start += fact*delta[i]
    
    return ret
def getLon(nla,sla,nlo,slo,la):
    return (nlo-slo)/(nla-sla)*(la-sla) + slo 
        
def getVal(string):
    return string.strip().replace('<value>','').replace('</value>','')


def readInput(filename,dire):
    
    corners = []
    nFrames = []
    orbit = []
    fp = open(filename)
    allL = fp.readlines()
    i = 0
    corn = []
    while i < len(allL):
        if allL[i].count('NW_Lat'):
            i+=1
            nwla = getVal(allL[i]) 
            i += 3
            nwlo = getVal(allL[i]) 
            i += 3
            nela = getVal(allL[i]) 
            i += 3
            nelo = getVal(allL[i]) 
            i += 3
            sela = getVal(allL[i]) 
            i += 3
            selo = getVal(allL[i]) 
            i += 3
            swla = getVal(allL[i]) 
            i += 3
            swlo = getVal(allL[i]) 
            corners.append([[nwla,nwlo],[nela,nelo],[sela,selo],[swla,swlo]])
        elif allL[i].count('Orbit'):
            i += 1
            orbit.append(getVal(allL[i]))
            
        elif allL[i].count('Frames'):
            i += 1
            nFrames.append(getVal(allL[i]))
        i += 1
    fp.close()
    if(len(orbit) != len(corners) or len(orbit) != len(nFrames)):
        print("Error")
    return corners,nFrames,orbit
    
def main():
    fp = open('peg_file_csk.txt', 'w')
    corners,nFrames,orbit = readInput(sys.argv[1],'asc')
    for i in range(len(corners)):
        lines = createLines(corners[i],int(nFrames[i]),'asc',int(orbit[i])%237,(int(orbit[i])-193)%237)
        for line in lines:
            fp.write(line[0] + '\n')
            fp.write(line[1] + '\n')    
    corners,nFrames,orbit = readInput(sys.argv[2],'dsc')
    for i in range(len(corners)):
        lines = createLines(corners[i],int(nFrames[i]),'dsc',int(orbit[i])%237,(int(orbit[i])-193)%237)
        for line in lines:
            fp.write(line[0] + '\n')
            fp.write(line[1] + '\n')

    fp.close()
if __name__ == '__main__':
    sys.exit(main())

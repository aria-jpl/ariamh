#/usr/bin/env python3
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.widgets import RadioButtons
import json
import pickle as cp
import os
import binascii
import matplotlib.image as mpimg
import sys
class TagCheck:
    def __init__(self,filename):
        urls = json.load(open(filename))
        self._urls = []
        self._image_cache = 'image_cache'
        for i in urls:
            self._urls.append(i[0])
        self._seli = 1
        self._u = b'7075636f7073'
        self._p = b'7075635f307073'
        self._saved_tags = 'saved_tags.pck'
        self._prd_list = ['topophase.cor.geo.browse.png','filt_topophase.unw.geo.browse.png','filt_topophase.unw.geo_20rad.browse.png']
        if os.path.exists(self._saved_tags):
            self._status = cp.load(open(self._saved_tags,'rb'))
        else:
            self._status = [-1]*len(self._urls)
        self._cnt = np.nonzero(np.array(self._status,np.int32) == -1)[0][0]
        self.load_data()
    
    def crop(self,img):
        indxc = np.nonzero(np.sum(img[:,:,0],0) > 0)[0]
        indxr = np.nonzero(np.sum(img[:,:,0],1) > 0)[0]
        return img[indxr[0]:indxr[-1]+1,indxc[0]:indxc[-1]+1,:]
                
    
    def get_real(self,v):
        return binascii.unhexlify(v).decode('utf8')
   
    def load_data(self):
        self._imgs = []
        try:
            os.mkdir('image_cache')
            curl_img = True
        except:
            curl_img = False
        
        for url in self._urls:
            imgs = []
            if curl_img:
                os.mkdir(os.path.join(self._image_cache,url.split('/')[-2]))
            for pr in self._prd_list:
                os.chdir(os.path.join(self._image_cache,url.split('/')[-2]))
                if curl_img:
                    command = 'curl -k -f -u' + self.get_real(self._u) + ':' + self.get_real(self._p) + ' -O ' + os.path.join(url, pr)
                    os.system(command)
                imgs.append(self.crop(mpimg.imread(pr)))
                os.chdir('../../')
            self._imgs.append(imgs)           
                
   
        
def main():
    tc = TagCheck(sys.argv[1])
    fig = plt.figure()
    ax = fig.add_subplot(1,2,1)
    ax1 = fig.add_subplot(1,2,2)
    ax.imshow(tc._imgs[tc._cnt][0])
    ax1.imshow(tc._imgs[tc._cnt][tc._seli])
    fig.suptitle("Fig " + str(tc._cnt + 1) + " of " + str(len(tc._status)))
    def on_click_radio(label):
        tc._status[tc._cnt] = int(label)
        plt.draw()
    
    def on_close(event):
        cp.dump(tc._status,open(tc._saved_tags,'wb'))
    plt.subplots_adjust(left=.15,right=.99,bottom=.01,top=.99,wspace=.01,hspace=.01)
    axcolor = 'lightgoldenrodyellow'
    ax2 = plt.axes([.01,.5,.1,.2], axisbg=axcolor)
    radio1 = RadioButtons(ax2, ('2r','20r'))
    ax3 = plt.axes([.01,.15,.1,.3], axisbg=axcolor)
    radio = RadioButtons(ax3, ('-1','0','1','2','3'))
    def redraw():
        ax.clear()
        ax1.clear()
        ax.imshow(tc._imgs[tc._cnt][0])
        ax1.imshow(tc._imgs[tc._cnt][tc._seli])
        ax.axis('off')
        ax1.axis('off')
        radio.set_active(tc._status[tc._cnt]+1)  
        fig.suptitle("Fig " + str(tc._cnt + 1) + " of " + str(len(tc._status)))
        plt.draw()
    def on_click_radio1(label):
        dv = {'2r':1,'20r':2}
        tc._seli = dv[label]
        redraw()
    def on_click(event):
        if(event.key == 'right'):
            tc._cnt = min(tc._cnt + 1,len(tc._urls) - 1)
        elif(event.key == 'left'):
            tc._cnt = max(tc._cnt - 1,0)
        redraw()
    ax.axis('off')
    ax1.axis('off')
    #ax3.axis('off')
    fig.canvas.mpl_connect('key_press_event', on_click)
    fig.canvas.mpl_connect('close_event', on_close)
    radio.on_clicked(on_click_radio)
    radio1.on_clicked(on_click_radio1)

    plt.show()
if __name__ == '__main__':
    sys.exit(main())


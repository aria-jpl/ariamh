from __future__ import print_function

import sys, glob
import numpy as np
from os.path import dirname
from os.path import join as pathjoin, exists as pathexists, split as pathsplit
import json
import datetime as dtime

from bokeh.models import *
from bokeh.document import Document
from bokeh.plotting import figure, curdoc, vplot, show
from bokeh.layouts import layout, column, row, gridplot, widgetbox
from bokeh.client import push_session

sys.path.append('../ifgretrieve')
from ifgretrieve import ifg_dir, dataset, sensor, platform, \
    system_version, unwpng, unw20png, cohpng, load_ifg_rgba

from ifgretrieve import query_tags, retrieve_ifg
from ariaml_util import *

from aria_tag import add_tag, rm_tag

user_tags   = ['UWE','TNS','TNT','TNR']
tag_scores = [0,1,2,3]
tag_prefix = 'QA'
num_custom = 5

legendurl = "http://ml.jpl.nasa.gov/people/bbue/ifgtagger_legend.html"

use_session=False

figtoolloc='below' #'right' # 'left' # 'above' # 
figtools="pan,wheel_zoom,box_zoom,reset,previewsave"
#figtools="pan,box_zoom,reset"

ncohl = 10
pagew = 1024
colw  = 400
plotw = 325
figw  = plotw
figh  = 2*figw
img   = None
img20 = None

meta_abbrev = [
    ('sensor','Sensor'),
    ('dataset_type','Dataset Type'),
    ('direction','Direction'),
    ('perpendicularBaseline','Perp. Baseline'),
    ('parallelBaseline','Parallel Baseline'),
    ('sensingStart','Sensing Start'),
    ('sensingStop','Sensing Stop'),
    ('imageCorners.minLat', 'Min Latitude'),
    ('imageCorners.minLon', 'Min Longitude'),
]

tabrowheight = 28
tabheight = len(meta_abbrev)*tabrowheight
tabwidth = pagew
tabcols = [
    TableColumn(field='metaid', title='Property'),
    TableColumn(field='metaval', title='Value')
]


ifgtagger_home = pathsplit(__file__)[0]
log_dir = pathjoin(ifgtagger_home,'logs')

png_scalef=0.66 if platform=='CSK' else 0.5 
png_flip = True

def format_tag(userid,tagval,tag_prefix=tag_prefix):
    return '%s_%s-%s'%(tag_prefix,userid,tagval)        

def get_meta(meta_id,ifg_meta):
    meta_val = ifg_meta.get(meta_id,'n/a')
    if 'baseline' in meta_id.lower():
        return '%8.5f'%meta_val
    elif 'imageCorners' in meta_id:
        key0,key00 = meta_id.split('.')
        meta_val = ifg_meta[key0][key00]
        return '%8.5f'%meta_val
    return meta_val

def get_loc(ifg_meta):
    lat = ifg_meta['imageCorners']['minLat']
    lon = ifg_meta['imageCorners']['minLon']
    return lat,lon

def datestr(fmt='%m/%d/%y %H:%M.%S'):
    date = dtime.datetime.now()
    return date.strftime(fmt)

def mdy():
    return datestr(fmt='%m%d%y')

def load_ifg(ifg,ifg_dir=ifg_dir):
    unwrgba,unw20rgba,coh = load_ifg_rgba(pathjoin(ifg_dir,ifg),png_scalef,
                                          flip=png_flip,doplot=False)
    
    unwv   = unwrgba.view(dtype=np.uint32).squeeze() 
    unw20v = unw20rgba.view(dtype=np.uint32).squeeze()
    
    metaf  = pathjoin(ifg_dir,ifg,ifg.replace('-'+system_version,'')+'.met.json')
    with open(metaf) as fid:
        meta = json.load(fid)
    return dict(unwv=unwv,unw20v=unw20v,coh=coh,meta=meta)

class IFGTagger():
    def __init__(self,ifg_urls=[]):
        self.ifg_urls = ifg_urls

        self.fig   = None
        self.img   = None
        self.fig20 = None
        self.img20 = None
        self.tabsource = None

        #self.cohslider_callback = CustomJS.from_py_func(self.cohslider_callback2)
        self.cohslider = Slider(title="Minimum Coherence", start=0.0, end=1.0,
                                value=0.0, step=1.0/ncohl, 
                                callback_policy="mouseup",width=plotw*2)

        self.cohslider.on_change('value',self.cohslider_callback) 
        
        srcpath = dirname(__file__)
        with open(pathjoin(srcpath, "header.html")) as fid:
            self.head_html = fid.read()
        with open(pathjoin(srcpath, "footer.html")) as fid:
            self.foot_html = fid.read()
        
        self.header = Div(text='',width=pagew)
        self.footer = Div(text='',width=pagew)
        
        ifg,ifg_url,ifg_data = self.next_ifg()

        legendlink = "<a href=\"%s\" target=\"_blank\">legend</a>"%legendurl
        menuhdrtxt = "<b>Predefined Tags</b> (%s)<b>:</b>"%legendlink
        tagmenuhdr = Div(text=menuhdrtxt,width=colw)
        tagcustomhdr = Div(text="<b>Custom Tags (up to %d comma-separated tags):</b>"%num_custom,
                           width=colw)
        useridhdr = Div(text="<b>User Id:</b>",width=colw,height=15)
        
        self.feattab = DataTable(source=self.tabsource, columns=tabcols,
                                 width=tabwidth, height=tabheight,
                                 row_headers=False)

        self.tagheader = Div(text="<b>Status:</b>",width=colw,height=15)
        self.tagstate = Paragraph(text="Enter user id to add tags.",
                                  width=colw,height=65)
        self.tagfooter = Div(text="<hr>",width=colw,height=15)
  
        self.tagmenus = []
        for tag in user_tags:
            taglabs = ["%s %d"%(tag,score) for score in tag_scores]
            self.tagmenus.append(RadioGroup(labels=taglabs, width=85))

        tagcustomplace = 'tag1, tag2, ..., tag%d'%num_custom
        self.tagcustom = TextInput(placeholder=tagcustomplace,width=colw)
            
        self.submitbtn = Button(label='Submit Tags')
        self.submitbtn.on_click(self.tagsubmit_callback)

        self.clearbtn = Button(label='Clear Selections')
        self.clearbtn.on_click(self.clearbutton_callback)

        self.skipbtn = Button(label='Skip Current Interferogram')
        self.skipbtn.on_click(self.skipbutton_callback)

        self.logintxt = TextInput(placeholder='userid')
        
        # root layout
        usercol   = column([self.tagheader,self.tagstate,useridhdr,self.logintxt])
        btncol    = column([self.submitbtn,self.clearbtn,self.skipbtn])
        tagmenu   = column([tagmenuhdr,row(self.tagmenus),tagcustomhdr,self.tagcustom])
        inputcol  = column([usercol,tagmenu,btncol])
        ifgcol    = layout([self.fig,self.fig20],[self.cohslider])
        figifg    = column(row([ifgcol,inputcol]),self.feattab)

        self.docroot = column([self.header,figifg,self.footer])

    def update_table(self,ifg_meta):            
        meta_lab = [mabv for mid,mabv in meta_abbrev]
        meta_val = [get_meta(mid,ifg_meta) for mid,mabv in meta_abbrev]
        tabdata = dict(metaid=meta_lab,metaval=meta_val)
        if self.tabsource is None:
            self.tabsource = ColumnDataSource(data=tabdata)
        else:
            self.tabsource.data['metaval'] = meta_val
        
    def update_cohthr(self,thr):
        global img, img20
        ithr = int(thr*10)
        if img is not None:
            img.data_source.data['image']   = [self.unwl[ithr]]
        if img20 is not None:
            img20.data_source.data['image'] = [self.unw20l[ithr]]
        
    def update_images(self,ifg_data):
        global img, img20
        coh    = ifg_data['coh']
        unwv   = ifg_data['unwv']
        unw20v = ifg_data['unw20v']        
        ploth = int(coh.shape[0]*(plotw/float(coh.shape[1])))

        self.plotw = plotw
        self.ploth = ploth

        print('plotw,ploth=%d,%d'%(self.plotw,self.ploth))

        self.dx,self.dy = (figw-plotw)/2,max(0,figh-ploth)/2
        self.dw,self.dh = plotw,ploth 
        imgkw = dict(image=[unwv],x=[self.dx],y=[self.dy],dw=[self.dw],dh=[self.dh])
        img20kw = dict(image=[unw20v],x=[self.dx],y=[self.dy],dw=[self.dw],dh=[self.dh])

        cohldims = [ncohl+1]+list(unwv.shape)
        self.unwl   = np.zeros(cohldims,dtype=unwv.dtype)
        self.unw20l = np.zeros(cohldims,dtype=unw20v.dtype)
        for i in range(ncohl):
            mask = coh>(255*(float(i)/ncohl))
            self.unwl[i,:,:]   = unwv*mask
            self.unw20l[i,:,:] = unw20v*mask
            
        if self.fig is None or self.fig20 is None:
            self.init_figs()

        if img is None: 
            img   = self.fig.image_rgba(**imgkw)
        else:
            img.data_source.data = imgkw
        
        if img20 is None:
            img20 = self.fig20.image_rgba(**img20kw)
        else:
            img20.data_source.data = img20kw

        self.update_cohthr(self.cohslider.value)
        
    def init_figs(self):
        fig = figure(plot_width=figw, plot_height=figh, 
                     x_range=(1,figw), y_range=(1,figh),
                     x_axis_location=None, y_axis_location=None,
                     tools=figtools, toolbar_location=figtoolloc,
                     toolbar_sticky=True)

        fig.min_border_left       = 1
        fig.min_border_right      = 20
        fig.background_fill_color = "#000000"
        fig.background_fill_alpha = 1.0

        fig.outline_line_color    = None
        fig.grid.grid_line_color  = None

        fig.xaxis.major_label_text_color = '#FFFFFF'
        fig.xaxis.major_tick_line_color  = None

        fig.yaxis.major_label_text_color = '#FFFFFF'
        fig.yaxis.major_tick_line_color  = None
        fig.title.text = unwpng.replace('.browse.png','')

        fig20 = figure(plot_height=figh, plot_width=figw,
                       x_range=(1,figw), y_range=(1,figh),
                       x_axis_location=None, y_axis_location=None,
                       tools=figtools, toolbar_location=figtoolloc,
                       toolbar_sticky=True)

        fig20.min_border_left       = 1
        fig20.min_border_right      = 20
        fig20.background_fill_color = "#000000"
        fig20.background_fill_alpha = 1.0

        fig20.outline_line_color    = None
        fig20.grid.grid_line_color  = None

        fig20.xaxis.major_label_text_color = '#FFFFFF'
        fig20.xaxis.major_tick_line_color  = None

        fig20.yaxis.major_label_text_color = '#FFFFFF'
        fig20.yaxis.major_tick_line_color  = None
        fig20.title.text = unw20png.replace('.browse.png','')

        self.fig = fig
        self.fig20 = fig20
        
    def cohslider_callback2(self, source=None, window=None):
        data = source.data
        f = cb_obj.value
        x, y = data['x'], data['y']
        for i in range(len(x)):
            y[i] = window.Math.pow(x[i], f)
        source.trigger('change')

    def next_ifg(self,randomize=True):
        if len(self.ifg_urls)==0:
            ifg_urls = query_tags(user_tags,exclude=True)
            #ifg_urls = np.array(ifg_urls,dtype=str).reshape([-1,1])
            #ifg_urls = list(np.random.permutation(ifg_urls))
            self.ifg_urls = ifg_urls

	if randomize:
	    self.ifg_urls = list(np.random.permutation(self.ifg_urls))
        ifg,ifg_url = retrieve_ifg(self.ifg_urls,ifg_dir,verbose=True)
        # remove this ifg till the next iteration
        self.ifg = ifg
        self.ifg_urls.remove(ifg_url)
        ifg_data = load_ifg(ifg)
        ifg_meta = ifg_data['meta']
        
        self.update_images(ifg_data)
        self.update_table(ifg_meta)

        lat,lon = get_loc(ifg_meta)
        head_ifg = self.ifg.replace('interferogram__','')
        head_html = self.head_html.format(**dict(platform=sensor,
                                                 ifg_id=head_ifg,
                                                 url=ifg_url,
                                                 lat=lat,lon=lon))
        foot_html = self.foot_html

        self.header.text = head_html
        self.footer.text = foot_html
        
        return ifg,ifg_url,ifg_data

    def cohslider_callback(self,attr,old,new):
        self.update_cohthr(self.cohslider.value)

    def clearbutton_callback(self):
        for i,menu in enumerate(self.tagmenus):
            menu.active = None
        self.tagcustom.value = ''
        self.tagstate.text = 'Tags cleared.'

    def skipbutton_callback(self):
        self.tagstate.text = 'Skipped %s'%(self.ifg)
        ifg,ifg_url,ifg_data = self.next_ifg()
        
    def tagsubmit_callback(self):
        statemsg = ''
        userid = self.logintxt.value
        if userid=='':
            statemsg = 'Please enter a valid userid.'
            self.tagstate.text = statemsg
        else:        
            activetags = []
            for i,menu in enumerate(self.tagmenus):
                tagid = user_tags[i]
                tagval = menu.active
                if tagval is not None:
                    tagstr = format_tag(userid,'_'.join([tagid,str(tagval)]))
                    activetags.append(tagstr)
                    if 'bbtest' not in userid:
                        add_tag(self.ifg,tagstr)
                    menu.active = None

            customvals = self.tagcustom.value
            if customvals != '':
                customlist = customvals.split(',')
                if len(customlist)>num_custom:
                    customlist = customlist[:num_custom]
                for customval in customlist:
                    customval = customval.strip()
                    if customval not in ('','tag%d'%(i+1)):
                        tagstr = format_tag(userid,customval)
                        activetags.append(tagstr)
                        if 'bbtest' not in userid:
                            add_tag(self.ifg,tagstr)
                        self.tagcustom.value = ''

            tagmsg = ', '.join(activetags)
            statemsg = '%s tags: [%s]'%(self.ifg,tagmsg)
            self.tagstate.text = statemsg

            tstamp = datestr()
            logf = pathjoin(log_dir,'ifgtagger_log%s.txt'%mdy())
            try:                
                with open(logf,'a') as fid:
                    cohmsg = 'coherence threshold=%3.2f'%self.cohslider.value
                    logmsg = '%s: %s, %s'%(tstamp,statemsg,cohmsg)
                    print(logmsg,file=fid)
            except:
                warn('%s: unable to update log file %s'%(tstamp,logf))
                pass
            
            ifg,ifg_url,ifg_data = self.next_ifg()

doc = curdoc()
doc.title = 'ARIA-ML %s Interferogram Tagger'%sensor

if use_session:
    # open a session to keep our local document in sync with server
    session = push_session(doc)

tagger = IFGTagger()
if use_session:
    session.show(tagger.docroot)    
    session.loop_until_closed() # run forever
else:
    doc.add_root(tagger.docroot)
    doc.validate()



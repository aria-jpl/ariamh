from utils import queryBuilder as qb 
from UrlUtils import UrlUtils as UU
import sys
import os
import json
from lxml.etree import parse
from utils.createImage import createImage

uu = UU()

def get_list(version,sensor):
    meta = {'system_version':version,'dataset':'interferogram','sensor':sensor}
    query = qb.buildQuery(meta,[])
    return qb.postQuery(query,version)

def localize_data(url,files):
    for f in files:
        if uu.download(url,f):
            print("Error in downloading",f)
            raise Exception

def main():
    #ret = json.load(open('sent_list.json'))
    if sys.argv[2].lower() == 'sentinel':
        sensor = 'SAR-C Sentinel1'
        to_app = 'merged'
    elif sys.argv[2].lower() == 'csk':
        sensor = sys.argv[2]
        to_app = ''
    
    ret,status = get_list(sys.argv[1],sensor)

    cor_file = "topophase.cor.geo"
    cor_xml = "topophase.cor.geo.xml"
    flist = [cor_file,cor_xml]
    for i,f in enumerate(flist):
        flist[i] = os.path.join(to_app,f)
    
    mdx_path = "{}/bin/mdx".format(os.environ['ISCE_HOME'])

    for l in ret:
        cwd = os.getcwd()
        ls = l['url'].split('/')
        ndir = ls[-2] + '_' + ls[-1]
        if not os.path.exists(ndir):
            os.mkdir(ndir)
        try: 
            os.chdir(ndir)
            #some failed to download
            if not os.path.exists('topophase_ph_only.cor.geo.browse.png'):
                localize_data(l['url'],flist)
                rt = parse(cor_xml)
                size = eval(rt.xpath('.//component[@name="coordinate1"]/property[@name="size"]/value/text()')[0])
                rhdr = size * 4
                createImage("{} -P {} -s {} -r4 -rhdr {} -cmap cmy -wrap 1.2".format(mdx_path, cor_file,size,rhdr),"topophase_ph_only.cor.geo")  
                os.remove(cor_file)
                os.remove(cor_xml)
        except Exception:
            print(l['url'],'failed')
            pass
        os.chdir(cwd)
          
        '''
        ADD CODE TO PUSH BACK THE PRODUCT
        '''
if __name__ == '__main__':
    #usage python3 create_cor_png.py v1.0 sentinel or python3 create_cor_png.py v0.6 csk
    sys.exit(main())

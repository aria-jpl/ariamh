
# data_sling_asf.py
CONF_FILE = "/home/ops/ariamh/conf/settings.conf"
def getConf():
    uu = {}
    with open(CONF_FILE, 'r') as fp:
        allL = fp.readlines()
        dc = {}
        for line in allL:
            ls = line.split('=')
            if (len(ls) == 2):
                dc[ls[0]] = ls[1]
        fp.close()
        try:
            uu['rest_url'] = dc['GRQ_URL'].strip()
        except:
            uu['rest_url'] = None
            pass
        try:
            uu['dav_url'] = dc['ARIA_DAV_URL'].strip()
        except:
            uu['dav_url'] = None
            pass
        try:
            uu['grq_index_prefix'] = dc['GRQ_INDEX_PREFIX'].strip()
        except:
            pass
        try:
            uu['datasets_cfg'] = dc['DATASETS_CONFIG'].strip()
        except:
            pass
    return uu

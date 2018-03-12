#!/usr/bin/env python3
import json
import os
def getParameters(project):
    #Need to update values depending on runs
    if(project.lower().count('trigger')):
        name = 'coherenceParams_' + project.lower() + '.json'
    else:
        name = os.path.join(os.environ['ARIAMH_HOME'],'conf','coherenceParams_' + project.lower() + '.json')
        if not os.path.exists(name):
            name = os.path.join(os.environ['ARIAMH_HOME'],'conf','coherenceParams_default.json')
    params = json.load(open(name))
    return params['bCrit'],params['tau'],params['doppler'],params['coherenceThreshold']

#!/usr/bin/env python
import os, sys, json, re, shutil

from utils.queryBuilder import postQuery



def prep_inputs(ml_dir, ctx_file, in_file):
    # get context
    with open(ctx_file) as f:
        j = json.load(f)

    # get kwargs
    kwargs = j #mstarch - with containerization, "kwargs" are in context at top level #json.loads(j['rule']['kwargs'])

    # get classmap file and version
    cm_file = os.path.basename(kwargs['classmap_file'].strip())
    match = re.search(r'classmap_(datav.*?)\.json', cm_file)
    if not match:
        raise RuntimeError("Failed to extract classmap version: %s" % cm_file)
    cm_version = match.group(1)

    # get features file and version
    ft_file = os.path.basename(kwargs['feat_file'].strip())
    match = re.search(r'(featv.*?)\.json', ft_file)
    if not match:
        raise RuntimeError("Failed to extract feature version: %s" % ft_file)
    ft_version = match.group(1)

    # set classifier ID
    clf_version = kwargs['clf_version']
    clf_type = kwargs['clf_type']
    username = j['username'] #mstarch - username is a paramemter
    rule_name = j['name'] #mstarch - rule_name is a parameter
    clf_name = "predictor_model-phunw_clfv%s_%s_%s-%s-%s" % (clf_version, cm_version, 
                                                             ft_version, username, rule_name)

    # get urls
    ret, status = postQuery({ 'query': j['query']}) #mstarch - passthrough is now a parameter
    urls = [i['url'] for i in ret]
    
    # create input json
    input = {
        "clf_name": clf_name,
        "clf_type": clf_type,
        "classmap_file": cm_file,
        "feat_file": ft_file,
        "crossvalidate": 0,
        "saveclf": 1,
        "cacheoutput": 0,
        "urls": urls,
    }

    # create product directory and chdir
    os.makedirs(clf_name)
    os.chdir(clf_name) 

    # write input file
    with open(in_file, 'w') as f:
        json.dump(input, f, indent=2)

    # copy classmap and feature files
    shutil.copy(os.path.join(ml_dir, 'classmaps', cm_file), cm_file)
    shutil.copy(os.path.join(ml_dir, 'features', ft_file), ft_file)


if __name__ == "__main__":
    prep_inputs(sys.argv[1], sys.argv[2], sys.argv[3])

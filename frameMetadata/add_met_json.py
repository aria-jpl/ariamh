#!/usr/bin/env python
import os, sys, json
import osaka.main
from os.path import expanduser
home = expanduser("~")

default_account_name="CSK_DEFAULT"

def add_metadata(seed_file, metadata_file):
    """Add metadata to json file."""
   
    with open(metadata_file) as f:
        metadata = json.load(f)

    with open(seed_file) as f:
        seed = json.load(f)
	seed["account_name"]=default_account_name
        metadata.update(seed)
   
    # overwrite metadata json file
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2, sort_keys=True)
    

    # update met file with account_name and machine tags
    update_misc_data(seed_file, metadata_file) 


def add_account_info(in_context_file, in_context_url, metadata_file):

    try:
	print("add_account_name")
	print(in_context_url)

	home = expanduser("~")
	netrc_file = os.path.join(home, ".netrc")
        osaka.main.get(in_context_url, in_context_file)
        if os.path.isfile(in_context_file):
            with open(in_context_file, 'r') as fr:
		seed =json.load(fr)	    
		if "localize_urls" in seed:
	    	    datas =seed["localize_urls"]
            	    for data in datas: 
	            	if "url" in data:
		    	    url_path=data["url"].split('/')
			    print(url_path)
		    	    ftp_name =url_path[2]
		    	    print("ftp_name : %s" %ftp_name)
			    if os.path.isfile(netrc_file):
                            	with open(netrc_file, 'r') as fr:
				    for line in fr:
				    	if ftp_name in  line:
					    account_name =line.split('login')[1].split('password')[0].strip()
					    print(account_name)
		    
			    		    with open(metadata_file, 'r') as f:
					    	metadata = json.load(f)
                                    	    	metadata["account_name"]=account_name
                                	    with open(metadata_file,'w') as f:
                                    		json.dump(metadata, f, indent=2, sort_keys=True)
						print("met file has been updated with account name : %s " %metadata["account_name"])
						return account_name
				    print("Account Name : %s NOT found in netrc file " %ftp_name)
                            else:
                                print(".netrc file not found")
                else:
		    print("localize_url not found in %s " %in_context_url)
	else:
	    print("incoming context file not found : %s "%in_context_file)
    except:
	e = sys.exc_info()[0]
	print(e)
    return default_account_name

def update_misc_data(seed_file, metadata_file):

    try:
        with open(seed_file) as f:
            seed = json.load(f)
	    datas =seed["job_specification"]["params"]
            for data in datas: 
	        if data["destination"]== "localize" and data["name"]=="localize_url":
		    dest_path=data["value"].split('/')
		    file_name =dest_path[-1]
		    print("file_name : %s" %file_name)

		    in_url_path=data["value"].split(file_name)[0]
		    in_met_file="incoming-"+dest_path[-5]+"-"+dest_path[-4]+"-"+dest_path[-3]+"-"+os.path.splitext(file_name)[0]+".met.json"		    
		    in_met_url=os.path.join(in_url_path, in_met_file)
    		    in_context_file="incoming-"+dest_path[-5]+"-"+dest_path[-4]+"-"+dest_path[-3]+"-"+os.path.splitext(file_name)[0]+".context.json"
   		    in_context_url=os.path.join(in_url_path, in_context_file)
   		    add_machine_tags(in_met_file, in_met_url, metadata_file)
		    account_name = add_account_info(in_context_file, in_context_url, metadata_file)
		    add_machine_tags(in_met_file, in_met_url, metadata_file, account_name)
    except:
        e = sys.exc_info()[0]
        print(e)


def add_machine_tags(in_met_file, in_met_url, metadata_file, account_name=default_account_name):
	
    try:
        print("add_machine_tags")
        print(in_met_url)
	account_name=unicode(account_name, 'utf_8')
        tags = [account_name]
 	osaka.main.get(in_met_url, in_met_file)
	if os.path.isfile(in_met_file):
	    with open(in_met_file, 'r') as fr:
        	in_meta = json.load(fr)
		if "tags" in in_meta:
		    tags.extend(in_meta['tags'])
		else:
		    print("tags not found in : %s" %in_met_url)
	else:
	    print("Incoming met file not found : " %in_met_file)
	
	with open(metadata_file, 'r') as f:
	    metadata = json.load(f)
	    metadata["tags"]=tags
        with open(metadata_file,'w') as f:
	    json.dump(metadata, f, indent=2, sort_keys=True)
	    print("Updated Metadata file machine tags with %s" %metadata["tags"])
    except:
	e = sys.exc_info()[0]
	print(e)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("%s <seed metadata JSON file> <metadata JSON file>" % sys.argv[0])
        sys.exit(1)

    add_metadata(sys.argv[1], sys.argv[2])
    sys.exit(0)

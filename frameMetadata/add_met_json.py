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
        metadata.update(seed)
   
    # overwrite metadata json file
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2, sort_keys=True)
    

    # update met file with account_name and machine tags
    update_misc_data(seed_file, metadata_file) 



def clean_up(file_name):
    try:
	if os.path.isfile(file_name):
	    os.remove(file_name)
	    print("Deleted : %s" %file_name)
    except:
        e = sys.exc_info()[0]
        print(e)

def update_misc_data(seed_file, metadata_file):

    try:
	account_name=None
	product_path=[]
	machine_tags=[]
	tags=[]
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
                    print(in_met_url)
		    osaka.main.get(in_met_url, in_met_file)
        	    if os.path.isfile(in_met_file):
            		with open(in_met_file, 'r') as fin, open(metadata_file, 'r') as fout:
                	    seed =json.load(fin)
			    metadata = json.load(fout)
                            if "account_name" in seed:
                                account_name=seed["account_name"]
                                metadata["account_name"]=account_name
                                #account_name=unicode(account_name, 'utf_8')
                                tags = [account_name]
                	    if "urls" in seed:
                    		urls =seed["urls"]
                    		url_path=urls.split('/')
                    		print(url_path)
                    		ftp_name =url_path[2]
                    		product_path=url_path[3:]
                    		print("get_account_path_info : ftp_name : %s" %ftp_name)
                    		print("get_account_path_info : product_path : %s" %product_path)
				tags.extend(x for x in product_path if x not in tags)

			    if "machine_tags" in seed:
				machine_tags=seed["machine_tags"]
				tags.extend(x for x in machine_tags if x not in tags)

			    if len(tags)>0:
				print("tags : %s" %tags)
				metadata["tags"]=tags
                        with open(metadata_file,'w') as f:
            		    json.dump(metadata, f, indent=2, sort_keys=True)
	

		        clean_up(in_met_file)
			return
                    else:
			print("Incoming Met file NOT found : %s" %in_met_file)
    except:
        e = sys.exc_info()
        print(e)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("%s <seed metadata JSON file> <metadata JSON file>" % sys.argv[0])
        sys.exit(1)

    add_metadata(sys.argv[1], sys.argv[2])
    sys.exit(0)

import requests
es_index='grq_aria'

def _add_tag(ifg_id,ifg_tag,es_index):
    login_url = 'https://{{ GRQ_PVT_IP }}/login'
    tag_url = 'https://{{ GRQ_PVT_IP }}/user_tags/add'
    login_data = dict(username='username', password='password')
    tag_data = dict(id=ifg_id,tag=ifg_tag,es_index=es_index)
    headers = dict(Referer=login_url)
    client = requests.session()
    response = client.post(login_url, data=login_data, headers=headers)
    print(response.text)
    print("\n\n")
    response = client.post(tag_url, data=tag_data, headers=headers)
    print(response.text)

def _add_tag_noauth(ifg_id,ifg_tag,es_index=es_index):
    tag_url = 'https://{{ GRQ_PVT_IP }}/user_tags/add_no_auth'
    tag_data = { 'id': ifg_id, 'es_index': es_index, 'tag': ifg_tag }
    print('request: %s data %s'%(tag_url,tag_data))
    response = requests.post(tag_url, data=tag_data)
    #print(response.text)

def _rm_tag_noauth(ifg_id,ifg_tag,es_index=es_index):
    tag_url = 'https://{{ GRQ_PVT_IP }}/user_tags/remove_no_auth'
    tag_data = { 'id': ifg_id, 'es_index': es_index, 'tag': ifg_tag }
    print('request: %s data %s'%(tag_url,tag_data))
    response = requests.post(tag_url, data=tag_data)
    #print(response.text)    

add_tag = _add_tag_noauth
rm_tag = _rm_tag_noauth

if __name__ == '__main__':
    
   
    ifg_id='S1-IFG_STCM2S3_TN035_20161120T020610-20161208T020749_s3-resorb-v1.0'
    ifg_tag='bbtest'
    add_tag(ifg_id,ifg_tag,es_index)
    rm_tag(ifg_id,ifg_tag,es_index)

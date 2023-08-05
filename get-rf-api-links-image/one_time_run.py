if __name__ == "__main__":
    from redfin_scraper import RedfinScraper
    import numpy as np
    from config import BUCKET,SECRET,KEY,REDFIN_URL,REDFIN_ZIP_URL,REDFIN_API_CLASS_DEF,REDFIN_API_CLASS_ID
    import requests
    import concurrent.futures
    import json
    from bs4 import BeautifulSoup
    import time
    import boto3
    import sys
    
    def threaded_request(func,urls):
        responses=[]

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_url = {executor.submit(func,url):url for url in urls}
            
            for future in concurrent.futures.as_completed(future_to_url):
                url=future_to_url[future]
                responses.append((url,future.result()))

                    
        return responses
    
    def randomized_UA():
        num_var=np.random.randint(100,1000)
        num_var3=np.random.randint(10,100)
        num_var2=num_var3%10
        num_var4=np.random.randint(1000,10000)
        num_var5=np.random.randint(100,1000)

        user_agent={"User-Agent": f"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/{num_var5}.36 (KHTML, like Gecko) "+
                f"Chrome/51.{num_var2}.2704.{num_var} Safari/537.{num_var3} OPR/38.0.{num_var4}.41"}
        
        return user_agent
    
    
    def get_req(url):
        res=requests.get(url,headers=randomized_UA())
        return res
    
    def generate_urls(zip_list):
        urls=[]
        for zip in zip_list:
            urls.append(REDFIN_URL.format(REDFIN_ZIP_URL.format(zip_code=zip)))
            
        return urls
    
    
    if len(sys.argv)>0:
        choice=sys.argv[1]
        if choice not in ("1",'2','3','4','5'):
            raise Exception("Invalid CMD. A digit of 1, 2, 3, 4, or 5 is required.")
    else:
        raise Exception("CMD required.")
        
    
    scraper = RedfinScraper()

    scraper.setup("./zip_code_database.csv",multiprocessing=False)

    filtered=scraper.zip_database[(scraper.zip_database['decommissioned']==0)&
                                (scraper.zip_database['type']=='STANDARD')&
                                (scraper.zip_database['irs_estimated_population']>0)]
    
    df1,df2,df3=np.array_split(filtered,3)
    
    df1_1,df1_2=np.array_split(df1,2)
    df2_1,df2_2=np.array_split(df2,2)

    
    zip_arrays={"1":df1_1,"2":df1_2,"3":df1_2,"4":df2_2,"5":df3}
    
    zip_codes=list(zip_arrays[choice]['zip'])
    
    urls=generate_urls(zip_codes)
    
    split_urls=[]
    sub_list=[]

    for i in range(len(urls)):
        if (i!= 0 and i%300==0) or (i+1==len(urls)):
            split_urls.append(sub_list)
            sub_list=[]
        sub_list.append(urls[i])
    
    output_data={"data":[]}
    
    for g_urls in split_urls:
        responses=threaded_request(get_req,g_urls)
        for url,res in responses:
            if res.status_code==429:
                raise Exception(f"ERROR: {res.status_code} for {url}")
            elif res.status_code!=200:
                print(f"WARNING: {res.status_code} for {url}")
            else:
                try:
                    soup=BeautifulSoup(res.text,'html.parser')
                    link=soup.find(REDFIN_API_CLASS_DEF[0],REDFIN_API_CLASS_DEF[1])[REDFIN_API_CLASS_ID]
                    output_data["data"].append({"url":url,"api_link":REDFIN_URL.format(link)})
                except:
                    print(f"WARNING: API Link not available for {url}")
        time.sleep(180)
            
    json_string=json.dumps(output_data)
    
    s3=boto3.client('s3',
                      aws_access_key_id=KEY,
                      aws_secret_access_key=SECRET)
    
    
    s3.put_object(Body=json_string,Bucket=BUCKET,Key=f"api_links/links{choice}.json")
    
    
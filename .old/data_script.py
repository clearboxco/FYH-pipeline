if __name__ == "__main__":

    from redfin_scraper import RedfinScraper
    import pandas as pd
    import time
    from config import BUCKET,SECRET,KEY

    scraper = RedfinScraper()

    scraper.setup("./zip_code_database.csv",multiprocessing=True)

    filtered=scraper.zip_database[(scraper.zip_database['decommissioned']==0)&
                                (scraper.zip_database['type']=='STANDARD')&
                                (scraper.zip_database['irs_estimated_population']>0)]

    states=['AL',
            'NE',
            'DE',
            'NV',
            'PR',
            'VI',
            'AK',
            'AZ',
            'NH',
            'AR',
            'NJ',
            'CA',
            'NM',
            'CO',
            'NY',
            'CT',
            'NC',
            'ND',
            'DC',
            'OH',
            'FL',
            'OK',
            'GA',
            'OR',
            'HI',
            'PA',
            'ID',
            'IL',
            'RI',
            'IN',
            'SC',
            'IA',
            'SD',
            'KS',
            'TN',
            'KY',
            'TX',
            'LA',
            'UT',
            'ME',
            'VT',
            'MD',
            'VA',
            'MA',
            'MI',
            'WA',
            'MN',
            'WV',
            'MS',
            'WI',
            'MO',
            'WY',
            'MT',
            ]
    
    NA_states=['NE',
            'DE',
            'NV',
            'PR',
            'VI']
    
    
    time_stamp = pd.to_datetime('now').replace(microsecond=0)

    ts_string=str(time_stamp).split()[0]

    subtotal=0
    
    for i in range(len(states)):
        zip_list=filtered[filtered["state"]==states[i]]
        res=scraper.scrape(zip_codes=list(zip_list['zip']))
        
        if (res is None or res.empty) and (states[i] not in NA_states):
            c=0
            while(c<10 and (res is None or res.empty)):
                time.sleep(1)
                res=scraper.scrape(zip_codes=list(zip_list['zip']))
                c+=1
            else:
                if(c==10):
                    print(f"Failed to scrape valid state: {states[i]}")
                    
            subtotal+=c
        
        time.sleep(1)

    df_list=[]
    
    for i in range(len(states)+subtotal):
        df=scraper.get_data(f'D{i+1:03d}')
        
        try:
            df.insert(1, 'TimeStamp', time_stamp)
            df_list.append(df)
            df.astype(str).to_parquet(f"{BUCKET}/{ts_string}/{states[i]}.parquet",index=False,storage_options={"key":KEY,
                                                                                               "secret":SECRET})
        except:
            continue
        
    if len(df_list)<1:
        raise Exception("Scrape Failed: Nothing to concatenate.")
        
    concat_df=pd.concat(df_list)
    
    concat_df.astype(str).to_parquet(f"{BUCKET}/all/{ts_string}.parquet",index=False,storage_options={"key":KEY,
                                                                                               "secret":SECRET})
        
    
        


    


    

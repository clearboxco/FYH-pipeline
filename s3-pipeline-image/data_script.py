if __name__ == "__main__":

    from redfin_scraper import RedfinScraper
    import pandas as pd
    import time
    import os

    aws_bucket=os.environ['AWS_RE_BUCKET']
    aws_key=os.environ['AWS_ADMIN_KEY']
    aws_secret=os.environ['AWS_ADMIN_SECRET']

    scraper = RedfinScraper()

    scraper.setup("./zip_code_database.csv",multiprocessing=True)

    filtered=scraper.zip_database[(scraper.zip_database['decommissioned']==0)&
                                (scraper.zip_database['type']=='STANDARD')&
                                (scraper.zip_database['irs_estimated_population']>0)]

    states=['AL',
 'NE',
 'AK',
 'NV',
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
 'DE',
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
 'PR',
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
 'VI',
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

    for i in range(len(states)):
        zip_list=filtered[filtered["state"]==states[i]]
        scraper.scrape(zip_codes=list(zip_list['zip']))
        time.sleep(1)

    time_stamp = pd.to_datetime('now').replace(microsecond=0)

    ts_string=str(time_stamp).split()[0]

    for i in range(len(states)):
        df=scraper.get_data(f'D{i+1:03d}')
        try:
            df.insert(1, 'TimeStamp', time_stamp)
            df.astype(str).to_parquet(f"{aws_bucket}/{ts_string}/{states[i]}.parquet",index=False,storage_options={"key":aws_key,
                                                                                               "secret":aws_secret})
        except:
            continue
        


    


    

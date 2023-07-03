# IMPORTS
import os
import sys
import time

import pandas as pd
import numpy as np

from sqlalchemy import URL,create_engine,text

from config import USER,PASS,ENDPOINT,PORT,DBNAME

if __name__=="__main__":
    # STEP 1: IMPORT S3 PARQUET DATA
    
    aws_bucket=os.environ['AWS_RE_BUCKET']
    aws_key=os.environ['AWS_ADMIN_KEY']
    aws_secret=os.environ['AWS_ADMIN_SECRET']


    ts= pd.to_datetime('now').replace(microsecond=0)
    
    date=sys.argv[1] if len(sys.argv)>1 else str(ts).split()[0]
    
    states=['AL', # NO NEVADA; NO DELAWARE; # NO PUERTO RICO; # NO VIRGIN ISLANDS
            'NE',
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
    
    df_list=[]
    for st in states:
        try:
            df = pd.read_parquet(path=aws_bucket+f"/{date}/{st}.parquet",storage_options={"key":aws_key,"secret":aws_secret})
            df_list.append(df)
        except:
            raise Exception(f"Could not locate {aws_bucket}/{date}/{st}.parquet")
        
    df=pd.concat(df_list)
        
    #1B: Applying conceptual adjustments
    
    df=df.applymap(lambda x: np.nan if (x=='nan') or (x=="") else x)

    prop_types=['Single Family Residential', 'Mobile/Manufactured Home','Townhouse', 'Multi-Family (2-4 Unit)', 'Condo/Co-op','Multi-Family (5+ Unit)']

    df=df[df['PROPERTY TYPE'].isin(prop_types)]

    df=df.dropna(subset=['ADDRESS','CITY','STATE OR PROVINCE','ZIP OR POSTAL CODE','PROPERTY TYPE','PRICE','BEDS','BATHS','SQUARE FEET','YEAR BUILT','URL (SEE https://www.redfin.com/buy-a-home/comparative-market-analysis FOR INFO ON PRICING)'],axis=0).reset_index(drop=True)
    
    #1C: Apply logical adjustments
    
    with open('./scripts/columns.txt','r') as f: # CHANGE TO INSTANCE PATHS
        lines=f.readlines()
    
    sql_columns=[str(line.strip()) for line in lines]
    
    df.columns=sql_columns
    
    df=df.astype({
        sql_columns[0]:'object', 
        sql_columns[1]:'object', 
        sql_columns[2]:'object', 
        sql_columns[3]:'object', 
        sql_columns[4]:'object', 
        sql_columns[5]:'object', 
        sql_columns[6]:'object', 
        sql_columns[7]:'object', 
        sql_columns[8]:'float64', 
        sql_columns[9]:'float64', 
        sql_columns[10]:'float64', 
        sql_columns[11]:'object', 
        sql_columns[12]:'float64', 
        sql_columns[13]:'float64', 
        sql_columns[14]:'float64', 
        sql_columns[15]:'float64', 
        sql_columns[16]:'float64', 
        sql_columns[17]:'float64', 
        sql_columns[18]:'object', 
        sql_columns[19]:'object', 
        sql_columns[20]:'object', 
        sql_columns[21]:'object', 
        sql_columns[22]:'object', 
        sql_columns[23]:'object', 
        sql_columns[24]:"bool", 
        sql_columns[25]:"bool", 
        sql_columns[26]:"float64", 
        sql_columns[27]:"float64"
        },
        errors='ignore'
    )
    
    df[sql_columns[1]]=pd.to_datetime(df[sql_columns[1]],errors='raise',format="ISO8601")
    df[sql_columns[2]]=pd.to_datetime(df[sql_columns[2]],errors='coerce',format="mixed")
    df[sql_columns[20]]=pd.to_datetime(df[sql_columns[20]],errors='coerce',format="mixed")
    df[sql_columns[19]]=pd.to_datetime(df[sql_columns[19]],errors='coerce',format="mixed")
            
    def extract_zip(zip:str):
        
        # Hm... bit error prone you'd think
        
        zip=zip.strip()
        return zip[0:5]
        
    df[sql_columns[7]]=df[sql_columns[7]].map(lambda x: extract_zip(x))
    
    
    
    
    # STEP 2: CONNECT TO POSTGRESQL DB
    
    with open('./scripts/tables.txt','r') as f:
        lines=f.readlines()
    
    sql_tables=[str(line.strip()) for line in lines]
    
    url=URL.create(
        drivername="postgresql+psycopg2",
        host=ENDPOINT,
        database=DBNAME,
        username=USER,
        password=PASS,
        port=PORT
    )
    
    engine = create_engine(url)
    
    conn=engine.connect()

    df.to_sql(sql_tables[2],conn,if_exists='replace',index=False,method='multi',chunksize=1000)

    
    
    
    # Need to iterate through dataframe and insert values into houses
    # Would prefer to keep the schema hidden in sql scripts and format the values in later
    
    


    # STEP 3: JOIN DATA; ASSIGN IDS TO NEW RECORDS AND MOVE OLD TO BAKCUP

    # STEP 4: PUSH NEW DATA TO POSTGRES DATABASE
    
    
    conn.close()
    
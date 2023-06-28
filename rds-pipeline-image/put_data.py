# IMPORTS
import pandas as pd
import psycopg2 as pg
import os


if __name__=="__main__":
    # STEP 1: IMPORT S3 PARUQET DATA
    
    aws_bucket=os.environ['AWS_RE_BUCKET']
    aws_key=os.environ['AWS_ADMIN_KEY']
    aws_secret=os.environ['AWS_ADMIN_SECRET']


    time_stamp = pd.to_datetime('now').replace(microsecond=0)

    date=str(time_stamp).split()[0]
    
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
    
    df_list=[]
    for st in states:
        try:
            df = pd.read_parquet(path=aws_bucket+f"/{date}/{st}.parquet",storage_options={"key":aws_key,"secret":aws_secret})
            df_list.append(df)
        except:
            raise Exception(f"Could not locate {aws_bucket}/{date}/{st}.parquet")



    # STEP 2: READ IN CURRENT POSTGRESQL DATA

    # STEP 3: JOIN DATA; ASSIGN IDS TO NEW RECORDS AND DELETE OLD

    # STEP 4: PUSH NEW DATA TO POSTGRES DATABASE
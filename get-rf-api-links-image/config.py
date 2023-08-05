import os

BUCKET='real-estate-data-bucket'
KEY=os.environ['AWS_ADMIN_KEY']
SECRET=os.environ['AWS_ADMIN_SECRET']

REDFIN_URL="https://www.redfin.com{}"
REDFIN_ZIP_URL="/zipcode/{zip_code}"

REDFIN_API_CLASS_DEF=("a",{"class":"downloadLink"})
REDFIN_API_CLASS_ID='href'
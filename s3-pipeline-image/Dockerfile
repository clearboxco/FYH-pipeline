FROM python:latest
WORKDIR /pipeline

COPY requirements.txt requirements.txt
COPY data_script.py data_script.py
COPY zip_code_database.csv zip_code_database.csv
COPY execute_script.sh execute_script.sh

RUN pip install --no-cache-dir -r requirements.txt

RUN chmod +x execute_script.sh

CMD ["./execute_script.sh"]
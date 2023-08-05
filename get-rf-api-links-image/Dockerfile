FROM python:latest
WORKDIR /pipeline

COPY config.py config.py
COPY one_time_run.py one_time_run.py
COPY requirements.txt requirements.txt
COPY zip_code_database.csv zip_code_database.csv

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python","one_time_run.py"]

CMD []
FROM python:latest
WORKDIR /pipeline

COPY requirements.txt requirements.txt
COPY config.py config.py
COPY put_data.py put_data.py
COPY scripts scripts

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]

CMD ["put_data.py"]
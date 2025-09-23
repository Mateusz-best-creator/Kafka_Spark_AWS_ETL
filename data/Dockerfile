FROM python:3.10

RUN mkdir /app

WORKDIR /app

COPY requirements.txt /app

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY transform_upload_data.sh /app 
COPY *.py /app

RUN chmod a+x /app/transform_upload_data.sh

ENTRYPOINT ["./transform_upload_data.sh"]
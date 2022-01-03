FROM python:3.8-buster

RUN pip install psycopg2-binary

ADD main.py /
ADD config.ini /

CMD [ "python", "./main.py" ]

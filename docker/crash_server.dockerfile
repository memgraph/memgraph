FROM python:3.5

COPY src/crash_analysis/server /server

RUN pip install -r /server/requirements.txt

WORKDIR /server

CMD python app.py

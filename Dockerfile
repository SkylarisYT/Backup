FROM python:latest

WORKDIR /usr/src/backup

ADD . /usr/src/backup

RUN pip3 install --trusted-host pypi.python.org -U -r requirements.txt

ENV ACCESS_TOKEN 0
ENV RETHINKDB_PW 0
ENV DUMP_NAME 0
ENV REPO 0
ENV HOST localhost:28015

CMD ["sh", "-c", "python3 backup.py ${ACCESS_TOKEN} ${RETHINKDB_PW} ${DUMP_NAME} ${REPO} ${HOST}"]

FROM quay.io/astronomer/astro-runtime:12.7.1
USER root
RUN apt-get update && apt-get install -y sqlite3 libsqlite3-dev unixodbc unixodbc-dev
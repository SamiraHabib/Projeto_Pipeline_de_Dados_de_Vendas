# Usa a imagem base do Astro Runtime
FROM quay.io/astronomer/astro-runtime:12.7.1

USER root

RUN apt-get update && apt-get install -y unixodbc unixodbc-dev

COPY .env .env
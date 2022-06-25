FROM python:3.10-slim as base

FROM base as builder

RUN mkdir /install
WORKDIR /install

COPY requirements.txt /requirements.txt
RUN pip install --target=/install -r /requirements.txt --upgrade

FROM base
EXPOSE 8000

RUN apt-get update \
  && apt-get install libgomp1 -y \
  && apt-get clean

RUN mkdir /deps

COPY --from=builder /install /deps
COPY . /app

WORKDIR /app

ENV PYTHONPATH=/deps


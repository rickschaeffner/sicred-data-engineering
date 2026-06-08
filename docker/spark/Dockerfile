FROM apache/spark:3.5.1-python3

LABEL maintainer="Sicred Data Engineering"
LABEL version="1.0.0"

USER root

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
        procps \
        curl \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/spark/jars

RUN curl -fsSL \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar" \
    -o /opt/spark/jars/postgresql-42.7.3.jar

RUN ln -sf /usr/bin/python3 /usr/bin/python

WORKDIR /opt/spark/work-dir

COPY requirements.txt .

RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir pyspark==3.5.1 PyYAML==6.0.1 python-dotenv==1.0.1

COPY spark/ ./spark/
COPY config/ ./config/
COPY setup.py .

RUN pip3 install --no-cache-dir --no-deps -e .

RUN mkdir -p /data/output && chmod 777 /data/output

ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3
ENV PYTHONPATH=/opt/spark/work-dir

ENTRYPOINT ["python3", "-m", "spark.jobs.carteira_flat_etl"]
CMD ["--help"]
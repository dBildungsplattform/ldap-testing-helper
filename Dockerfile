FROM ubuntu:20.04


ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /usr/src/app


RUN apt-get update && apt-get install -y \
    python3.8 \
    python3-pip \
    python3-dev \
    libsasl2-dev \
    libldap2-dev \
    libssl-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

RUN python3.8 -m pip install --upgrade pip

COPY . .

RUN pip install --no-cache-dir pandas python-ldap requests openpyxl python-dotenv

RUN chmod +x main.py

CMD ["python3.8", "main.py"]


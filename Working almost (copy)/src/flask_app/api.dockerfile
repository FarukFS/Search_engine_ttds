FROM python:3.6.10-slim-buster


ENV LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONUNBUFFERED=1

WORKDIR /api/

COPY api_requirements.txt ./

RUN pip install --no-cache-dir -r api_requirements.txt && \
    rm api_requirements.txt

COPY app/ .

EXPOSE 5001

ENTRYPOINT ["/bin/sh", "-c", "sh deploy_api.sh"]
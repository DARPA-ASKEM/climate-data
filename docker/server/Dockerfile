FROM python:3.11
RUN useradd -m svc
RUN mkdir -p /opt/climate-search
WORKDIR /opt/climate-search
ADD ./ /opt/climate-search
RUN apt-get update && apt-get install -y curl
RUN pip install poetry
RUN chown -R svc:svc /opt/climate-search
USER svc
RUN poetry install
# CMD nohup poetry run uvicorn api.server:app --reload --host 0.0.0.0
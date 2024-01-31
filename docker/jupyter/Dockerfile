FROM python:3.11
RUN useradd -m svc
RUN mkdir -p /opt/climate-search
WORKDIR /opt/climate-search
COPY ./ /opt/climate-search
RUN apt-get update && apt-get install -y curl
RUN pip install poetry
RUN chown -R svc:svc /opt/climate-search
COPY ./.cdsapirc /opt/climate-search/.cdsapirc
# RUN cp /opt/climate-search/.cdsapirc /home/svc/.cdsapirc && chmod 644 /home/svc/.cdsapirc && chown svc:svc /home/svc/.cdsapirc
USER svc
RUN poetry install --with dev
CMD nohup poetry run jupyter notebook --ip 0.0.0.0 --no-browser
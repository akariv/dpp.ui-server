FROM frictionlessdata/datapackage-pipelines:latest

ENV APP_PATH=/opt/app
ENV GUNICORN_USER=datapipes

RUN pip3 install --upgrade pip gunicorn \
    && mkdir -p $APP_PATH \
    && adduser -D -h $APP_PATH $GUNICORN_USER

# Copy the application dependencies over into the container.
ADD datapipes/requirements.txt $APP_PATH/requirements.txt
ADD datapackage_pipelines_datapipes/requirements.txt $APP_PATH/requirements2.txt

# Install  the application's dependencies.
RUN pip3 install -r $APP_PATH/requirements.txt
RUN pip3 install -r $APP_PATH/requirements2.txt

# Copy the application over into the container.
ADD . $APP_PATH

RUN pip3 install -e $APP_PATH/datapackage_pipelines_datapipes
RUN mkdir -p /var/datapip.es && chown $GUNICORN_USER.$GUNICORN_USER -R /var/datapip.es

USER $GUNICORN_USER
WORKDIR $APP_PATH/datapipes
EXPOSE 8000

ENTRYPOINT $APP_PATH/datapipes/entrypoint.sh

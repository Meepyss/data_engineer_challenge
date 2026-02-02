FROM mongo:6.0

ENV MONGO_INITDB_DATABASE=raw_data


COPY init-mongo.sh /docker-entrypoint-initdb.d/init-mongo.sh


RUN sed -i 's/\r$//' /docker-entrypoint-initdb.d/init-mongo.sh && \
    chmod +x /docker-entrypoint-initdb.d/init-mongo.sh
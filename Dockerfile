FROM postgres
ENV POSTGRES_USER sparkdb
ENV POSTGRES_PASSWORD sparkdb
ADD schema.sql /docker-entrypoint-initdb.d
EXPOSE 5432
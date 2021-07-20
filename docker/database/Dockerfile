FROM __BASEIMAGE__
COPY ./sql/addon_i2b2metadata.i2b2.sql /sql.d/10_i2b2_addon_metadata.sql
COPY ./sql/addon_i2b2crcdata.concept_dimension.sql /sql.d/11_i2b2_addon_crcdata.sql
COPY ./sql/aktin_postgres_init.sql /sql.d/20_aktin_init.sql
COPY ./sql/aktin_postgres_drop.sql /sql.d/aktin_drop.sql
RUN cat /sql.d/10_i2b2_addon_metadata.sql >>/docker-entrypoint-initdb.d/init.sql
RUN cat /sql.d/11_i2b2_addon_crcdata.sql >>/docker-entrypoint-initdb.d/init.sql
RUN cat /sql.d/20_aktin_init.sql >>/docker-entrypoint-initdb.d/init.sql


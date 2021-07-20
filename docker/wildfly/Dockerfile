FROM __BASEIMAGE__
COPY ./dwh-j2ee-*.ear /opt/jboss/wildfly/standalone/deployments/
COPY ./ds/* /usr/share/aktin-notaufnahme-i2b2/datasource/
COPY ./aktin.properties /etc/aktin/
RUN ln -sf /etc/aktin/aktin.properties /opt/jboss/wildfly/standalone/configuration/
COPY ./import-scripts /var/lib/aktin/import-scripts
USER root
RUN mkdir -p /var/lib/aktin/import
RUN chown -R jboss:jboss /opt/jboss/wildfly/standalone/deployments /usr/share/aktin-notaufnahme-i2b2/datasource /etc/aktin /var/lib/aktin
USER jboss


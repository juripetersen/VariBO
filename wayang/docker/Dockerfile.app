FROM thesis-wayang-base:latest

RUN mkdir -p /var/www/html
RUN mkdir -p /opt/data

COPY . /var/www/html
WORKDIR /var/www/html

COPY ./data /opt/data

ENV MAVEN_CONFIG ""

# Aliases for builds
RUN echo 'build () { cd /var/www/html && mvn clean install -DskipTests -Drat.skip=true -Djacoco.skip=true -Dmaven.javadoc.skip=true; }' >> /root/.bashrc && \
    echo 'rebuild () { cd /var/www/html && mvn -T1C clean install -DskipTests -Drat.skip=true -Djacoco.skip=true -Dmaven.javadoc.skip=true -pl :"$1" && mvn -T1C clean package -DskipTests -Djacoco.skip=true -Dmaven.javadoc.skip=true -pl :"$1"; }' >> /root/.bashrc && \
    echo 'assemble () { mvn clean package -T1C -Djacoco.skip=true -Dmaven.javadoc.skip=true -pl :wayang-assembly -Pdistribution && cd wayang-assembly/target && tar -xvf apache-wayang-assembly-0.7.1-incubating-dist.tar.gz && cd wayang-0.7.1; }' >> /root/.bashrc

ENTRYPOINT "/bin/bash"

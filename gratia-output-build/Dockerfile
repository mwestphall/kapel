FROM almalinux:9

# Install EPEL, the OSG software base repo, and gratia-probe-common
RUN yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm && \
    yum install -y https://repo.opensciencegrid.org/osg/23-main/osg-23-main-el9-release-latest.rpm && \
    yum install -y gratia-probe-common python3-pip

WORKDIR /etc/kapel-gratia-output
COPY python/requirements.txt /etc/kapel-gratia-output/
RUN pip install -r requirements.txt
COPY python/*.py /etc/kapel-gratia-output/

CMD python3 gratia_output.py

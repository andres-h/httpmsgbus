FROM centos:7


# update base os and install dependencies
RUN yum update -y && \
    yum install -y \
        git \
        go \
        pcre-devel && \
    yum clean all


# copy project into container
COPY . /app/

# install as unprivileged user
RUN useradd -m httpmsgbus_user
RUN chown -R httpmsgbus_user:httpmsgbus_user /app
USER httpmsgbus_user
RUN /app/install.sh


# configure entrypoint
WORKDIR /home/httpmsgbus_user
EXPOSE 8000
ENTRYPOINT [ "seiscomp3/sbin/httpmsgbus" ]

FROM harbor.mty.wang/gobase/ubuntu:20.04
LABEL maintainer="bluketalk <bluketalk@qq.com>"
ENV WORK_HOME  /opt/mtoss/nameserver
ENV development=prod
ENV PATH=/usr/local/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ADD ./.build/nameserver  /$WORK_HOME/
RUN chmod +x $WORK_HOME/docker-startup.sh && \
    chmod +x $WORK_HOME/bin/nameserver
VOLUME $WORK_HOME/cert
VOLUME $WORK_HOME/logs
WORKDIR $WORK_HOME
EXPOSE     8522
ENTRYPOINT [ "./docker-startup.sh" ]

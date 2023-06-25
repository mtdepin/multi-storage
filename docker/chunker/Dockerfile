FROM harbor.mty.wang/gobase/ubuntu:20.04
LABEL maintainer="bluketalk <bluketalk@qq.com>"
ENV WORK_HOME  /opt/mtoss/chunker
ENV development=prod
ENV PATH=/usr/local/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ADD ./.build/chunker/  /$WORK_HOME/
RUN chmod +x $WORK_HOME/docker-startup.sh && \
    chmod +x $WORK_HOME/bin/chunker
VOLUME $WORK_HOME/cert
VOLUME $WORK_HOME/logs
WORKDIR $WORK_HOME
EXPOSE     8521
ENTRYPOINT [ "./docker-startup.sh" ]

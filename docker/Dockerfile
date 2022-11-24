FROM python:3.7

ARG ENV
ARG APP_NAME
ENV env ${ENV}
ENV APP_NAME ${APP_NAME}
ENV APP_HOME /home/${APP_NAME}

ENV PIP_INSTALL "pip3 install -i https://mirrors.aliyun.com/pypi/simple"

WORKDIR $APP_HOME
RUN chmod 777 $APP_HOME

# 创建项目目录
RUN mkdir -p ${APP_HOME}/logs/app && mkdir -p ${APP_HOME}/logs/supervisord

# 将项目源代码复制到容器的APP_HOME目录
ADD . $APP_HOME

# 更新apt-get、 pip、安装pip包
RUN ${PIP_INSTALL} supervisor && \
    ${PIP_INSTALL} -r ${APP_HOME}/docker/requirements.txt

#设置时区
RUN /bin/cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo 'Asia/Shanghai' >/etc/timezone

# 启动supervisor
COPY ./docker/supervisord.conf /etc/supervisor/supervisord.conf
CMD ["supervisord", "-n", "-c", "/etc/supervisor/supervisord.conf"]
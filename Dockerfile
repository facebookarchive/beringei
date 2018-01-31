# Pull base image.
FROM ubuntu:17.10

ENV WORKDIR /usr/local/beringei
ENV RUN_CMD ./beringei/service/beringei_main \
              -beringei_configuration_path $WORKDIR/beringei.json \
              -create_directories \
              -sleep_between_bucket_finalization_secs 60 \
              -allowed_timestamp_behind 300 \
              -bucket_size 600 \
              -buckets 144 \
              -logtostderr \
              -v=2

RUN apt-get update
RUN apt-get -y upgrade
# Copy files from CircleCI into docker container.
COPY . $WORKDIR

# Define default command.
CMD ["bash"]

# Setup the docker container.
WORKDIR $WORKDIR
RUN $WORKDIR/setup_ubuntu.sh

RUN apt-get install -y libmysqlclient20 libmysqlclient-dev libmysql++3v5 libmysqlcppconn7v5 libmysqlcppconn-dev libboost-all-dev libcap-dev libdouble-conversion-dev libevent-dev libgflags2.2 libgoogle-glog-dev libjemalloc-dev libkrb5-dev liblz4-dev liblzma-dev libnuma-dev libsasl2-dev libsnappy-dev libssl-dev zlib1g-dev

RUN mkdir -p /usr/local/mysql/lib
RUN ln -s /usr/lib/libmysqlpp.so* /usr/local/mysql/lib
RUN ln -s /usr/lib/x86_64-linux-gnu/libmysqlclient.* /usr/local/mysql/lib/

# Create a build directory.
RUN mkdir $WORKDIR/build
WORKDIR $WORKDIR/build

# Compile and install
RUN cmake ..
RUN make install

RUN ./beringei/tools/beringei_configuration_generator --host_names localhost --file_path $WORKDIR/beringei.json

ENTRYPOINT $RUN_CMD

# Pull base image.
FROM ubuntu:16.10

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

# Copy files from CircleCI into docker container.
COPY . $WORKDIR

# Define default command.
CMD ["bash"]

# Setup the docker container.
WORKDIR $WORKDIR
RUN $WORKDIR/setup_ubuntu.sh

# Create a build directory.
RUN mkdir $WORKDIR/build
WORKDIR $WORKDIR/build

# Compile and install
RUN cmake ..
RUN make install

RUN ./beringei/tools/beringei_configuration_generator --host_names localhost --file_path $WORKDIR/beringei.json

ENTRYPOINT $RUN_CMD
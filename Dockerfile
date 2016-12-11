# Pull base image.
FROM ubuntu:16.10

# Copy files from CircleCI into docker container.
COPY . /tmp/beringei

# Define default command.
CMD ["bash"]

# Setup the docker container.
WORKDIR /tmp/beringei
RUN /tmp/beringei/setup_ubuntu.sh

# Create a build directory.
RUN mkdir /tmp/beringei/build
WORKDIR /tmp/beringei/build

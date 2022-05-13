FROM ubuntu:20.04

# Setup python and java and base system
ENV DEBIAN_FRONTEND noninteractive
ENV LANG=en_US.UTF-8

RUN apt-get update && \
  apt-get upgrade -y && \
  apt-get install -q -y openjdk-8-jdk python3-pip

# set up location of code
WORKDIR /code
ENV PYTHONPATH=/code/src

# install python requirements
ADD ./containers/pipelinerunner/requirements.txt requirements.txt
RUN pip install -r requirements.txt

# copy repo
COPY ./ /code/

# install cron
RUN apt-get update && apt-get install cron -y

# ref: https://stackoverflow.com/questions/37458287/how-to-run-a-cron-job-inside-a-docker-container
# Copy pull_data file to the cron.d directory
COPY /scheduler/pull_data /etc/cron.d/pull_data
 
# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/pull_data

# Apply cron job
RUN crontab /etc/cron.d/pull_data

# Create the log file to be able to run tail
RUN touch /var/log/cron.log
 
# Run cron
CMD cron && tail -f /var/log/cron.log
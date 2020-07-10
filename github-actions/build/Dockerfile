FROM ubuntu:20.04
ENV TZ=Europe/Budapest
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Lynx dev environment.
ENV PATH="/usr/local/go/bin:/opt/conda/bin:$PATH"

# Enable HTTPS repositories.
RUN \
  mkdir -p /var/lib/apt/lists/partial && apt-get update && \
  apt-get install -y apt-transport-https gnupg curl wget

# Add repository for SBT.
RUN \
  echo 'deb https://dl.bintray.com/sbt/debian /' > /etc/apt/sources.list.d/sbt.list && \
  apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 \
    --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823

# Add repository for Node.js.
RUN \
  curl -sL https://deb.nodesource.com/setup_12.x | bash -

# Install packages.
RUN \
  apt-get update && \
  apt-get install -y build-essential coreutils default-libmysqlclient-dev git nodejs rsync sbt xvfb && \
  npm install -g yarn gulp

# Google Chrome for frontend testing.
RUN \
  apt-get install -y fonts-liberation libappindicator3-1 libxss1 xdg-utils libasound2 libnspr4 libnss3 libgbm1 && \
  wget -nv 'https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb' && \
  dpkg -i 'google-chrome-stable_current_amd64.deb'

# Anaconda Python.
RUN \
  wget -nv 'https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh' -O conda.sh && \
  bash conda.sh -b -p /opt/conda && \
  rm conda.sh && \
  conda install python=3.7

# Install Go.
RUN \
  wget -nv https://dl.google.com/go/go1.14.4.linux-amd64.tar.gz && \
  tar -C /usr/local -xzf go1.14.4.linux-amd64.tar.gz

RUN \
  apt-get install -y zip openjdk-8-jdk

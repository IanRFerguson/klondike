FROM ubuntu:16.04

# Update default packages
RUN apt-get update

# Get Ubuntu packages
RUN apt-get install -y \
    build-essential \
    curl

# Update new packages
RUN apt-get update

# Get Rust
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y

RUN echo 'source $HOME/.cargo/env' >> $HOME/.bashrc

#####

FROM python:3.10-alpine

COPY . /app/
WORKDIR /app

# Install Python dependencies
RUN pip install -r requirements.txt
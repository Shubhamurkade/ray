# This file shouldn't be pushed to open-source repo
FROM harbor-repo.vmware.com/dockerhub-proxy-cache/rayproject/ray:latest-cpu
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

RUN pip install --upgrade git+https://github.com/vmware/vsphere-automation-sdk-python.git
RUN pip install https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp37-cp37m-manylinux2014_x86_64.whl
RUN pip install grpcio==1.54.2
ADD . ray
RUN python ray/python/ray/setup-dev.py --skip rllib air tune train cloudpickle data internal tests experimental util workflow dag widgets cluster_utils.py _private dashboard scripts -y
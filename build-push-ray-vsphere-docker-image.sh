# This file should't be pushed to open-source repo
user=$1
repository="ray-on-vsphere"
docker login harbor-repo.vmware.com
docker build -f docker/ray-on-vsphere-dev/Dockerfile . -t harbor-repo.vmware.com/ray/$repository:$user
docker push harbor-repo.vmware.com/ray/$repository:$user

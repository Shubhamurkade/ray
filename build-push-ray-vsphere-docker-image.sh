# This file should't be pushed to open-source repo
user=$1
repository="ray-on-vsphere"

docker login harbor-repo.vmware.com -u 'robot$ray+ray' -p 'uwrDfmRKtWAgFvqHDolxAqgtI48aemxL'

if [ $? -ne 0 ]
then
    echo "Login failed"
    exit 1
fi
docker build -f docker/ray-on-vsphere-dev/Dockerfile . -t harbor-repo.vmware.com/ray/$repository:$user
if [ $? -ne 0 ]
then
    echo "Docker build failed"
    exit 1
fi
docker push harbor-repo.vmware.com/ray/$repository:$user
if [ $? -ne 0 ]
then
    echo "Docker push failed"
    exit 1
fi
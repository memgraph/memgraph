#!/bin/bash -e

function echo_info { printf "\n\033[1;36m~~ $1 ~~\033[0m\n"; }
function echo_success { printf "\n\n\033[1;32m~~ $1 ~~\033[0m\n"; }

CPUS=$( cat /proc/cpuinfo | grep processor | wc -l )
NPROC=$(( $CPUS / 2 ))

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

version="2.4.47"
name="openldap-$version"

if [ -d "$name" ]; then
    rm -rf "$name"
fi

echo_info "Downloading and unpacking OpenLDAP"
wget -nv -O $name.tgz https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/$name.tgz
tar -xf $name.tgz
rm $name.tgz

cd "$name"

echo_info "Configuring OpenLDAP build"
sed 's/include\ libraries\ clients\ servers\ tests\ doc/include libraries servers/g' -i Makefile.in
./configure --prefix="$DIR/$name/exe" \
    --disable-bdb \
    --disable-hdb \
    --disable-relay \
    --disable-monitor \
    --disable-dependency-tracking

echo_info "Building dependencies"
make depend -j$NPROC

echo_info "Building OpenLDAP"
make -j$NPROC

echo_info "Installing OpenLDAP"
make install -j$NPROC

echo_info "Configuring and importing schema"
sed 's/my-domain/memgraph/g' -i exe/etc/openldap/slapd.conf
sed 's/Manager/admin/g' -i exe/etc/openldap/slapd.conf
mkdir exe/var/openldap-data
./exe/sbin/slapadd -l ../schema.ldif

echo_success "Done!"

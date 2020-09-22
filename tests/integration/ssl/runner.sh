#!/bin/bash -e

pushd () { command pushd "$@" > /dev/null; }
popd () { command popd "$@" > /dev/null; }
die () { printf "\033[1;31m~~ Test failed! ~~\033[0m\n\n"; exit 1; }

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

tmpdir=/tmp/memgraph_integration_ssl

if [ -d $tmpdir ]; then
    rm -rf $tmpdir
fi
mkdir -p $tmpdir
cd $tmpdir

easyrsa="EasyRSA-3.0.4"
wget -nv https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/$easyrsa.tgz

tar -xf $easyrsa.tgz
mv $easyrsa ca1
cp -r ca1 ca2

for i in ca1 ca2; do
    pushd $i
    ./easyrsa --batch init-pki
    ./easyrsa --batch build-ca nopass
    ./easyrsa --batch build-server-full server nopass
    ./easyrsa --batch build-client-full client nopass
    popd
done

binary_dir="$DIR/../../../build"

set +e

echo
for i in ca1 ca2; do
    for j in none ca1 ca2; do
        for k in false true; do
            printf "\033[1;36m~~ Server CA: $i; Client CA: $j; Verify peer: $k ~~\033[0m\n"

            if [ "$j" == "none" ]; then
                client_key=""
                client_cert=""
            else
                client_key=$j/pki/private/client.key
                client_cert=$j/pki/issued/client.crt
            fi

            $binary_dir/tests/integration/ssl/tester \
                --server-key-file=$i/pki/private/server.key \
                --server-cert-file=$i/pki/issued/server.crt \
                --server-ca-file=$i/pki/ca.crt \
                --server-verify-peer=$k \
                --client-key-file=$client_key \
                --client-cert-file=$client_cert

            exitcode=$?

            if [ "$i" == "$j" ]; then
                [ $exitcode -eq 0 ] || die
            else
                if $k; then
                    [ $exitcode -ne 0 ] || die
                else
                    [ $exitcode -eq 0 ] || die
                fi
            fi

            printf "\033[1;32m~~ Test ok! ~~\033[0m\n\n"
        done
    done
done

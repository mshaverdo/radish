#!/bin/bash
set -e

LDFLAGS="-X main.debug=0 $LDFLAGS"

# Check go install
if [ "$(which go)" == "" ]; then
	echo "error: Go is not installed. Please download and follow installation instructions at https://golang.org/dl to continue."
	exit 1
fi

# Check dep install
if [ "$(which dep)" == "" ]; then
	echo "error: dep Go dependency manager is not installed. Please download and follow installation instructions at https://github.com/golang/dep to continue."
	exit 1
fi

# Check go version
GOVERS="$(go version | cut -d " " -f 3)"
if [ "$GOVERS" != "devel" ]; then
	vercomp () {
		if [[ $1 == $2 ]]
		then
			echo "0"
			return
		fi
		local IFS=.
		local i ver1=($1) ver2=($2)
		# fill empty fields in ver1 with zeros
		for ((i=${#ver1[@]}; i<${#ver2[@]}; i++))
		do
			ver1[i]=0
		done
		for ((i=0; i<${#ver1[@]}; i++))
		do
			if [[ -z ${ver2[i]} ]]
			then
				# fill empty fields in ver2 with zeros
				ver2[i]=0
			fi
			if ((10#${ver1[i]} > 10#${ver2[i]}))
			then
				echo "1"
				return
			fi
			if ((10#${ver1[i]} < 10#${ver2[i]}))
			then
				echo "-1"
				return
			fi
		done
		echo "0"
		return
	}
	GOVERS="${GOVERS:2}"
	EQRES=$(vercomp "$GOVERS" "1.7")
	if [ "$EQRES" == "-1" ]; then
		  echo "error: Go '1.7' or greater is required and '$GOVERS' is currently installed. Please upgrade Go at https://golang.org/dl to continue."
		  exit 1
	fi
fi

export GO15VENDOREXPERIMENT=1

cd $(dirname "${BASH_SOURCE[0]}")
OD="$(pwd)"


# temp directory for storing isolated environment.
TMP="$(mktemp -d -t radish.XXXX)"
function rmtemp {
	rm -rf "$TMP"
}
trap rmtemp EXIT

if [ "$NOCOPY" != "1" ]; then
	# copy all files to an isloated directory.
	WD="$TMP/src/github.com/mshaverdo/radish"
	export GOPATH="$TMP"
	for file in `find . -type f`; do
		if [[ "$file" != "." && "$file" != ./.git* ]]; then
			mkdir -p "$WD/$(dirname "${file}")"
			cp -P "$file" "$WD/$(dirname "${file}")"
		fi
	done
	cd $WD
fi

#install dependencies
dep ensure

# test if requested
if [ "$1" == "test" ]; then
		go test -short ./... | grep -Pv "^\?"
fi

if [ "$1" == "full-test" ]; then
		GOCACHE=off go test -race -tags integration ./... | grep -Pv "^\?"
fi

# build and store objects into original directory.
go build -ldflags "$LDFLAGS" -o "$OD/radish-server" cmd/radish-server/*.go
go build -ldflags "$LDFLAGS" -o "$OD/radish-benchmark-http" cmd/radish-benchmark-http/*.go


all: build

build:
	bash build.sh

clean:
	rm -f radish-server
	rm -f radish-benchmark

test:
	bash build.sh test
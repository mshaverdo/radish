all: build

build:
	bash build.sh test

clean:
	rm -f radish-server
	rm -f radish-benchmark-http

test:
	bash build.sh test

full-test:
	bash build.sh full-test

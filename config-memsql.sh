# Pull the image
docker pull memsql/quickstart


# Verify your machine satisfies our minimum requirements
docker run --rm --net=host memsql/quickstart check-system


# Spin up a MemSQL cluster on your machine
docker run -d -p 3306:3306 -p 9000:9000 --name=memsql memsql/quickstart


# Run a quick benchmark against MemSQL
docker run --rm -it --link=memsql:memsql memsql/quickstart simple-benchmark


# Open a MemSQL command line shell
#docker run --rm -it --link=memsql:memsql memsql/quickstart memsql-shell

# Stop and remove the container
#docker stop memsql && docker rm -v memsql
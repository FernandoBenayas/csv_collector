## CSV COLLECTOR
This docker image will download data from sdn_intelligence's elasticsearch and turn it into a csv.

Build it by going into the directory of the Dockerfile and executing
 
```python
sudo docker build -t name/oftheimage .
```

then create and run the container

```bash
sudo docker run --rm --name=nameofthecontainer name/oftheimage  -v path/to/the/csv/folder:/root/csv
```

and it will began instantly.


If you want to use docker-compose, execute in the directory with docker-compose.yml the following command:

```python
docker-compose up
```

But, BEWARE: change the paths in the docker-compose file.


Be advised: it will take a lot of time with a fairly big data size. 

## CSV COLLECTOR
This docker image will download data from elasticsearch and turn it into a csv, processing it.
First you need to import the data by copying the "data" folder in the testbed and pasting it in this
project (mind the permissions of the folder).

Then, build the project. Build it by going to the directory of the docker-compose and executing
 
```bash
docker-compose up
```

and it will began instantly. Then, enter the container,
 
 ```bash
docker exec -ti csv_collector bash
```

go to the scripts folder and execute the collector:

```bash
cd ~/script
python collector.py
```

After collecting and processing is done, the results are stored in the ~/csv folder.
## PLATER

PLATER is a service to stand up REST endpoint over a neo4j or redis graph database.
There are some restrictions on the data structure of the Neo4j backend to be fully utilized through PLATER.

> **Data structure restrictions:**
> * All nodes should have an `id` to be searchable (Used in querying single Nodes)
> * All edges should have an `id` to be searchable (Used in generating ReasonerAPI)
> * Data is recommended to comply to the [Translator Reasoner API](https://github.com/NCATSTranslator/ReasonerAPI).

### Installation

To run the web server directly:

#### Create a virtual Environment and activate.

    cd <PLATER-ROOT>
    python<version> -m venv venv
    source venv/bin/activate
    
#### Install dependencies
    
    pip install -r PLATER/requirements.txt
    
 
#### Configure PLATER settings
   
   Populate `.env-template` file with settings and save as `.env` in repo root dir.
   
   ```bash   
    WEB_HOST=0.0.0.0
    WEB_PORT=8080
    DB_HOST=<neo4j_or_redis_address>
    DB_USERNAME=<user name required for neo4j, empty for redis>
    DB_PASSWORD=''
    DB_PORT=<redis port or neo4j http port>
    DB_NAME=<db name for redis, uses default db for neo4j>
    DB_TYPE=<use values 'redis' or 'neo4j'>
    PLATER_TITLE='Plater'
    PLATER_VERSION='1.0.0'
    

   ```
   
  
#### Run Script
  
    ./main.sh
 
    
 ### DOCKER 
   Or build an image and run it. 
  
  ```bash
    cd PLATER
    docker build --tag <image_tag> .
    cd ../
  ```
  
  ```bash
   docker run --env-file .env\
    --name plater\
    -p 8080:8080\
    --network <network_where_neo4j_is_running>\
    plater-tst

  ```
 
 ### Clustering with [Automat Server](https://github.com/RENCI-AUTOMAT/Automat-server/) \[Optional\]
 You can also serve several instances of plater through a common gateway(Automat). On specific instructions 
 please refer to [AUTOMAT's readme](https://github.com/RENCI-AUTOMAT/Automat-server/blob/master/README.md)
  
 

 ### Miscellaneous
 ###### `/about` Endpoint 
 The `/about` endpoint can be used to present meta-data about the current PLATER instance. 
 This meta-data is served from `<repo-root>/PLATER/about.json` file. One can edit the contents of
 this file to suite needs. In containerized environment we recommend mounting this file as a volume.
 
 Eg:
 ```bash
docker run -p 0.0.0.0:8999:8080  \
               --env DB_HOST=<your_db_host> \
               --env DB_PORT=<your_db_port> \
               --env DB_USERNAME=<db_user_name required for neo4j, ignored for redis> \
               --env DB_PASSWORD=<db_password> \
               --env DB_NAME=<required for redis> \ 
               --env DB_TYPE=<redis or neo4j> \  
               --env WEB_HOST=0.0.0.0 \
               -v <your-custom-about>:/<path-to-plater-repo-home>/plater/about.json \
               --network=<docker_network_neo4j_is_running_at> \    
                <image_tag>   
``` 
 
   
    
    

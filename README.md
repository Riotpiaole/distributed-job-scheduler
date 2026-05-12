# Project Overview 


## Before you begin

- cli source dest plugin

## Source 
- Folder and filenames 

## POC

- Map text to token using bypart 
- Reduce token to vectordb format 
- Sink to vector DB

## Scaling Point 

- Worker main.go <numbers of work partition>
- Coordinator main.go <numbers of job>


## API 

- DataSource

- Map(mapf, **kwargs)
 - We can scale up worker up to size of RAM 

- Reduce(   reducef )

- SinkTo()

- On(...).Map(...)
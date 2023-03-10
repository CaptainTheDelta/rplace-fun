# rplace-fun
Having fun with the data of r/place 2022

## Project Milestones

- [x] Create a postgres database
- [ ] Initialize it
- [ ] Fill it with the official data
- [ ] Find a way to represent the data
- [ ] Find *amogus*
- [ ] Add a table with the less offficial dump
- [ ] Cross the data

## Create a postgress database

```bash
podman run -d --name rplace-db \
  -e POSTGRES_PASSWORD=kikou \
  -v rplace-db:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres
```


## Data sources
[Official](https://www.reddit.com/r/place/comments/txvk2d/rplace_datasets_april_fools_2022/) and a little bit [less official](https://www.reddit.com/r/place/comments/txh660/dump_of_the_raw_unprocessed_data_i_collected/)
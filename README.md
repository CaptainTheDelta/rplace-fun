# rplace-fun
Having fun with the data of r/place 2022

## Project Milestones

- [x] Create a postgres database
- [x] Initialize it
- [x] Fill it with the official data
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
## Setting up the project

On Windows, to avoid getting a linking error about `libpq.lib`, install PostgresQL, from [here](http://www.enterprisedb.com/downloads/postgres-postgresql-downloads) for instance, with only the command line tools. By default, libs are added to the `C:\Program Files\Postgres\15\lib` folder (might vary depending on your version and system).
Then, go to a terminal and type `setx PQ_LIB_DIR "C:\Program Files\Postgres\15\lib"` and close the shell. You might need to restart your computer, and add the folders `C:\Program Files\Postgres\15\lib` and `C:\Program Files\Postgres\15\bin` to your `PATH` environment variable.

On Linux, you will need to have the `libpq-devel` package installed (`libpq` is not enough).

Then, type (from [here](https://diesel.rs/guides/getting-started)) :
```
cargo install diesel_cli --no-default-features --features postgres
# to apply or redo migrations
diesel migration run
diesel migration redo
```

## Data sources
[Official](https://www.reddit.com/r/place/comments/txvk2d/rplace_datasets_april_fools_2022/) and a little bit [less official](https://www.reddit.com/r/place/comments/txh660/dump_of_the_raw_unprocessed_data_i_collected/)
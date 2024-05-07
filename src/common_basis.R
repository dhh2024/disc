library(here)
library(tidyverse)
library(hscidbutil)
library(keyring)
library(DBI)
library(RMariaDB)
library(googlesheets4)
library(yaml)
library(arrow)
library(gghsci)

con <- get_connection()

register_tables(con,"disc_orig")
register_tables(con,"disc")

d <- c(read_yaml(here("s3_secret.yaml")),read_yaml(here("s3_params.yaml")))
Sys.setenv(AWS_S3_ENDPOINT = d$endpoint_url %>% str_replace("^https?://","") %>% str_replace("/$",""))
Sys.setenv(AWS_ENDPOINT_URL = d$endpoint_url)
Sys.setenv(AWS_ACCESS_KEY_ID = d$access_key_id)
Sys.setenv(AWS_SECRET_ACCESS_KEY = d$secret_access_key)
rm(d)

#a3s <- s3_bucket("dhh24",endpoint_override="a3s.fi")

#a3s$ls("disc/parquet") %>% map(~read_parquet(a3s$path(.x)), as_data_frame = FALSE)

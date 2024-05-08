#!/usr/bin/env Rscript
library(here)
library(tidyverse)
library(googledrive)
library(googlesheets4)
library(optparse)
library(glue)

option_list <- list(
  make_option(c("-p", "--table-prefix"), type="character", default=NULL, 
              help="table prefix", metavar="character"),
  make_option(c("-s", "--sample-number"), type="character", default=NULL, 
              help="sample number", metavar="character")
)

opt_parser <- OptionParser(option_list=option_list)
opt <- parse_args(opt_parser)

table_prefix <- opt$`table-prefix`
sample_number <- opt$`sample-number`

submissions <- read_tsv(here(glue("data/work/samples/{table_prefix}_submissions_sample_{sample_number}.tsv"))) %>%
  mutate(permalink=gs4_formula(str_c('=HYPERLINK("',permalink,'","',permalink,'")')))
comments <- read_tsv(here(glue("data/work/samples/{table_prefix}_comments_sample_{sample_number}.tsv"))) %>%
  mutate(permalink=gs4_formula(str_c('=HYPERLINK("',permalink,'","',permalink,'")')))

sheet_name <- glue("{table_prefix}_sample_{sample_number}")
folder <- as_id("1MnayT571RYFxySvzdERbpf-sJGXJhIE5")
sheet_id <- drive_ls(folder, pattern=sheet_name) %>% 
  pull(id)

if (length(sheet_id) == 0) {
  sheet_id <- drive_create(sheet_name,path=folder, type="spreadsheet")
} else {
  sheet_id <- sheet_id[[1]]
}
submissions %>% sheet_write(ss=sheet_id, sheet="submissions")
comments %>% sheet_write(ss=sheet_id, sheet="comments")

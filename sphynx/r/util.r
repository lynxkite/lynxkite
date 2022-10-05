# Simple access to operation parameters, input, and outputs.
library(arrow)
library(bit64)
library(dplyr)
library(jsonlite)

typemap <- list(
  'Long' = 'LongAttribute',
  'Double' = 'DoubleAttribute',
  'String' = 'StringAttribute',
  'ndarray' = 'DoubleVectorAttribute'
)
castmap <- list(
  'Long' = as.integer64,
  'Double' = as.numeric,
  'String' = as.character,
  'Vector' = identity
)

args <- commandArgs(trailingOnly = TRUE)
datadir <- args[1]
op <- fromJSON(args[2])
params <- op[['Operation']][['Data']]
inputs <- op[['Inputs']]
outputs <- op[['Outputs']]

input_table <- function(name) {
  return(read_feather(file.path(datadir, inputs[[name]], 'data.arrow')))
}

output <- function(name, values, type) {
  values <- castmap[[type]](values)
  d <- file.path(datadir, outputs[[name]])
  dir.create(d)
  f <- file(file.path(d, 'type_name'))
  writeLines(typemap[[type]], f)
  close(f)
  write_feather(tibble(values=values), file.path(d, 'data.arrow'), compression="uncompressed")
  f <- file(file.path(d, '_SUCCESS'))
  writeLines('OK', f)
  close(f)
}

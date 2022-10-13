# Simple access to operation parameters, input, and outputs.
options(show.error.locations = TRUE)
options(error = function() {
    traceback(3)
    quit(save = "no", status = 1, runLast = FALSE)
})
suppressPackageStartupMessages(library(arrow))
suppressPackageStartupMessages(library(bit64))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(jsonlite))

typemap <- list(Long = "LongAttribute", Double = "DoubleAttribute", String = "StringAttribute",
    `Vector[Double]` = "DoubleVectorAttribute", VertexSet = "VertexSet", EdgeBundle = "EdgeBundle")
castmap <- list(Long = as.integer64, Double = as.numeric, String = as.character,
    `Vector[Double]` = function(c) {
        # Vectors must be stored as list columns for write_feather().
        if (typeof(c) == "list") c else as.list(as.data.frame(t(c)))
    }, VertexSet = as.integer64, EdgeBundle = function(df) {
        df %>%
            mutate(src = as.integer64(src - 1)) %>%
            mutate(dst = as.integer64(dst - 1)) %>%
            mutate(sparkId = as.integer64(sparkId))
    })

args <- commandArgs(trailingOnly = TRUE)
datadir <- args[1]
op <- fromJSON(args[2])
params <- op[["Operation"]][["Data"]]
inputs <- op[["Inputs"]]
outputs <- op[["Outputs"]]

input_edges <- function(name) {
    read_feather(file.path(datadir, inputs[[name]], "data.arrow")) %>%
        mutate(src = src + 1) %>%
        mutate(dst = dst + 1)
}

input_parquet <- function(name) {
    open_dataset(sources = file.path(datadir, inputs[[name]]), format = "parquet")
}
input_one_table <- function(name) {
    read_feather(file.path(datadir, inputs[[name]], "data.arrow"))
}
input_table <- function(name) {
    sapply(name, input_one_table, USE.NAMES = FALSE)
}

input_one_scalar <- function(name) {
    f <- file(file.path(datadir, inputs[[name]], "serialized_data"))
    j <- readLines(f)
    close(f)
    fromJSON(j)
}
input_scalar <- function(name) {
    sapply(name, input_one_scalar, USE.NAMES = FALSE)
}

output_parquet <- function(name, values) {
    print("output_parquet")
    print(name)
    print(values)
    d <- file.path(datadir, outputs[[name]])
    write_dataset(values, d)
}

output_one_table <- function(name, values, type) {
    values <- castmap[[type]](values)
    t <- if (is.data.frame(values))
        values else tibble(values = values)
    d <- file.path(datadir, outputs[[name]])
    dir.create(d)
    f <- file(file.path(d, "type_name"))
    writeLines(typemap[[type]], f)
    close(f)
    write_feather(t, file.path(d, "data.arrow"), compression = "uncompressed")
    f <- file(file.path(d, "_SUCCESS"))
    writeLines("OK", f)
    close(f)
}
output_table <- function(name, values, type) {
    for (i in 1:length(name)) {
        output_one_table(name[[i]], values[[i]], type[[i]])
    }
}

output_one_scalar <- function(name, value) {
    j <- toJSON(value, auto_unbox = TRUE)
    d <- file.path(datadir, outputs[[name]])
    dir.create(d)
    f <- file(file.path(d, "type_name"))
    writeLines("Scalar", f)
    close(f)
    f <- file(file.path(d, "serialized_data"))
    writeLines(j, f)
    close(f)
    f <- file(file.path(d, "_SUCCESS"))
    writeLines("OK", f)
    close(f)
}
output_scalar <- function(name, value) {
    for (i in 1:length(name)) {
        output_one_scalar(name[[i]], value[[i]])
    }
}

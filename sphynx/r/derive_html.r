# Run user code on a graph and produce HTML output.
source("r/util.r")

# Create input tables.
if (op[["Operation"]][["Class"]] == "com.lynxanalytics.biggraph.graph_operations.DeriveHTMLR") {
    vs <- gettable("vs")
    es <- gettable("es")
    if ("edges-for-es" %in% names(inputs)) {
        es <- cbind(es, input_edges("edges-for-es"))
    }
    graph_attributes <- getscalars()
} else if (op[["Operation"]][["Class"]] == "com.lynxanalytics.biggraph.graph_operations.DeriveHTMLTableR") {
    for (t in names(inputs)) {
        assign(t, input_parquet(t))
    }
}

if (params[["mode"]] == "plot") {
    svgfile <- tempfile()
    svg(svgfile)
}

# Run user code.
print("RUNNING USER CODE")  # For log cleanup.
code <- params[["code"]]
eval(parse(text = code))
print("USER CODE FINISHED")

# Save outputs.
if (params[["mode"]] == "html") {
    if (!exists("html")) {
        stop("Please save the output as 'html'.")
    }
} else if (params[["mode"]] == "plot") {
    library(openssl)
    dev.off()
    buf <- readBin(svgfile, raw(), file.info(svgfile)$size)
    data <- openssl::base64_encode(buf, linebreaks = FALSE)
    html <- paste("<img src=\"data:image/svg+xml;base64,", data, "\">")
}
output_scalar("sc", html)

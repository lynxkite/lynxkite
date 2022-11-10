# Run user code on a graph and produce HTML output.
source("r/util.r")

ip = as.data.frame(installed.packages()[,c(1,3:4)])
ip = ip[is.na(ip$Priority),1:2,drop=FALSE]
print("packages")
print(ip)

# Create input tables.
gettable <- function(parent) {
    fields <- params[["inputFields"]]
    special <- parent == "es" & (fields$name == "src" | fields$name == "dst")
    ns <- fields[fields$parent == parent & !special, ]$name
    if (length(ns) != 0) {
        columns <- input_table(paste(parent, ns, sep = "."))
        # .name_repair='minimal' allows duplicate column names. They are all
        # called 'values'.
        t <- tibble(!!!columns, .name_repair = "minimal")
        # We set the correct column names here.
        names(t) <- ns
        t
    }
}
vs <- gettable("vs")
es <- gettable("es")
if ("edges-for-es" %in% names(inputs)) {
    es <- cbind(es, input_edges("edges-for-es"))
}

getscalars <- function() {
    fields <- params[["inputFields"]]
    ns <- fields[fields$parent == "graph_attributes", ]$name
    if (length(ns) != 0) {
        values <- input_scalar(paste("graph_attributes", ns, sep = "."))
        as.list(setNames(values, ns))
    }
}
graph_attributes <- getscalars()

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

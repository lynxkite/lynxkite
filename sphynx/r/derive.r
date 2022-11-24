# Runs user code that takes a graph as input and outputs new attributes.
source("r/util.r")

ip = as.data.frame(installed.packages()[,c(1,3:4)])
ip = ip[is.na(ip$Priority),1:2,drop=FALSE]

# Create input tables.
vs <- gettable("vs")
es <- gettable("es")
if ("edges-for-es" %in% names(inputs)) {
    es <- cbind(es, input_edges("edges-for-es"))
}
graph_attributes <- getscalars()

# Run user code.
print("RUNNING USER CODE")  # For log cleanup.
code <- params[["code"]]
eval(parse(text = code))
print("USER CODE FINISHED")

# Save outputs.
save <- function(parent, t) {
    fields <- params[["outputFields"]]
    fs <- fields[fields$parent == parent, ]
    if (nrow(fs) != 0) {
        columns <- t[fs$name]
        output_table(paste(parent, fs$name, sep = "."), columns, fs$tpe$typename)
    }
}
save("vs", vs)
save("es", es)
savescalars <- function() {
    fields <- params[["outputFields"]]
    fs <- fields[fields$parent == "graph_attributes", ]
    if (nrow(fs) != 0) {
        values <- graph_attributes[fs$name]
        output_scalar(paste("graph_attributes", fs$name, sep = "."), values)
    }
}
savescalars()

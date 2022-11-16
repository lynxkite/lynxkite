# Runs user code that takes a table as input and outputs a table.
source("r/util.r")

# Create input tables.
for (t in names(inputs)) {
    assign(t, input_parquet(t))
}

# Run user code.
print("RUNNING USER CODE")  # For log cleanup.
code <- params[["code"]]
eval(parse(text = code))
print("USER CODE FINISHED")

# Save outputs.
print(params[["outputFields"]])
for (t in names(outputs)) {
    print(t)
    fields <- params[["outputFields"]]
    fs <- fields[fields$parent == t, ]
    print(fs$name)
    output_parquet(t, get(t)[fs$name])
}

# Run user code on a graph.
source('r/util.r')

# Create input tables.
gettable <- function(parent) {
  fields <- params[['inputFields']]
  ns <- fields[fields$parent == parent, ]$name
  if (length(names) != 0) {
    columns <- input_table(paste(parent, ns, sep='.'))
    # .name_repair='minimal' allows duplicate column names. They are all called "values".
    t <- tibble(!!!columns, .name_repair='minimal')
    # We set the correct column names here.
    names(t) <- ns
  }
  return(t)
}
vs <- gettable('vs')
es <- gettable('es')

# Run user code.
print('RUNNING USER CODE') # For log cleanup.
code <- params[['code']]
eval(parse(text = code))

# Save outputs.
save <- function(parent, t) {
  fields <- params[['outputFields']]
  fs <- fields[fields$parent == parent, ]
  if (length(fs) != 0) {
    columns <- t[[fs$name]]
    output(paste(parent, fs$name, sep='.'), columns, fs$tpe$typename)
  }
}
save('vs', vs)
save('es', es)

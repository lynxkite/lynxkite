# Run user code on a graph.
source('r/util.r')

# Create input tables.
gettable <- function(parent) {
  fields <- params[['inputFields']]
  special <- parent == 'es' & (fields$name == 'src' | fields$name == 'dst')
  ns <- fields[fields$parent == parent & !special, ]$name
  if (length(ns) != 0) {
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
if ('edges-for-es' %in% names(inputs)) {
  es <- cbind(es, input_edges('edges-for-es'))
}

getscalars <- function() {
  fields <- params[['inputFields']]
  ns <- fields[fields$parent == 'graph_attributes', ]$name
  values <- input_scalar(paste('graph_attributes', ns, sep='.'))
  return(as.list(setNames(values, ns)))
}
graph_attributes <- getscalars()

# Run user code.
print('RUNNING USER CODE') # For log cleanup.
code <- params[['code']]
eval(parse(text = code))
print('USER CODE FINISHED')

# Save outputs.
save <- function(parent, t) {
  fields <- params[['outputFields']]
  fs <- fields[fields$parent == parent, ]
  if (length(fs) != 0) {
    # TODO: Good error message if output is missing.
    columns <- t[fs$name]
    output_table(paste(parent, fs$name, sep='.'), columns, fs$tpe$typename)
  }
}
save('vs', vs)
save('es', es)
savescalars <- function() {
  fields <- params[['outputFields']]
  fs <- fields[fields$parent == 'graph_attributes', ]
  if (length(fs) != 0) {
    # TODO: Good error message if output is missing.
    values <- graph_attributes[fs$name]
    output_scalar(paste('graph_attributes', fs$name, sep='.'), values)
  }
}
savescalars()

# Run user code on a graph.
source('r/util.r')

vs <- tibble()
es <- tibble()
graph_attributes <- list()

# Run user code.
print('RUNNING USER CODE') # For log cleanup.
code <- params[['code']]
eval(parse(text = code))
print('USER CODE FINISHED')

# Save outputs.
save <- function(parent, t) {
  fields <- params[['outputFields']]
  fs <- fields[fields$parent == parent, ]
  if (nrow(fs) != 0) {
    # TODO: Good error message if output is missing.
    columns <- t[fs$name]
    output_table(paste(parent, fs$name, sep='.'), columns, fs$tpe$typename)
  }
}
# We generate 0-based sparkIds to match Python behavior.
output_table('vertices', tibble(sparkId = 1:nrow(vs) - 1), 'VertexSet')
output_table('edges-idSet', tibble(sparkId = 1:nrow(es) - 1), 'VertexSet')
output_one_table('edges', es[c('src', 'dst')] %>% mutate(sparkId = 1:nrow(es) - 1), 'EdgeBundle')
save('vs', vs)
save('es', es)
savescalars <- function() {
  fields <- params[['outputFields']]
  fs <- fields[fields$parent == 'graph_attributes', ]
  if (nrow(fs) != 0) {
    # TODO: Good error message if output is missing.
    values <- graph_attributes[fs$name]
    output_scalar(paste('graph_attributes', fs$name, sep='.'), values)
  }
}
savescalars()

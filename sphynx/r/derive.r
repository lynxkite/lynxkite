# Run user code.
library('stringr')
library('tibble')
library('dplyr')
source('r/util.r')

# Create input tables.
fields <- params[['inputFields']]
ns <- fields[fields$parent == 'vs']$name
if (length(names) != 0) {
  columns <- input_table(paste('vs', ns, sep='.'))
  vs <- tibble(!!!columns, .name_repair='minimal')
  names(vs) <- ns
}

# Run user code.
code <- params[['code']]
eval(parse(text = code))
print('RESULT:')
print(vs)

# Save outputs.
fields <- params[['outputFields']]
fs <- fields[fields$parent == 'vs']
if (length(fs) != 0) {
  columns <- vs[[fs$name]]
  print('OUTPUT')
  output(paste('vs', fs$name, sep='.'), columns, fs$tpe$typename)
}

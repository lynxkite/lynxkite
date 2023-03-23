'''Prints the full licenses of all NPM runtime dependencies.'''
import subprocess
import json

p = subprocess.run('npx license-checker --production --json',
                   shell=True, capture_output=True, check=True)
j = json.loads(p.stdout)
for name, v in j.items():
  url = v.get('repository', '<could not find url>')
  if 'licenseFile' in v:
    with open(v['licenseFile']) as f:
      license = f.read()
  else:
    license = '<could not find license file>'
  print(f'''
-----
The following software may be included in this product: {name}
A copy of the source code may be downloaded from {url}
This software contains the following license and notice below:
{license}
  '''.strip())

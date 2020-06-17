Updating the demo site
======================

New workspaces can be added to the Tutorials folder in LynxKite. To add them to the docker image,
you need to do the following.

1. Go to [pizzabox](https://pizzabox.lynxanalytics.com) (or wherever you're importing the workspace from)
   and open the workspace. Select the workspace with the select tool, copy it to the clipboard, and then
   save it in the `workspaces` directory as a `yaml`-file, e.g., `NewWorkspace.yaml`

1. Still on [pizzabox](https://pizzabox.lynxanalytics.com), look at all the import operations in the
   workspace and find their `File` parameter, for example `UPLOAD$/f317f40169a4158dbd765d9d282a4b0e.sok-relationships-filtered.csv`.
   You need to retrieve all such imported files (in the case of pizzabox, they are
   in directory `/home/kite/kite/data/uploads`) and copy them to `home/.kite/data/uploads`.

1. Update the script `scripts/upload_workspaces.sh` so that it uploads `NewWorkspace.yaml` as well.

1. Run `./build.sh`.

1. Run `./testrun.sh`. This will start a local LynxKite instance at the usual `http://localhost:2200` address
   that has access to your new workspace. Fix the import errors in the workspace by computing a new Table GUID
   (click on the "Run import" button at the bottom). In the end, all import operations should be green, and the
   rest of the operations should be blue. Stop `./testrun.sh`

1. Now, on your local machine, select the workspace again and copy it to the clipboard. Then, overwrite
   `workspaces/NewWorkspace.yaml` with this file.

1. Update the contents of the directory `preloaded_lk_data`:

       rm -rf preloaded_lk_data
       ./preload_tool.py --task=copy --ws_dir workspaces --preloaded_dir preloaded_lk_data --source_dir home/.kite

1. Run `./build.sh`

1. Delete (or rename directory home/.kite)

       rm -rf home/.kite

1. Run `./testrun.sh`. Open the workspace again, and check that the workspace has no errors this time. If
   all is fine, stop `./testrun.sh`

1. Run `./push.sh` to push the changes to the demo site.

1. Make a tgz file of your new preloaded_lk_data directory and upload it to S3:

       tar czf preloaded_lk_data.tgz preloaded_lk_data/*
       aws s3 cp preloaded_lk_data.tgz s3://preloaded-lk-data

## LynxKite setup using a Docker container

**Note:** This is an experimental setup. Some things may not work.

The following instructions describe how to set up LynxKite in a Docker container.

Installing the required LynxKite dependencies is intrusive (e.g., `sudo pip ...`) and may interfere with the package management system of a distribution.
To prevent this potential interference, the dependencies can be installed in a Docker container that is isolated from the host system.
A directory containing the LynxKite repository is mounted into the container and LynxKite can be compiled, tested, and ran inside the container. Editing LynxKite source code can be done as usual, because repository is stored on the host file system and not in the container.

Start by cloning the LynxKite repository. For these instructions we assume that the repository is stored in `/home/user/biggraph` directory.

Install Docker as described [here](https://docs.docker.com/install/linux/docker-ce/ubuntu/#install-docker-ce).

Now pull the official Ubuntu 16.04 Docker image (this will be the base image for our Docker container in which we will install the rest of the dependencies):

```
sudo docker pull ubuntu:16.04
```

Run the Ubuntu image with the LynxKite repository mounted into the `/mnt` directory of the container. Port 2200 of the container will be forwarded to port 2200 of the host system, so LynxKite can be accessed outside of the container under `http://localhost:2200`:

```
sudo docker run -it -p 2200:2200  --mount type=bind,source=/home/user/biggraph,target=/mnt ubuntu:16.04
```

Inside the container, install some required tools:

```
apt install sudo, wget, nano
```

Create a user (e.g., `c-user`) under which we will compile and run LynxKite inside the container.

```
adduser c-user
```

Add the user to the `sudo` group:

```
adduser c-user sudo
```

Switch to the newly created user:

```
su c-user
```

Now, staying inside the container, proceed with setting up the LynxKite environment as described under "Global setup steps", "Per repository setup", and "Configure .kiterc" in [README.md](README.md).

You can test if everything went well by running LynxKite inside the container and trying to access it from a browser outside of the container under `http://localhost:2200`.

If everything worked well exit the container (by hitting Control-D two times or typing `exit` two times).

At this point, we have a stopped Docker container with all LynxKite dependencies.
Now we will create a Docker image from this container, which will effectively freeze the current state of the container, so that we can start from this point any time we want.

First, we will need the ID of the container. For this, run

```
sudo docker container ls --all
```

A list of all containers on the system will be displayed. Note the CONTAINER_ID of the most recently created container (see column CREATED).

Now create an image named `lk-dev` (replace CONTAINER_ID with the actual ID that you noted):

```
sudo docker container commit -m 'Ubuntu 16.04 with LynxKite dependencies' CONTAINER_ID lk-dev
```

Run the image with

```
sudo docker run --rm -it -p 2200:2200 -u c-user -w /mnt --mount type=bind,source=/home/user/biggraph,target=/mnt lk-dev
```

`--rm` instructs Docker to destroy the container on exit; a soon as we not installed any new LynxKite dependencies, we don't need this container anyway. `-u c-user` instructs Docker to start the container with user `c-user` and not `root`. `-w /mnt` instructs Docker to change into `/mnt` directory (in which we mount the LynxKite repository).

After running the image, LynxKite can be compiled, tested, and ran inside the container as usual (see [README.md](README.md)).

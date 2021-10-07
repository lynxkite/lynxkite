if [[ ! -e include ]]; then ln -s $CONDA_PREFIX/include .; fi
#export CPLUS_INCLUDE_PATH="$(pwd)/include"
#export LD_LIBRARY_PATH="$(pwd)"
#export LIBRARY_PATH="$(pwd)"
export CGO_CXXFLAGS="-Wno-deprecated-declarations"
export CGO_LDFLAGS="-lnetworkit -fopenmp"

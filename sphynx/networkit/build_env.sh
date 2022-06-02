rm -f include
ln -s $CONDA_PREFIX/include .
export CGO_CXXFLAGS="-Wno-deprecated-declarations"
export CGO_LDFLAGS="-lnetworkit -fopenmp"

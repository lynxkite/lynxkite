#!/bin/bash -xue
make backend
cd web
npx gulp serve

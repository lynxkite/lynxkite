#!/bin/bash -xu

kubectl --namespace cloud-lk delete deployment --all
kubectl --namespace cloud-lk delete job --all
kubectl --namespace cloud-lk delete pod --all
kubectl --namespace cloud-lk delete pvc --all
kubectl --namespace cloud-lk delete pv --all

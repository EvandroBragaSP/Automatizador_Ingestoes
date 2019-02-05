#!/bin/bash
####################################################################################
# Este script e responsavel por executar o automatizador de ingestoes.
#
# TENBU
####################################################################################
result=$(/etc/anaconda3/bin/python3 ../python/main.py)
if [ "$result" = "0" ]; then
    echo "Processo executado com sucesso."
    exit 0
else
    echo $result

    exit 1
fi
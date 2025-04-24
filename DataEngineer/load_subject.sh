#!/bin/bash
# LOAD FILE RESOUCE

wget https://cdn.intra.42.fr/document/document/23498/subject.zip -O subject.zip

unzip subject.zip

cp ./subject/customer/data_2022_dec.csv ./ex02/customer/
cp ./subject/customer/* ./ex03/customer/
cp ./subject/customer/* ./ex04/customer/
cp ./subject/item/* ./ex04/items/items.csv


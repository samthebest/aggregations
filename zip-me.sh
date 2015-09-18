#!/bin/bash
project=aggregations
rm -rf ../$project.bak
cp -r . ../$project.bak
rm -r ../$project.bak/target ../$project.bak/project/target ../$project.bak/.idea* ../$project.bak/project/project/target
zip -r ../$project.bak.zip ../$project.bak

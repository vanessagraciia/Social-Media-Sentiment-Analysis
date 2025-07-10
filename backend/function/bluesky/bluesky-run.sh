#!/bin/bash

# Execute the script
# deploy function as spec
fission spec init
fission spec apply --specdir ./specs --wait

#Run test
#Run to harvest newest post today
fission fn test --name bluesky-new-harvester

#Run to harvest next day of historical post 
fission fn test --name bluesky-new-harvester
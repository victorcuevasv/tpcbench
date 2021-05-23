#!/bin/bash   

aws emr --region us-west-2 \
start-notebook-execution \
--editor-id e-EYUEUFEDO1VW655D6L6LNS6KN \
--relative-path TPCDSLoadTableJarTest.ipynb \
--notebook-execution-name my-execution-TPCDSLoadTableJarTest \
--execution-engine '{"Id" : "j-13XAZUVSV5EK5"}' \
--service-role EMR_Notebooks_DefaultRole


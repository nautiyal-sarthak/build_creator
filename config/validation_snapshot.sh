#!/bin/bash

function logger {
   if [ ${1} -gt 0 ];
	then
	echo "`date +"%Y-%m-%d %H:%M:%S"` : ERROR : ${2} failed. Please check the log..!!! " 
	exit 1
   else
	echo "`date +"%Y-%m-%d %H:%M:%S"` : ${2} Successful...." 
   fi
}

<validationFunc>

script_name=$(basename "$0" .sh)
log=${BUILD_DIR}/Deploy_${script_name}.log
>${BUILD_DIR}/Deploy_${script_name}.log


cd /extfmewk/
logger 0 "Delete started" | tee -a ${log}
<WF_DELETE>
logger 0 "Delete completed" | tee -a ${log}
logger 0 "Create started" | tee -a ${log}
<WF_CREATE>
logger 0 "Create completed" | tee -a ${log}
logger 0 "Deploy started" | tee -a ${log}
<WF_DEPLOY>
logger 0 "Deploy completed" | tee -a ${log}

cd /temp/
logger 0 "Execution engine change started" | tee -a ${log}
<MR_CHANGE>
logger 0 "Execution engine change completed" | tee -a ${log}

<OLA>
  

<VIEW>


cd /scripts/
<validationFuncCall>

while kill -0 <while_chk> 2>/dev/null
do
sleep 300
done

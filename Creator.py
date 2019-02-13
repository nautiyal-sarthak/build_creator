import os
from os import listdir
from os.path import isfile, join



buildPath = 'Path of the bild'

sys1_sys = ["ABC","XYZ"]

validationFunc = """f_<SRC_L>_<CC_L>()
{
sh aml_global_wf_wrapper.sh <SRC_L>prd <CC_L> $1 <SRC_L> prd
ret_status=${?}
logger ${ret_status} "aml_global_wf_wrapper.sh <SRC_L>prd <CC_L> $1 <SRC_L> prd" | tee -a ${log}
sh aml_global_hdfs2nas.sh <SRC_L>prd <CC_L> $1 <SRC_L> prd
ret_status=${?}
logger ${ret_status} "aml_global_hdfs2nas.sh <SRC_L>prd <CC_L> $1 <SRC_L> prd" | tee -a ${log}
./aml_sftp_edm2mantas.sh <SRC_L> prd <CC_L> $1 /aml/batch/download/<CC_U>/<SRC_U>
 ret_status=${?}
logger ${ret_status} "aml_sftp_edm2mantas.sh <SRC_L> prd <CC_L> $1 /aml/batch/download/<CC_U>/<SRC_U>" | tee -a ${log}
}"""

mr_change = """rm prd_aml_<SRC_L>_<CC_L>_*.hql
<MR_GET>
sed -i "1i set hive.execution.engine=mr;" prd_aml_<SRC_L>_<CC_L>_*.hql 
<MR_PUT>
rm prd_aml_<SRC_L>_<CC_L>_*.hql
"""

view_deploy = """hive -S -f "/<SRC_U>/processing/config/<VIEW>"
ret_status=${?}
logger ${ret_status} "Executing, /<SRC_U>/processing/config/<VIEW>" | tee -a ${log}
"""

get_partition = """logger 0 "replacing the getPartitionInfoForBusinessDay started" | tee -a ${log}
<GETPARTITION>
logger 0 "replacing the getPartitionInfoForBusinessDay completed" | tee -a ${log}"""

get_partition_txt = """hadoop fs -rm /prd/edm/hadoop/sys/workflows/<WF>/getPartitionInfoForBusinessDay.sh
hadoop fs -put /EBBS/appl/scripts/getPartitionInfoForBusinessDay.sh /prd/edm/hadoop/sys/workflows/<WF>/
"""


# this class will hold all the details of the workflows
# and the view for a specific SRC-CTRY
class Deployer(object):
    def __init__(self):
        self._country = None

    def __init__(self):
        self._src = None

    def __init__(self):
        self._wf = None

    def __init__(self):
        self._view = None


    @property
    def country(self):
        return self._country

    @country.setter
    def country(self, value):
        self._country = value

    @property
    def src(self):
        return self._src

    @src.setter
    def src(self, value):
        self._src = value

    @property
    def wf(self):
        return self._wf

    @wf.setter
    def wf(self, value):
        self._wf = value

    @property
    def view(self):
        return self._view

    @view.setter
    def view(self, value):
        self._view = value


# This will list out all the files in the build dir
def get_changed_files(buildPath):
    out = []
    for root, dirnames, filenames in os.walk(buildPath):
        for filename in filenames:
            path = os.path.join(root, filename)
            out.append(path.replace(buildPath, ""))
    return out


# This function will create the build.txt
def CreateCopyBackup(lst):
    backup = []
    copy = []
    for file in lst:
        file = file.replace("\\","/")
        backup.append("mv " + "/" + file + " /" + file + "_<dt>")
        foldername = "/".join(file.split("/")[:-1])
        copy.append("cp ${BUILD_DIR}/" + foldername + "/*.* /" + foldername + "/")

    copylst = list(set(copy))

    CopyBackup = open("./output/build_steps.txt", "w+")
    CopyBackup.write("\n".join(backup) + "\n\n")
    CopyBackup.write("\n".join(copylst))
    CopyBackup.write("\n")


# This function will create the build.txt
def CreateRollback(lst):
    remove = []
    copy = []
    for file in lst:
        file = file.replace("\\","/")
        copy.append("cp " + "/" + file + "_<dt>" + " /" + file)
        remove.append("rm /" + file)


    CopyBackup = open("./output/rollback_steps.txt", "w+")
    CopyBackup.write("\n".join(remove) + "\n\n")
    CopyBackup.write("\n".join(copy))


# this function will populate the developer object
def CreateValidationObj(buildPath):
    lstObj = []
    for src in os.listdir(buildPath + "CTRLFW/PSAIL"):
        if src != "common":
            mypath = buildPath + "CTRLFW/PSAIL/" + src + "/processing/config"
            if(os.path.exists(mypath)):
                onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
                countries = list(set([x.split(src.lower() + "_")[1].split("_")[0] for x in onlyfiles]))
                for cc in countries:
                    obj = Deployer()
                    obj.src = src
                    obj.country = cc
                    wflst = []
                    viewlst = []
                    for file in [files for files in onlyfiles if (cc in files)]:
                        if file.endswith(".xml"):
                            wflst.append(file)

                        if file.endswith("_vw.hql"):
                            viewlst.append(file)

                    obj.view = viewlst
                    obj.wf = wflst
                    lstObj.append(obj)
    return lstObj


# this function will create the deploy validation file
def CreateValidationFiles(objlst):
    validationFunclst_sys2 = []
    func_call_sys2 = []
    while_lst_sys2 = []
    deletelst_sys2 = []
    createlst_sys2 = []
    deploylst_sys2 = []
    mrlst_sys2 = []
    viewlst_sys2 = []

    validationFunclst_sys1 = []
    func_call_sys1 = []
    while_lst_sys1 = []
    deletelst_sys1 = []
    createlst_sys1 = []
    deploylst_sys1 = []
    mrlst_sys1 = []
    viewlst_sys1 = []
    get_partition_lst = []



    for obj in objlst:
        mrGetlst_sys2 = []
        mrPutlst_sys2 = []

        mrGetlst_sys1 = []
        mrPutlst_sys1 = []

        src = obj.src
        cc = obj.country

        src_u = src.upper()
        src_l = src.lower()

        cc_u = cc.upper()
        cc_l = cc.lower()

        if str(obj.src).upper() not in sys1_sys:

            validationFunclst_sys2.append(validationFunc.replace("<SRC_L>",src_l).replace("<SRC_U>",src_u).\
                replace("<CC_L>",cc_l).replace("<CC_U>",cc_u))
            for wf in obj.wf:
                wf = wf.replace(".xml","")
                deletelst_sys2.append("./client.sh config -entity workflow -delete " + wf)
                createlst_sys2.append("./client.sh config -entity workflow -create /" + src_u +"/processing/config/" + wf + ".xml")
                deploylst_sys2.append("./client.sh execution -deploy --workflow " + wf + " --write_files")
                mrGetlst_sys2.append("hadoop fs -get /prd/edm/hadoop/sys/workflows010/" + wf + "/" + wf + "-src.hql")
                mrPutlst_sys2.append("hadoop fs -put -f /temp/" + wf + "-src.hql /prd/edm/hadoop/sys/workflows007/" + wf + "/")

            for view in obj.view:
                viewlst_sys2.append(view_deploy.replace("<SRC_U>",src_u).replace("<VIEW>",view))

            func_call_sys2.append("f_"+src_l+"_" + cc_l + " $1 &\np_" + src_l + "_" + cc_l+" =${!}")
            while_lst_sys2.append("$p_" + src_l + "_" + cc_l)
            mrlst_sys2.append(mr_change.replace("<SRC_L>",src_l).replace("<CC_L>",cc_l).
                         replace("<MR_GET>","\n".join(mrGetlst_sys2)).replace("<MR_PUT>","\n".join(mrPutlst_sys2)))
        else:
            validationFunclst_sys1.append(validationFunc.replace("<SRC_L>", src_l).replace("<SRC_U>", src_u). \
                                          replace("<CC_L>", cc_l).replace("<CC_U>", cc_u))
            for wf in obj.wf:
                wf = wf.replace(".xml", "")
                deletelst_sys1.append("./client.sh config -entity workflow -delete " + wf)
                createlst_sys1.append(
                    "./client.sh config -entity workflow -create /" + src_u + "/processing/config/" + wf + ".xml")
                deploylst_sys1.append("./client.sh execution -deploy --workflow " + wf + " --write_files")
                mrGetlst_sys1.append(
                    "hadoop fs -get /prd/edm/hadoop/sys/workflows/" + wf + "/" + wf + "-src.hql")
                mrPutlst_sys1.append(
                    "hadoop fs -put -f /temp/" + wf + "-src.hql /prd/edm/hadoop/sys/workflows/" + wf + "/")

                if src_u == "EBBS" and "master" in str(wf).lower() :
                    get_partition_lst.append(get_partition_txt.replace("<WF>",wf))

            for view in obj.view:
                viewlst_sys1.append(view_deploy.replace("<SRC_U>", src_u).replace("<VIEW>", view))

            func_call_sys1.append("f_" + src_l + "_" + cc_l + " $1 &\np_" + src_l + "_" + cc_l + "=${!}")
            while_lst_sys1.append("$p_" + src_l + "_" + cc_l)
            mrlst_sys1.append(mr_change.replace("<SRC_L>", src_l).replace("<CC_L>", cc_l).
                              replace("<MR_GET>", "\n".join(mrGetlst_sys1)).replace("<MR_PUT>",
                                                                                    "\n".join(mrPutlst_sys1)))


    with open("./config/validation_sys2_snapshot.sh") as f:
        sys2Text = f.read().replace('<validationFunc>', "\n\n".join(validationFunclst_sys2)).\
            replace('<validationFuncCall>', "\n".join(func_call_sys2)).\
            replace("<while_chk>"," ".join(while_lst_sys2)).\
            replace("<WF_DELETE>","\n".join(deletelst_sys2)).\
            replace("<WF_CREATE>","\n".join(createlst_sys2)).\
            replace("<WF_DEPLOY>","\n".join(deploylst_sys2)).replace("<MR_CHANGE>","\n".join(mrlst_sys2)).\
            replace("<VIEW>","\n\n".join(viewlst_sys2))

    with open("./config/validation_sys1_snapshot.sh") as f:
        sys1Text = f.read().replace('<validationFunc>', "\n\n".join(validationFunclst_sys1)).\
            replace('<validationFuncCall>', "\n".join(func_call_sys1)).\
            replace("<while_chk>"," ".join(while_lst_sys1)).\
            replace("<WF_DELETE>","\n".join(deletelst_sys1)).\
            replace("<WF_CREATE>","\n".join(createlst_sys1)).\
            replace("<WF_DEPLOY>","\n".join(deploylst_sys1)).replace("<MR_CHANGE>","\n".join(mrlst_sys1)).\
            replace("<VIEW>","\n\n".join(viewlst_sys1)).\
            replace("<GETPARTITION>",get_partition.replace("<GETPARTITION>","\n".join(get_partition_lst)))



    with open("./output/deploy_validation_sys2.sh", "w") as f:
        f.write(sys2Text)

    with open("./output/deploy_validation_sys1.sh", "w") as f:
        f.write(sys1Text)




if __name__ == '__main__':
    if os.path.exists("./output/build_steps.txt"):
        os.remove("./output/build_steps.txt")

    if os.path.exists("./output/deploy_validation_sys2.sh"):
        os.remove("./output/deploy_validation_sys2.sh")

    if os.path.exists("./output/deploy_validation_sys1.sh"):
        os.remove("./output/deploy_validation_sys1.sh")

    # create the build_steps.txt
    CreateCopyBackup(get_changed_files(buildPath))

    # create the build_steps.txt
    CreateRollback(get_changed_files(buildPath))

    # create the deploy_validation.sh
    CreateValidationFiles(CreateValidationObj(buildPath))

#####################################
# Call all of the Get Author scripts.
#####################################
import os
import ExecScripts as CMD
import MyEnvVars as MyEnv
from datetime import datetime, timedelta

wrksp = MyEnv.workspace # where to run from
dirOut = os.path.join(wrksp, "Data")
topic = MyEnv.TOPIC

def main():
    scriptDir = MyEnv.workspace

    args = []

    start_time = datetime.now()

    """
    This deletes any previous data files for each step, then runs the script for each step.

    Comment out any sections already completed if you need to re-run.
    """

    ### 01_GetAuthorList
    fileName = os.path.join(dirOut, "GS_" + topic + "_Authors_" + str(MyEnv.N_AUTHORS) + ".csv")
    if os.path.exists(fileName):
        os.remove(fileName)

    CMD.execfile("01_GetAuthorList.py", scriptDir, args)
    end_time = datetime.now()
    print("*** Finished Tourism 1 in: ", str(end_time - start_time))

    ### 02_GetAuthorData
    fileName = os.path.join(dirOut, "GS_" + topic + "_AuthorData_" + str(MyEnv.N_AUTHORS) + ".csv")
    if os.path.exists(fileName):
        os.remove(fileName)

    CMD.execfile("02_GetAuthorData.py", scriptDir, args)
    end_time = datetime.now()
    print("*** Finished Tourism 2", str(end_time - start_time))
    
    ### 03_GetPublications
    # have to clear Publication previous file if it exists.
    fileName = os.path.join(dirOut, "GS_" + topic + "_Publications_" + str(MyEnv.N_AUTHORS) + ".csv")
    if os.path.exists(fileName):
        os.remove(fileName)

    CMD.execfile("03_GetPublications.py", scriptDir, args)
    end_time = datetime.now()
    print("*** Finished Tourism 3", str(end_time - start_time))

    ### 04_GetPubAuthors
    fileName = os.path.join(dirOut, "GS_" + topic + "_PubAuthors_" + str(MyEnv.N_AUTHORS) + ".csv")
    if os.path.exists(fileName):
        os.remove(fileName)  

    CMD.execfile("04_GetPubAuthors.py", scriptDir, args)
    end_time = datetime.now()
    print("*** Finished Tourism 4", str(end_time - start_time))

    fileName = os.path.join(dirOut, "GS_" + topic + "_AuthAlias_" + str(MyEnv.N_AUTHORS) + ".csv")
    if os.path.exists(fileName):
        os.remove(fileName)  
        
    ### 05_GetAuthorAlias
    CMD.execfile("05_GetAuthorAlias.py", scriptDir, args)
    end_time = datetime.now()
    print("*** Finished Tourism 5", str(end_time - start_time))

    ### 06_SetPubAuth_Status
    CMD.execfile("06_SetPubAuth_Status.py", scriptDir, args)

    end_time = datetime.now()
    print("*** Finished Tourism 6", str(end_time - start_time))

    ### 08_BuildNodesAndEdges
    CMD.execfile("08_BuildNodesAndEdges.py", scriptDir, args)
    end_time = datetime.now()
    print("*** Finished Tourism 7", str(end_time - start_time))
    
    
    print("****************************************")
    print("****************************************")
    print("****************************************")
    print("*** Finished")
# end main

# call main
if __name__=="__main__":
    main()

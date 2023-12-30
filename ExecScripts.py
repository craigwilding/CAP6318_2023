import os
import sys
import subprocess
from subprocess import Popen, PIPE


##################################################
# Run Test/Train Process
##################################################

inWrkDir = os.getcwd()
print(inWrkDir)


def execfile(fileName, scriptDir, args) :
    os.chdir(scriptDir)
    path = os.path.join(scriptDir, fileName)
    print("executing: " + path)
    #exec(open(fileName).read())
    #subprocess.call(fileName, shell=True)
    #subprocess.run(fileName, shell=True)
    cmd = ['python', path]
    cmd += args
    print(cmd)
    #cmd = ['python', path, wrkdir]
    subprocess.Popen(cmd).wait()
    #subprocess.Popen(cmd, stdout=PIPE).wait()
# end execfile



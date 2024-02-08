from datetime import datetime
import sys
import subprocess
from subprocess import PIPE

def exec_command(command):
    """ Execute the command and return the exit status. """
    print(f'{datetime.now().strftime("%Y%m%d_%H%M%S")}')
    print('Running system command: \n {0} \n'.format(''.join(command)))
    pobj = subprocess.Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
    stdo, stde = pobj.communicate()
    exit_code = pobj.returncode
    return exit_code, stdo, stde


for i in range(1, 97):
	artifactory = f"https://artifactory.com/artifactory/validation_{i}.sh"
	formed_command = f'curl --user {username}:{psswd} -X DELETE "{artifactory}"'
	exec_command(formed_command)

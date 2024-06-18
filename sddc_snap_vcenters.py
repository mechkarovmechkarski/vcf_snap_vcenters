import os
import time
import sys
import requests
import getpass
import subprocess
import re
import random, string
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from typing import List, Dict
from datetime import datetime


requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
DEBUG = False
DATE = "_".join(str(datetime.now()).split(" "))
letters = string.ascii_letters
SNAP_NAME_EXTENSION = ''.join(random.choice(letters) for i in range(18))
SNAP_NAME = str(DATE) + "_" + SNAP_NAME_EXTENSION
YAML_FILES_CREATED = []
SDDC_WORKING_VERSIONS = [""]


def main() -> None:
    vcenter_vmnames = get_vcenter_hostnames_vmnames()
    if DEBUG: print("vcenter vm names: ", vcenter_vmnames)

    mngmnt_domain_name = get_mngmnt_domain_name()
    if DEBUG: print("domain name: ", mngmnt_domain_name)

    vcenter_token = get_vcenter_token()
    if DEBUG: print(f"token: '{vcenter_token}'")

    hosts_ids_hostnames = get_hosts_identifiers(vcenter_token) 
    if DEBUG: print("hosts - ids and hostnames: ", hosts_ids_hostnames)

    hostname_vmnames = locate_vcenter_vms(vcenter_vmnames, hosts_ids_hostnames, vcenter_token) #{host: {vm_name: vm_esxi_id}}
    if DEBUG: print("host names - vm_names and '': ", hostname_vmnames)

    hosts: List[EsxiHost] = [] # list of EsxiHost instances
    for id, hostname in hosts_ids_hostnames.items():
        hosts.append(EsxiHost(hostname, id))

    for hostname, vms in hostname_vmnames.items():
        for host in hosts:
            if host.hostname == hostname:
                for vm_name in vms:
                    vm = VirtualMachine(vm_name)
                    host.add_vm(vm)
    
    if DEBUG: [print(host) for host in hosts]

    hosts[0].enable_ssh(mngmnt_domain_name)

    [host.vm_esxiid(vcenter_vmnames) for host in hosts]
        
    if DEBUG: [print(host) for host in hosts]

    [host.vm_tools() for host in hosts]
        
    if DEBUG: [print(host) for host in hosts]

    if not vmtools_are_running(hosts):
        sys.exit("Make sure vmware tools is running for all vCenter virtual machines. Exiting.")

    print("move the vm you have 60 seconds")
    time.sleep(60)
    [host.vm_stop() for host in hosts]
    sys.exit("no need to continue")
    for i in range(10):
        time.sleep(1)

        print("Checking if vCenter virtual machines are stopped.")
        for host in hosts:
            host.vm_status()

        if confirm_all_vms_are_stopped(hosts):
            print("All vCenter virtual machines are stopped.")
            break
    else:
        sys.exit("Timeout: Some virtual machines are still running.")

    [host.vm_snap() for host in hosts]

    # Snap checking
    for i in range(10):
        print("Checking if vCenter virtual machines are snapped.")
        for host in hosts:
            if not host.vm_snap_info():
                print(f"Vms on host {host.hostname} are not snapped: {', '.join([vm.name for vm in host.vms])}")
                continue
        print("vCenter virtual machines are snapped successfully.")
        break
    else:
        sys.exit("Timeout: Some virtual machines are not running.")

    if DEBUG: [print(host) for host in hosts]

    [host.vm_start() for host in hosts]

    for i in range(10):
        print("Checking if vCenter virtual machines are started.")
        for host in hosts:
            host.vm_status()

        if confirm_all_vms_are_started(hosts):
            print("All vCenter virtual machines are running.")
            break
    else:
        sys.exit("Timeout: Some virtual machines are not running.")

    if DEBUG: [print(host) for host in hosts]

    hosts[0].disable_ssh(mngmnt_domain_name)
    cleanup()

class VirtualMachine:
    def __init__(self, name) -> None:
        self.name = name
        self.vm_running = True
        self.vmtools_running = None
        self.esxi_id = -1
        self.snapped = False

    def __repr__(self) -> str:
        return f"Name: {self.name}; Vm running: {self.vm_running}, Vm tools running: {self.vmtools_running}, Esxi ID: {self.esxi_id}"


class EsxiHost:
    def __init__(self, hostname, id) -> None:
        self.hostname = hostname
        self.id = id
        self.vms: List[VirtualMachine] = []
        self._yaml_file_name = f"{self.hostname}-{self.id}-{DATE}.yaml"

    def __create_yaml(self, esxi_cmd) -> None:
        l1 = "resources:                        # Must be one among VCENTER, ESX_HOST, SDDC_MANAGER_VCF\n"
        l2 = "  - ESX_HOST\n"
        l3 = "config:\n"
        l4 = "  parallel: false                 # If 'true' run starts parallel on resources types.\n"
        l5 = "ESX_HOST:\n"
        l6 = "  script:                         # List of commands in order to run on resources.\n"
        l7 = f"    - {esxi_cmd}\n"
        l8 = "  hosts:\n"
        l9 = f"    - {self.hostname}\n"
        l10 = "  config:\n"
        l11 = "    timeout: 120                 # timeout to run single command on resources. Default: 2700\n"
        l12 = "    parallel: false               # If 'true' runs commands in parallel. Default: 'false'\n"

        with open(self._yaml_file_name, "w") as file:
            file.writelines([l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l11, l12])
            file.close()

    @staticmethod
    def __format_sos_output(output: str) -> list: # IMPROVEMENT LOGS
        logs = ""
        fix_it_up_log = ""
        output_runs = []
        output_list = output.split("\\n")
        run_format = {
            "host": "",
            "command": "",
            "status": "",
            "return": 0,
            "errors": [],
            "output": [],
        }
        keys = ["host", "command", "status", "return"]
        checks = ["] Host:", "] Command initiated:", "] Operation Status:", "] Return Code:"]
        run_num = 0

        # Row 3
        if output_list[2][:6] == "Logs :":
            chr_index = output_list[2].find(":")
            logs = output_list[2][chr_index+1:].strip()
        
        # Row 4
        if output_list[3][:15] == "Fix-It-Up log :":
            chr_index = output_list[3].find(":")
            fix_it_up_log = output_list[3][chr_index+1:].strip()
        
        # Rows from 7 until (last row - 3)
        for i in range(6, len(output_list)-3):
            row = output_list[i]

            if checks[0] in row:
                run = run_format

            if run_num < 4 and checks[run_num] in row:

                chr_index = row.find(":")
                run[keys[run_num]] = row[chr_index+1:].strip()
                run_num += 1

            elif "] Errors:" in row:
                next_row = output_list[i+1]

                while "] Output:" not in next_row:
                    run["errors"].append(next_row)
                    i += 1
                    next_row = output_list[i+1]
            elif "] Output:" in row:
                next_row = output_list[i+1]

                while "" != next_row:
                    run["output"].append(next_row)
                    i += 1
                    next_row = output_list[i+1]
            
                output_runs.append(run)

        return [logs, fix_it_up_log, output_runs]

    def __output_error_check(self, output_runs: List[dict]) -> Dict[str, list]:
        error_found = False
        for run in output_runs:
            if run["status"] != "SUCCESS":
                error_found = True
            if run["return"] != "0":
                error_found = True
                errors = [err for err in run["errors"] if err]
                command = run["command"]
                print(f"Errors found on host {self.hostname}: {errors}\nCommand initiated on ESXi: {command}")
        return error_found

    def __find_vm_from_esxiid(self, vm_esxiid):
        for vm in self.vms:
            if vm.esxi_id == vm_esxiid:
                return vm

    def __execute(self, esxi_cmd):
        self.__create_yaml(esxi_cmd)
        if self._yaml_file_name not in YAML_FILES_CREATED:
            YAML_FILES_CREATED.append(self._yaml_file_name)
        sddc_cmd = ["/opt/vmware/sddc-support/sos", "--ondemand-service", self._yaml_file_name, "--force"]
        result = subprocess.run(sddc_cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE)

        if DEBUG: print("\n", result.stdout, "\n") # DEBUG
        
        if result.stderr:
            sys.exit(result.stderr)

        # IMPROVE LOG MANAGEMENT
        logs, fix_it_up_log, output_runs = self.__format_sos_output(str(result.stdout))

        if DEBUG: print("Logs: ", logs) # DEBUG
        if DEBUG: print("Fix-It-Up log: ", fix_it_up_log) # DEBUG

        if self.__output_error_check(output_runs):
            sys.exit("Exiting, errors found.")

        return output_runs

    def __change_ssh(self, ssh_enabled, domain: str):
        if ssh_enabled:
            sddc_cmd = ["/opt/vmware/sddc-support/sos", "--enable-ssh-esxi", "--domain-name", domain]
        else:
            sddc_cmd = ["/opt/vmware/sddc-support/sos", "--disable-ssh-esxi", "--domain-name", domain]
        result = subprocess.run(sddc_cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE)

        if result.stderr:
            sys.exit(result.stderr)
        
        logs, fix_it_up_log, output_runs = self.__format_sos_output(str(result.stdout))
        if self.__output_error_check(output_runs):
            sys.exit("Exiting, errors found.")

    def enable_ssh(self, domain: str):
        self.__change_ssh(True, domain)
        print(f"SSH enabled for hosts in '{domain}' domain.")

    def disable_ssh(self, domain: str):
        self.__change_ssh(False, domain)
        print(f"SSH disabled for hosts in '{domain}' domain.")

    def add_vm(self, vm):
        self.vms.append(    vm)

    def vm_stop(self) -> bool:
        if len(self.vms) == 0:
            return
        
        esxi_cmd = ""
        for vm in self.vms:
            # esxi_cmd += f"echo -n '{vm.esxi_id}:'; vim-cmd vmsvc/power.shutdown {vm.esxi_id};"
            # esxi_cmd += f"arr=$( vim-cmd vmsvc/getallvms | grep {vm.name} ) ; for el in $arr; do if [ $el -eq {vm.esxi_id} ]; then echo -n '{vm.name}:{vm.esxi_id}'; vim-cmd vmsvc/power.shutdown {vm.esxi_id}; break; else echo -n '{vm.name}:'; break; fi done"
            esxi_cmd += f"arr=$( vim-cmd vmsvc/getallvms | grep {vm.name} ) ; for el in $arr; do if [ $el -eq {vm.esxi_id} ]; then echo -n '{vm.name}:{vm.esxi_id}'; break; else echo -n '{vm.name}:'; break; fi done"
        output_runs = self.__execute(esxi_cmd)
        run = output_runs[0]

        print(run["output"])
        for vm_info in run["output"]:
            vm_name, vm_esxiid = vm_info.split(":")
            if not vm_esxiid.strip():
                print(f"vm {vm_name} not found")
                print([vm.esxi_id for vm in self.vms])
                self.vm_esxiid([vm.name for vm in self.vms])
                print([vm.esxi_id for vm in self.vms])
                return False
        return True

    def vm_snap(self):
        if len(self.vms) == 0:
            return
        
        esxi_cmd = ""
        for vm in self.vms:
            esxi_cmd += f"vim-cmd vmsvc/snapshot.create {vm.esxi_id} {SNAP_NAME};"
        self.__execute(esxi_cmd)

    def vm_snap_info(self) -> bool:
        if len(self.vms) == 0:
            return True
        
        esxi_cmd = ""
        for vm in self.vms:
            esxi_cmd += f"echo -n '{vm.esxi_id}:'; vim-cmd vmsvc/get.snapshotinfo {vm.esxi_id};"
        output_runs = self.__execute(esxi_cmd)
        run = output_runs[0]

        count = 0
        for vm_info in run["output"]:
            if SNAP_NAME in vm_info:
                count += 1

        if not len(self.vms) == count:
            return False
        
        for vm in self.vms:
            vm.snapped = True
        return True

    def vm_start(self) -> Dict[str, str]:
        if len(self.vms) == 0:
            return
        
        esxi_cmd = ""
        for vm in self.vms:
            esxi_cmd += f"echo -n '{vm.esxi_id}:'; vim-cmd vmsvc/power.on {vm.esxi_id};"
        self.__execute(esxi_cmd)

    def vm_status(self):
        if len(self.vms) == 0:
            return

        esxi_cmd = ""
        for vm in self.vms:
            esxi_cmd += f"echo -n '{vm.esxi_id}:'; vim-cmd vmsvc/power.getstate {vm.esxi_id}| head -2 | tail -1;"
        
        output_runs = self.__execute(esxi_cmd)
        run = output_runs[0]
        
        for vm_info in run["output"]:
            vm_esxiid, vm_status = vm_info.split(":")
            vm = self.__find_vm_from_esxiid(vm_esxiid)
            vm.vm_running = True if vm_status == "Powered on" else False

    def vm_tools(self):
        if len(self.vms) == 0:
            return
        
        esxi_cmd = ""
        for vm in self.vms:
            esxi_cmd += f"echo -n '{vm.esxi_id}:'; vim-cmd vmsvc/get.guest {vm.esxi_id} | grep toolsRunningStatus;"
        
        output_runs = self.__execute(esxi_cmd)
        run = output_runs[0]

        for vm_info in run["output"]:
            vm_esxiid, vm_status = vm_info.split(":")
            vm = self.__find_vm_from_esxiid(vm_esxiid)
            vm.vmtools_running = True if "guestToolsRunning" in vm_status else False

    def vm_esxiid(self, vm_names):
        esxi_cmd = "vim-cmd vmsvc/getallvms | { grep -E " + f"\"{'|'.join(vm_names)}\"" + " || true; }"
        output_runs = self.__execute(esxi_cmd)
        run = output_runs[0]

        for vm_info in run["output"]: 
            vm_info_lst = re.split(r'\s{1,}', vm_info) # split by one or more spaces
            vm_esxiid = vm_info_lst[0]
            vm_name = vm_info_lst[1]

            for vm in self.vms:
                if vm.name == vm_name:
                    vm.esxi_id = vm_esxiid

    def __repr__(self) -> str:
        if len(self.vms) > 0:
            vms_str = "\n -".join([vm.__repr__() for vm in self.vms])
        else:
            vms_str = ""
        return f"Host {self.hostname} with id {self.id} has the following vCenter Virtual Machines:\n- {vms_str}"

def cleanup() -> None:
    for file in YAML_FILES_CREATED:
        os.remove(file)
        YAML_FILES_CREATED.remove(file)

def confirm_all_vms_are_started(hosts: List[EsxiHost]) -> bool:
    vms_started = True
    for host in hosts:
        if len(host.vms) == 0:
            return vms_started
        
        for vm in host.vms:
            if not vm.vm_running:
                print(f"Virtual machine {vm.name} is not running.")
                vms_started = False

    return vms_started

def confirm_all_vms_are_stopped(hosts: List[EsxiHost]) -> bool:
    vms_stopped = True
    for host in hosts:
        if len(host.vms) == 0:
            return vms_stopped
        
        for vm in host.vms:
            if vm.vm_running:
                print(f"Virtual machine {vm.name} is still running. Waiting vm to stop.")
                vms_stopped = False
    
    return vms_stopped

def vmtools_are_running(hosts: List[EsxiHost]) -> bool:
    """
    Confirm if vmware tools is running for vCenter virtual machines
    """
    vmtools_running = True

    for host in hosts:
        for vm in host.vms:
            if not vm.vmtools_running:
                print(f"Vmware tools for virtual machine {vm.name} is not running.")
                vmtools_running = False
    
    return vmtools_running

def vm_is_running(virtual_machine: dict) -> bool:
    """
    Confirm virtual machine is Powered on, throw an error if state is different from normal
    """
    vm_state = virtual_machine["power_state"]
    if vm_state == "POWERED_ON":
        return True
    elif vm_state == "POWERED_OFF":
        return False
    
    vm_name = virtual_machine["name"]
    sys.exit(f"vCenter virtual machine {vm_name} is not running but is {vm_state}")

def locate_vcenter_vms_on_host(vcenter_token, host_id, host_hostname, vcenter_vmnames) -> list:
    """
    Locate which vCenter VMs resides on a particular host
    """
    vcenter_hostname, token = vcenter_token
    h = {"vmware-api-session-id": token}
    vms = ""
    try:
        url = f"https://{vcenter_hostname}/api/vcenter/vm?hosts={host_id}"
        vms = requests.get(url, headers=h, verify=False)
    except:
        print(f"Couldn't get vms on host {host_hostname} from vcenter {vcenter_hostname}")
        return
    
    vms: List[dict] = vms.json()
    vm_names = []
    for vm in vms:
        vm_name = vm["name"]
        if vm_name in vcenter_vmnames:
            if not vm_is_running(vm):
                sys.exit(f"vCenter virtual machine is not running: {vm_name}")
            vm_names.append(vm_name)

    return vm_names

def locate_vcenter_vms(vcenter_vmnames: list, hosts_ids_hostnames: Dict[str, str], vcenter_token: tuple) -> Dict[str, List[str]]:
    """
    Locate on which hosts resides all vCenter virtual machines.
    Returns: Dict{Host Hostname: List[VM name]}
    """
    hostname_vmnames = {}
    counter = 0
    
    for host_id, host_hostname in hosts_ids_hostnames.items():
        vm_names = locate_vcenter_vms_on_host(vcenter_token, host_id, host_hostname, vcenter_vmnames)
        if not vm_names:
            continue

        counter += len(vm_names)
        hostname_vmnames[host_hostname] = vm_names

    if counter != len(vcenter_vmnames):
        sys.exit(
            f"vCenter virtual machine number mismatch between those in SDDC DB and those registered in vCenter\n"
            f"VMs recorded in SDDC DB: {', '.join(vcenter_vmnames)}\n"
            f"VMs recorded in vCenter: {', '.join([', '.join(vms.keys()) for vms in hostname_vmnames.values()])}"
        )
    return hostname_vmnames 

def get_hosts_identifiers(vcenter_token: tuple) -> Dict[str, str]:
    """
    Get Management domain hosts IDs and hostnames
    """
    vcenter_hostname, token = vcenter_token
    identifiers = ""
    h = {"vmware-api-session-id": token}
    try:
        url = f"https://{vcenter_hostname}/api/vcenter/host"
        identifiers = requests.get(url, headers=h, verify=False)
        identifiers = identifiers.json()
    except Exception:
        sys.exit("Couldn't get identifiers")

    if not identifiers:
        sys.exit("Couldn't get identifiers")
    
    query = 'psql -U postgres -h localhost -d platform -qAtX -c "select host.hostname from host join host_and_domain on host.id=host_and_domain.host_id join domain on host_and_domain.domain_id=domain.id where domain.type=\'MANAGEMENT\';"'
    hostnames = query_db(query)
    ids_hostnames = {}
    for dict in identifiers:
        if dict["power_state"] == "POWERED_ON" and dict["name"] in hostnames:
            ids_hostnames[dict["host"]] = dict["name"]
        else:
            # host is powered off or not available in SDDC DB. The second is mismatch
            continue
    return ids_hostnames

def get_vcenter_token() -> tuple:
    """
    Create vCenter token from the management vCenter.
    Return tuple with the vCenter hostname and the token
    """
    vcenter_hostname = get_mngmnt_vcenter_hostname()
    
    username = input("Enter vCenter Administrator account [DEFAULT: administrator@vsphere.local]:")
    if not username:
        username = "administrator@vsphere.local"

    password = getpass.getpass(prompt="Enter your password: ", stream=None)
    token = None

    try:
        url = f"https://{vcenter_hostname}/api/session"
        token = requests.post(url, auth=(username, password), verify=False)
    except Exception:
        pass
    
    if not token:
        sys.exit(f"Couldn't get vCenter token. Check if vCenter virtual '{vcenter_hostname}' machine is running.")
    
    return (vcenter_hostname, token.text[1:-1])

def get_mngmnt_domain_name() -> list:
    """
    Get Management domain name from SDDC DB
    """
    query = 'psql -U postgres -h localhost -d platform -qAtX -c "SELECT name from domain WHERE type=\'MANAGEMENT\'"'
    mngmnt_domain_name = query_db(query)[0]
    return mngmnt_domain_name

def get_vcenter_hostnames_vmnames() -> Dict[str, str]:
    """
    Get vCenter hostnames with virtual machine names from SDDC DB
    """
    query = 'psql -U postgres -h localhost -d platform -qAtX -c "SELECT vm_name from vcenter"'
    vmnames = query_db(query)
    return vmnames

def get_mngmnt_vcenter_hostname() -> str:
    """
    Get Management domain vCenter hostname from SDDC DB
    """
    query = 'psql -U postgres -h localhost -d platform -qAtX -c "SELECT vm_hostname from vcenter where type=\'MANAGEMENT\'"'
    vcenter_hostname = query_db(query)
    return vcenter_hostname[0]

def query_db(query: str) -> str:
    """
    Query SDDC DB and return result
    """
    try:
        p = subprocess.Popen(query,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,universal_newlines=True,shell=True)
    except Exception as err:
        sys.exit(f"Connection to postgres throw an error:\n{err}")
    
    stdout, stderr = p.communicate()
    if stderr:
        sys.exit(f"Connection to postgres throw an error:\n{stderr}")
    stdout = [x for x in stdout.split("\n") if x]
    return stdout

main()

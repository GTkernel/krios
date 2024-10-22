#kube-sat aka krios

Code repository with modifications to kubernetes (k8s) to use it for LEO compute cloud.

## Requirements for run Krios
Currently, to run Krios, you need at least 6 ubuntu nodes that can all talk to each other.
Amongst these nodes, one node will be Krios orchestrator, one will be the client and the rest will be Krios satellite nodes.


## Installation steps:
- Install Go, Docker, Kubernetes, python3, and pip from the official sources. This needs to be done on all the nodes. If you are using cloudlab nodes, you can use [this script](https://github.com/vaibhavb007/cloudlab-k8s-script) to install all the necessary software on all the cloudlab nodes.
- Install all the requirements listed in requirements.txt using `pip install -r requirements.txt`. This needs to be done on the orchestrator and client.

### Orchestrator script changes:
In file `krios_daemon/script-main.py`, edit lines 22-37, updating the names and ip addresses of the nodes. Be careful about assigning the correct orchestrator and client identities.

### Orchestrator script changes:
In file `krios_daemon/script-client.py`, edit lines 15-27, updating the names and ip addresses of the nodes. Ensure that the names of the nodes and ip addresses match exactly what is provided in the `script-main.py`.

## Steps to run:
- On the client, run the client script using `python script-client.py` from the `krios_daemon` directory.
- On the orchestrator side, perform the `kubeadm` setup and install the `calico` CNI.
- Install Krios scheduler from `yamls` directory using `kubectl apply -f krios_scheduler.yaml`.
- Install the nginx pod from `yamls` directory using `kubectl apply -f nginx.yaml`. Ensure that the client location specified in the client and server scripts matches the one provided in this yaml file.
- On the orchestrator, run the main script using `python script-main.py` from the `krios_daemon` directory. 

Post this, you should start seeing the client getting response, and also logging the latency values.

from kubernetes import client, config

def get_follower_nodes(api):
	ret = api.list_node(watch = False)
	return [node for node in ret.items if 'node-role.kubernetes.io/control-plane' not in node.metadata.labels]

def get_node(api, node_name):
	return api.read_node(node_name)

def add_label_to_node(api, node_name, key, value):
	body = {"metadata":{"labels":{key: value}}}
	api.patch_node(node_name, body)

def remove_label_from_node(api, node_name, key):
	body = {"metadata":{"labels":{key: None}}}
	api.patch_node(node_name, body)

def get_pods(api):
	ret = api.list_pod_for_all_namespaces(watch = False)
	return [pod for pod in ret.items if pod.metadata.namespace != 'kube-system']

def get_pod(api, pod_name, namespace):
	return api.read_namespaced_pod(pod_name, namespace)

def get_pods_for_node(api, node_name):
	field_selector = 'spec.nodeName=' + node_name
	ret = api.list_pod_for_all_namespaces(watch = False, field_selector = field_selector)
	return [pod for pod in ret.items if pod.metadata.namespace != 'kube-system']

def add_label_to_pod(api, pod_name, namespace, key, value):
	body = {"metadata":{"labels":{key: value}}}
	api.patch_namespaced_pod(pod_name, namespace, body)

def remove_label_from_pod(api, pod_name, namespace, key, value):
	body = {"metadata":{"labels":{key: None}}}
	api.patch_namespaced_pod(pod_name, namespace, body)

def create_new_pod(api, pod, new_pod_name, new_node_name=None):	
	metadata = client.V1ObjectMeta(namespace = pod.metadata.namespace, name = new_pod_name, labels = pod.metadata.labels)
	spec = pod.spec
	spec.node_name = new_node_name
	new_pod = client.V1Pod(metadata = metadata, spec = pod.spec)
	ret = api.create_namespaced_pod(pod.metadata.namespace, new_pod)
	return ret

def is_pod_ready(api, pod):
	if pod.status.phase == "Pending":
		return False
	if pod.status.conditions is None:
		return False
	for condition in pod.status.conditions:
		if not condition.status:
			return False

	for container in pod.status.container_statuses:
		if not container.ready:
			return False

	return True


def delete_pod(api, pod):
	ret = api.delete_namespaced_pod(pod.metadata.name, pod.metadata.namespace)

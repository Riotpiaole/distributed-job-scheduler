IMAGE   := goflink:v1.0.0
CLUSTER := localhost:8000
PLUGIN  := wc
NREDUCE := 4

.PHONY: build deploy undeploy dev submit logs clean kafka-image

# Pull third-party images into minikube's image cache
kafka-image:
	minikube image pull bitnami/kafka:3.7.0

# Build binary + image inside minikube's Docker daemon
build:
	eval $$(minikube docker-env) && docker build -t $(IMAGE) .

# Apply all k8s manifests (assumes image already built)
deploy:
	kubectl apply \
		-f k8s/output-pvc-minikube.yaml \
		-f k8s/plugins-pvc-minikube.yaml \
		-f k8s/kafka.yaml \
		-f k8s/headless-service.yaml \
		-f k8s/rpc-service.yaml \
		-f k8s/statefulset.yaml \
		-f k8s/worker-deployment.yaml
	kubectl rollout status statefulset/go-flink

# Remove all k8s resources
undeploy:
	kubectl delete -f k8s/ --ignore-not-found

# Full local dev cycle: start minikube, mount datasets, build image, deploy
dev:
	minikube start --driver=docker
	$(MAKE) kafka-image
	$(MAKE) build
	minikube mount ./datasets:/mnt/datasets &
	$(MAKE) deploy

# Submit a word-count job (port-forward must be running)
submit:
	./go-flink submit \
		--cluster $(CLUSTER) \
		--plugin   $(PLUGIN) \
		--dir      /data/input \
		--n-reduce $(NREDUCE)

# Forward coordinator RPC port (run in background)
port-forward:
	kubectl port-forward svc/go-flink-rpc 8000:8000

# Tail logs for all coordinator + worker pods
logs:
	kubectl logs -l app=go-flink     -f --prefix --max-log-requests=10 &
	kubectl logs -l app=go-flink-worker -f --prefix --max-log-requests=10

# Wipe the entire minikube cluster
clean:
	minikube stop
	minikube delete

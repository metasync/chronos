include Makefile.env

build.release:
	@${CONTAINER_CLI} build docker \
		-t $(CHRONOS_IMAGE) \
		--build-arg RUBY_IMAGE_TAG=${RUBY_IMAGE_TAG}

build.dev:
	@${CONTAINER_CLI} build docker \
		-f docker/Dockerfile.dev \
		-t $(RUBY_IMAGE) \
		--build-arg RUBY_IMAGE_TAG=${RUBY_IMAGE_TAG}

push:
	@${CONTAINER_CLI} push $(CHRONOS_IMAGE)

dev:
	@${CONTAINER_CLI} run --rm -it \
		-v $(SRC_PATH):$(WORKDIR) \
		-v ${CITRINE_SRC_PATH}:${CITRINE_GEM_PATH} \
		${RUBY_IMAGE} /bin/sh

run:
	@${CONTAINER_CLI} run --rm -it \
		${CHRONOS_IMAGE} /bin/sh

prune:
	@${CONTAINER_CLI} image prune -f

clean: prune

.PHONY: build.release build.dev dev prune clean

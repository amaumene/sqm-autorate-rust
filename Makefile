NAME := sqm-autorate-rust

ARCH ?= arm64

.PHONY: container-build
container-build:
	podman build --platform linux/$(ARCH) -f Containerfile -t $(NAME)-static .
	mkdir -p bin
	podman create --name $(NAME)-extract $(NAME)-static
	podman cp $(NAME)-extract:/$(NAME) bin/$(NAME)-static-aarch64
	podman rm $(NAME)-extract

.PHONY: clean
clean:
	rm -rf ./bin


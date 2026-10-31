# A throwaway Murder cluster on Hetzner Cloud. Needs tofu, jq, ssh and go.
#
#   make up TOKEN_FILE=path    create the servers and wait for them
#   make deploy cluster smoke  install Murder, form the cluster, test it
#   make spare                 add a fourth, bigger node and join it
#   make ssh N=n2              a shell on a node
#   make down                  delete everything (servers bill by the hour)
#
# The token is read from TOKEN_FILE (a Hetzner Cloud project token with
# read and write access) and never printed.

TOKEN_FILE ?= .state/hcloud.token
STATE      := .state
KEY        := $(STATE)/id_ed25519
N          ?= n1

export HCLOUD_TOKEN = $(shell cat $(TOKEN_FILE) 2>/dev/null)
TF_VARS := -var ssh_public_key=$(KEY).pub

.PHONY: up down plan deploy cluster smoke spare status ssh token-check

token-check:
	@test -n "$$HCLOUD_TOKEN" || { echo "no token in $(TOKEN_FILE); pass TOKEN_FILE=path" >&2; exit 1; }

$(KEY):
	@mkdir -p $(STATE) && chmod 700 $(STATE)
	ssh-keygen -q -t ed25519 -N '' -C murder-test -f $(KEY)

$(STATE)/.init: main.tf
	tofu init -upgrade -input=false >/dev/null
	@touch $@

plan: token-check $(KEY) $(STATE)/.init
	tofu plan $(TF_VARS) $(TF_EXTRA)

up: token-check $(KEY) $(STATE)/.init
	tofu apply -auto-approve -input=false $(TF_VARS) $(TF_EXTRA)
	./nodes.sh wait

deploy: token-check
	./nodes.sh deploy $(NODES)

cluster: token-check
	./nodes.sh cluster

smoke: token-check
	./nodes.sh smoke

spare: TF_EXTRA = -var spare=true
spare: up
	./nodes.sh deploy n4
	./nodes.sh join n4

status: token-check
	./nodes.sh status

ssh: token-check
	./nodes.sh ssh $(N)

down: token-check $(STATE)/.init
	tofu destroy -auto-approve -input=false $(TF_VARS) -var spare=true
	rm -f $(STATE)/master.key $(STATE)/known_hosts

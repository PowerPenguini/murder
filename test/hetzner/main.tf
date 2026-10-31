# A throwaway Murder cluster on Hetzner Cloud: three nodes in three
# locations on one private network, each with a volume for its ZFS pool.
# `make -C test/hetzner up` creates it; `make down` deletes everything.

terraform {
  required_version = ">= 1.8"
  # State, with the servers' addresses, stays beside the SSH key, out of git.
  backend "local" {
    path = ".state/terraform.tfstate"
  }
  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "~> 1.70"
    }
  }
}

# The token comes from HCLOUD_TOKEN, which the Makefile reads from a file.
provider "hcloud" {}

variable "server_type" {
  description = "Type of the three nodes. cpx02 (1 vCPU, 1 GB) was the cheapest orderable in the EU in October 2026."
  type        = string
  default     = "cpx02"
}

variable "spare" {
  description = "Add a fourth node, of spare_type, for testing `murder cluster replace`."
  type        = bool
  default     = false
}

variable "spare_type" {
  description = "Type of the spare node: a bigger server to move a node onto."
  type        = string
  default     = "cpx12"
}

variable "client" {
  description = "Add a server outside the cluster that load tests run from, inside Hetzner's network, so the operator's own link does not blur the numbers."
  type        = bool
  default     = true
}

variable "image" {
  type    = string
  default = "ubuntu-26.04"
}

variable "volume_gb" {
  description = "Size of each node's volume, which holds its ZFS pool."
  type        = number
  default     = 10
}

variable "ssh_public_key" {
  description = "Path of the public key the Makefile generated for these servers."
  type        = string
}

variable "ssh_sources" {
  description = "Addresses allowed to reach SSH."
  type        = list(string)
  default     = ["0.0.0.0/0", "::/0"]
}

locals {
  nodes = merge(
    {
      n1 = { location = "fsn1", ip = "10.0.1.1", type = var.server_type }
      n2 = { location = "nbg1", ip = "10.0.1.2", type = var.server_type }
      n3 = { location = "hel1", ip = "10.0.1.3", type = var.server_type }
    },
    var.spare ? { n4 = { location = "fsn1", ip = "10.0.1.4", type = var.spare_type } } : {},
  )
  labels = { purpose = "murder-test" }
}

resource "hcloud_ssh_key" "test" {
  name       = "murder-test"
  public_key = file(var.ssh_public_key)
  labels     = local.labels
}

# The cluster port (7400) and the edge relay (7402) are reached only here:
# Hetzner firewalls filter public interfaces, not private networks.
resource "hcloud_network" "test" {
  name     = "murder-test"
  ip_range = "10.0.0.0/16"
  labels   = local.labels
}

resource "hcloud_network_subnet" "test" {
  network_id   = hcloud_network.test.id
  type         = "cloud"
  network_zone = "eu-central"
  ip_range     = "10.0.1.0/24"
}

resource "hcloud_firewall" "test" {
  name   = "murder-test"
  labels = local.labels
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "22"
    source_ips = var.ssh_sources
  }
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "80"
    source_ips = ["0.0.0.0/0", "::/0"]
  }
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "443"
    source_ips = ["0.0.0.0/0", "::/0"]
  }
  rule {
    direction  = "in"
    protocol   = "icmp"
    source_ips = ["0.0.0.0/0", "::/0"]
  }
}

resource "hcloud_volume" "pool" {
  for_each = local.nodes
  name     = "murder-test-${each.key}"
  size     = var.volume_gb
  location = each.value.location
  labels   = local.labels
}

resource "hcloud_server" "node" {
  for_each     = local.nodes
  name         = "murder-test-${each.key}"
  server_type  = each.value.type
  image        = var.image
  location     = each.value.location
  ssh_keys     = [hcloud_ssh_key.test.id]
  firewall_ids = [hcloud_firewall.test.id]
  labels       = local.labels
  user_data = templatefile("${path.module}/cloud-init.yaml", {
    device = "/dev/disk/by-id/scsi-0HC_Volume_${hcloud_volume.pool[each.key].id}"
  })
  public_net {
    ipv4_enabled = true
    ipv6_enabled = true
  }
  network {
    network_id = hcloud_network.test.id
    ip         = each.value.ip
  }
  depends_on = [hcloud_network_subnet.test]
  # A changed cloud-init applies to new servers; it does not replace the
  # running ones.
  lifecycle {
    ignore_changes = [user_data]
  }
}

resource "hcloud_volume_attachment" "pool" {
  for_each  = local.nodes
  volume_id = hcloud_volume.pool[each.key].id
  server_id = hcloud_server.node[each.key].id
  automount = false
}

resource "hcloud_server" "client" {
  count        = var.client ? 1 : 0
  name         = "murder-test-client"
  server_type  = var.server_type
  image        = var.image
  location     = "nbg1"
  ssh_keys     = [hcloud_ssh_key.test.id]
  firewall_ids = [hcloud_firewall.test.id]
  labels       = local.labels
  user_data    = "#cloud-config\npackages: [curl, jq]\nruncmd:\n  - mkdir -p /var/lib/murder-test && touch /var/lib/murder-test/ready\n"
  public_net {
    ipv4_enabled = true
    ipv6_enabled = true
  }
}

output "client" {
  value = var.client ? hcloud_server.client[0].ipv4_address : ""
}

output "nodes" {
  value = {
    for name, node in local.nodes : name => {
      public  = hcloud_server.node[name].ipv4_address
      private = node.ip
      type    = node.type
    }
  }
}

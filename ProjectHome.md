# Overview #

DelugeFS is a distributed filesystem I implemented as a proof of concept of several ideas I've had over the past few years.

Key features include:
  * a [shared-nothing architecture](http://en.wikipedia.org/wiki/Shared_nothing_architecture) (meaning there is no master node controlling everything - all peers are equal and there is no single point of failure)
  * auto-discovery of peers
  * able to utilize highly heterogeneous computational resources  (ie: a random bunch of disks stuck in a random number of machines)

Key insights this FS proves:
  * Efficient distribution of large blocks of immutable data without centralized control is a solved problem.  I use [libtorrent](http://www.rasterbar.com/products/libtorrent/).
  * Efficient sharing of small quantities of mutable data (without a central server) is a solved problem.  I use [Mercurial](http://mercurial.selenic.com/), but [Git](http://git-scm.com/) would have been equally useful.
  * Automatic discovery of peers on a local network is a solved problem.  [Zeroconf/Bonjour](http://en.wikipedia.org/wiki/Zeroconf) is well established.
  * Emulating a filesystem is a solved problem with [FUSE](http://en.wikipedia.org/wiki/Filesystem_in_Userspace).
  * All of these projects have Python bindings!
  * The key node in a distributed filesystem is the **disk**, not the machine.  Everything above the disk is network topology.

# Current Status #

**HIGHLY EXPERIMENTAL!** -- **PROOF OF CONCEPT ONLY** -- DO NOT USE FOR ANY CRITICAL DATA AT THIS POINT!

I'm using it as personal media center storage spanning three disks on two machines.  It works well so far, but it still very early in development.

Speed:
  * I/O across the FUSE boundary is CPU limited.  Max observed is ~10MB/s.  I suspect this is a limitation of the Python FUSE bindings I'm using.
  * I/O between nodes is limited by the disk read/write speeds.  I've observed >70MB/s sustained on my home network.

## Known Issues ##
  * Files over ~4GB are not stored (and their zero-length stubs cannot be deleted).  I believe this is due to an int vs. long incompatibility with libtorrent, but I haven't confirmed.

# Basic Algorithm #
To start up:
  1. Filesystem is started given a volume id, a storage location, and a mount point.
  1. Filesystem searches for local peers.
  1. Filesystem either pulls from our clones other peer's Hg repositories.
  1. Filesystem looks for any files it has locally (complete or not), and starts seeding them.

To write a file:
  1. Filesystem client opens a file and attempts to write.  Filesystem returns a handle to a local temporary file.
  1. Client writes to file and then closes it.
  1. Filesystem create a torrent of that file (containing metadata about the file along with secure hashes of its contents) and commits it to a local Hg repository.
  1. Filesystem contacts local peers and sends them a pull request.
  1. Other peers pull the file metadata update.

To read a file:
  1. If filesystem already has a copy of the file requested it returns the data directly.
  1. Filesystem loads the torrent file and starts searching for a peer with the file data via Bittorrents distributed hash table (DHT) peer discovery mechanism.
  1. Filesystem waits for the blocks covering the read request to become available, and then returns the data to the filesystem client.

To replicate a file:
  1. All peers participate in the bittorrent swarms associated with each file they have local copies of.
  1. If a peer notices the swarm size falls below a threshold, it will send out clone requests to a subset of its peers until the swarm size increases.


# Example Usage #

The first time the first node is ever brought up:

```
server1$ ./delugefs.py --create --id bigstore \
    --root /mnt/disk1/.bigstoredb \
    --mount ~/bigstore
```

All future invocations would omit the "--create".

To bring up an additional node on a different disk on the same machine:

```
server1$ ./delugefs.py --id bigstore \
    --root /mnt/disk2/.bigstoredb
```

(note the lack of the optional mount point)

To bring up an additional node on a different machine:

```
server2$ ./delugefs.py --id bigstore \
    --root /mnt/disk3/.bigstoredb \
    --mount ~/bigstore
```

That's all there is to it!
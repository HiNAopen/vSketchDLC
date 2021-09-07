<<<<<<< HEAD
# vSketchDLC
=======

vSketchDLC is a fine-grained and user-friendly 
communication measurement tool implemented in ps-lite, which 
is a light and efficient implementation of the parameter server framework and 
applied by deep learning frameworks such as MXNet.
vSketchDLC can trace low-level communication events between 
framework and communication library interface, and capture 
end-to-end push and pull communications between workers and servers. 

A light and efficient implementation of the parameter server
framework. It provides clean yet powerful APIs. For example, a worker node can
communicate with the server nodes by
- `Push(keys, values)`: push a list of (key, value) pairs to the server nodes
- `Pull(keys)`: pull the values from servers for a list of keys
- `Wait`: wait untill a push or pull finished.

A simple example:

```c++
  std::vector<uint64_t> key = {1, 3, 5};
  std::vector<float> val = {1, 1, 1};
  std::vector<float> recv_val;
  ps::KVWorker<float> w;
  w.Wait(w.Push(key, val));
  w.Wait(w.Pull(key, &recv_val));
```

More features:

- Flexible and high-performance communication: zero-copy push/pull, supporting
  dynamic length values, user-defined filters for communication compression
- Server-side programming: supporting user-defined handles on server nodes

### Build

`ps-lite` requires a C++11 compiler such as `g++ >= 4.8`. On Ubuntu >= 13.10, we
can install it by
```
sudo apt-get update && sudo apt-get install -y build-essential git
```
Instructions for gcc 4.8 installation on other platforms:
- [Ubuntu 12.04](http://ubuntuhandbook.org/index.php/2013/08/install-gcc-4-8-via-ppa-in-ubuntu-12-04-13-04/)
- [Centos](http://linux.web.cern.ch/linux/devtoolset/)
- [Mac Os X](http://hpc.sourceforge.net/).

Then clone and build

```bash
git clone https://github.com/dmlc/ps-lite
cd ps-lite && make -j4
```

### How to use

`ps-lite` provides asynchronous communication for other projects: 
  - Distributed deep neural networks:
    [MXNet](https://github.com/dmlc/mxnet),
    [CXXNET](https://github.com/dmlc/cxxnet),
    [Minverva](https://github.com/minerva-developers/minerva), and
    [BytePS](https://github.com/bytedance/byteps/)
  - Distributed high dimensional inference, such as sparse logistic regression,
    factorization machines:
    [DiFacto](https://github.com/dmlc/difacto)
    [Wormhole](https://github.com/dmlc/wormhole)


### Research papers
  1. Mu Li, Dave Andersen, Alex Smola, Junwoo Park, Amr Ahmed, Vanja Josifovski,
     James Long, Eugene Shekita, Bor-Yiing
     Su. [Scaling Distributed Machine Learning with the Parameter Server](http://www.cs.cmu.edu/~muli/file/parameter_server_osdi14.pdf). In
     Operating Systems Design and Implementation (OSDI), 2014
  2. Mu Li, Dave Andersen, Alex Smola, and Kai
     Yu. [Communication Efficient Distributed Machine Learning with the Parameter Server](http://www.cs.cmu.edu/~muli/file/parameter_server_nips14.pdf). In
     Neural Information Processing Systems (NIPS), 2014
>>>>>>> 50228ca (add vSketchDLC)

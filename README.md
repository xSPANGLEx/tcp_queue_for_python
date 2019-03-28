# TCP Queue for python 
This project is a queue for parallel computing in networks.
How to use is like Queue in Python built-in.

## Getting Started

### Get the source code
```bash
git clone https://github.com/xSPANGLEx/tcp_queue_for_python
```

### Import the library 

```python
from tcp_queue import TCPQueue
```

### Run server for example

```python
tq = TCPQueue(op_type="server")
tq.start()
tq.put(b"key", b"value")
msg = tq.get(b"key")
# msg = b"value"
```

### Client for example 

```python
tq = TCPQueue(host="127.0.0.1")
tq.put(b"key", b"value")
msg = tq.get(b"key")
# msg = b"value"
```

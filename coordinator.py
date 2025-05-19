from xmlrpc.server import SimpleXMLRPCServer
server = SimpleXMLRPCServer(("localhost", 8000), logRequests=True)

def add(x, y):
    """Return the sum of two numbers."""
    return x + y

def echo(msg):
    """Return the same message back."""
    return f"Echo: {msg}"

def chunker(num_lines=2000):
    with open('small.txt', 'r') as f:
        chunk = []
        for i, line in enumerate(f.readlines(), 1):
            if i % 

print("RPC server listening on http://localhost:8000")

server.register_function(add, 'add')
server.register_function(echo, 'echo')

# Run the server loop
server.serve_forever()

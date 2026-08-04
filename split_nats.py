import re

with open("gateway/src/nats_listener.rs", "r") as f:
    content = f.read()

# We won't do it via script if it's too complex. 
# Let's just create a new file and manually move it.

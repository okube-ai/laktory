import os

laktory_root = "./laktory/"

# Read version file
with open(os.path.join(laktory_root, "_version.py")) as fp:
    fp.read()

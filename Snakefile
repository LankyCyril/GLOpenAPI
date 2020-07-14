from os.path import exists

configfile: "config.yaml"
SANDBOX = "sandbox.snake"
if exists(SANDBOX):
    include: SANDBOX

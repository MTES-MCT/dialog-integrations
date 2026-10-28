"""What every call to a producer's server shares."""

# (connect, read): a large layer weighs tens of megabytes, and without a read timeout a
# silent server holds the job until GitHub kills it.
DOWNLOAD_TIMEOUT = (10, 600)

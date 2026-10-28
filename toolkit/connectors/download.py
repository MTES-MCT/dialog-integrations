"""What every call to a producer's server shares."""

# Connect timeout, then read timeout: the longest silence allowed while a file arrives, and
# a large layer can weigh tens of megabytes. Without one, a server that accepts the
# connection and never answers holds the job until GitHub kills it.
DOWNLOAD_TIMEOUT = (10, 600)

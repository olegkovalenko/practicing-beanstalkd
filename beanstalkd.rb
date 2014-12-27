require 'rubygems'
require 'optparse'

module Beanstalkd
	def self.optparse(options = {})
# Use: ./beanstalkd [OPTIONS]

# Options:
#  -b DIR   wal directory
#  -f MS    fsync at most once every MS milliseconds (use -f0 for "always fsync")
#  -F       never fsync (default)
#  -l ADDR  listen on address (default is 0.0.0.0)
#  -p PORT  listen on port (default is 11300)
#  -u USER  become user and group
#  -z BYTES set the maximum job size in bytes (default is 65535)
#  -s BYTES set the size of each wal file (default is 10485760)
#             (will be rounded up to a multiple of 512 bytes)
#  -c       compact the binlog (default)
#  -n       do not compact the binlog
#  -v       show version information
#  -V       increase verbosity
#  -h       show this help
	end
	def self.beanstalkd(argv)

		# parse command line options
		#
		# setup logging
		# set owner setgid/setuid
		#
		# tcp server family PF_UNSPEC, type SOCK_STREAM, flags AI_PASSIVE
		# socket +| O_NONBLOCK
    # r = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof flags);
    # r = setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &flags, sizeof flags);
    # r = setsockopt(fd, SOL_SOCKET, SO_LINGER, &linger, sizeof linger);
    # r = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof flags);
		# bind fd, addr
		# listen fd, 1024
		#
		# set signal handlers sigpipe, usr1
		#
		# wal, lock dir
	end
end

#!/bin/sh
set -eu
umask 077
printf '%s' "$1" > /home/ubuntu/.beeper/oauth-url

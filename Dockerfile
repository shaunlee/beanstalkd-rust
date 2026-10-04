# syntax=docker/dockerfile:1
#
# beanstalkd-rs container image: docker build -t beanstalkd-rs .
#
#   docker run -d -p 11300:11300 beanstalkd-rs                 # in memory
#   docker run -d -p 11300:11300 -v bstk:/data beanstalkd-rs \
#       -l 0.0.0.0 -p 11300 -b /data                           # with a binlog
#   docker run -d -v /etc/beanstalkd-rs:/etc/beanstalkd-rs:ro -v bstk:/data \
#       beanstalkd-rs --config /etc/beanstalkd-rs/config.toml  # config file
#
# Arguments replace the default command, so give -l / -p again (or use
# [[listener]] in a config file) when adding flags. Point binlog.dir or
# cluster.data_dir at /data, which belongs to the non-root user the server
# runs as (uid 10001).

# Builder and runtime share Debian 13 (trixie), so the binary links against
# the same glibc it runs with.
FROM rust:1.98-slim-trixie AS build
WORKDIR /src
# Every workspace member's manifest must be present for cargo to resolve
# the workspace, even though only bstk-server is built.
COPY . .
# target/ lives in a cache mount that disappears after this step, so the
# binary is copied out within the same RUN. The release profile keeps line
# tables for profiling (debug = 1), which makes a Linux binary about 6x
# larger; the shipped binary drops them but keeps symbols, so backtraces
# still name functions.
ENV CARGO_PROFILE_RELEASE_STRIP=debuginfo
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p bstk-server \
 && cp target/release/beanstalkd-rs /usr/local/bin/beanstalkd-rs

# debian-slim rather than distroless: the HEALTHCHECK below needs a shell.
FROM debian:13-slim
RUN useradd --system --uid 10001 --user-group --home-dir /data \
        --shell /usr/sbin/nologin beanstalkd \
 && mkdir -p /data \
 && chown beanstalkd:beanstalkd /data
COPY --from=build /usr/local/bin/beanstalkd-rs /usr/local/bin/beanstalkd-rs
COPY LICENSE README.md CHANGELOG.md /usr/share/doc/beanstalkd-rs/
USER beanstalkd
WORKDIR /data
VOLUME /data
EXPOSE 11300
# Sends `stats` on the default plaintext port and expects an OK reply, which
# a hung server would not give (a bare TCP connect can succeed from the
# kernel's accept backlog alone). It needs no HTTP listener, so it works
# with the default command. When the server listens elsewhere or only on
# TLS, override it: with an [http] listener, probe /healthz or /readyz
# from the orchestrator, or use --health-cmd / --no-healthcheck.
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD ["bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/11300 && printf 'stats\\r\\n' >&3 && read -r -t 2 reply <&3 && [[ $reply == OK* ]]"]
ENTRYPOINT ["beanstalkd-rs"]
CMD ["-l", "0.0.0.0", "-p", "11300"]

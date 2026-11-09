FROM scratch
LABEL org.opencontainers.image.title="Sigbot local verification carrier"
LABEL org.opencontainers.image.description="Host ABI matched debug binaries and external test fixtures; not a release image"
COPY rootfs /
ENV PATH=/usr/local/bin:/usr/bin
ENV PYTHONHOME=/usr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/opt/marketpulse:/opt/marketpulse/dependencies:/opt/marketpulse/deploy/mock-twitter-service/app
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV SIGBOT_CFG_PATH=/etc/sigbot/config.yaml
ENV SIGBOT_STRATEGY_WORKER_BINARY=/usr/local/bin/sigbot-strategy-worker
USER 1000:1000
WORKDIR /tmp
ENTRYPOINT ["sigbot"]

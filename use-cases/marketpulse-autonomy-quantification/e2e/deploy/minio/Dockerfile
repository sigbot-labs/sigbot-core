# A local verification carrier for the official binary pinned by config/minio/provenance.json.
# The builder verifies the binary checksum before creating this context; no registry image is implied.
FROM scratch
COPY minio /usr/local/bin/minio
COPY ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --chown=1000:1000 data /data
ENV HOME=/tmp
USER 1000:1000
ENTRYPOINT ["/usr/local/bin/minio"]

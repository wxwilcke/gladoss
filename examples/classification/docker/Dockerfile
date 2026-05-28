FROM gladoss:latest
LABEL org.opencontainers.image.authors="w.x.wilcke@vu.nl"

USER root

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

USER gladoss

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

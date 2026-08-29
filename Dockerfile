# syntax=docker/dockerfile:1.7
#
# Multi-stage image for happy-wakey-api-server.
# Prefer linux/arm64 (Apple Silicon / Graviton / Hetzner ARM):
#
#   docker buildx build --platform linux/arm64 -t happy-wakey-api-server:dev .
#   docker run --rm --platform linux/arm64 \
#     -e SOPS_AGE_KEY="$(cat ~/.config/sops/age/keys.txt)" happy-wakey-api-server:dev
#
# ores-sops contract (https://github.com/ORESoftware/ores-sops):
#   env/enc/dev.env.enc  and  env/enc/prod.env.enc   — ciphertext, committed
#   env/dec/<name>.env                               — plaintext, gitignored
# Decrypt at `docker run`, never at `docker build`. The image ships ciphertext
# (when present) plus sops; the age key arrives at runtime.

############################
# Stage 1 — build + strip
############################
FROM rust:1.88-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked --bin happy-wakey-api-server \
    && strip "target/release/happy-wakey-api-server"

############################
# Stage 2 — slim runtime + sops
############################
FROM debian:bookworm-slim AS runtime
ARG SOPS_ENV=prod
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && apt-get clean \
    && find /var/lib/apt/lists -mindepth 1 -delete \
    && useradd --system --uid 65532 --no-create-home --shell /usr/sbin/nologin app
COPY --from=build "/src/target/release/happy-wakey-api-server" "/usr/local/bin/happy-wakey-api-server"
COPY --from=ghcr.io/getsops/sops:v3.10.2-alpine --chmod=0755 /usr/local/bin/sops /usr/local/bin/sops
COPY --chmod=0755 scripts/sops-entrypoint.sh /usr/local/bin/sops-entrypoint.sh
# Ciphertext is optional. Bind-mount the repo so a missing env/enc does not
# fail the build; when present it is renamed to .env so sops can infer dotenv.
RUN --mount=type=bind,source=.,target=/src,ro \
    mkdir -p /app/secrets \
    && if [ -f /src/env/enc/${SOPS_ENV}.env.enc ]; then \
         cp "/src/env/enc/${SOPS_ENV}.env.enc" /app/secrets/app.env; \
       fi \
    && chown -R 65532:65532 /app /usr/local/bin/happy-wakey-api-server
ENV SOPS_SECRETS_FILE=/app/secrets/app.env
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/sops-entrypoint.sh", "/usr/local/bin/happy-wakey-api-server"]

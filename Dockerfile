FROM rust:1-bookworm AS build
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=build /app/target/release/vectaraft /usr/local/bin/vectaraft
RUN useradd -m -u 10001 app && mkdir -p /data && chown -R app:app /data
USER app
ENV VECTARAFT_ENABLE_WAL=1
ENV VECTARAFT_WAL_PATH=/data/wal.jsonl
EXPOSE 50051
CMD ["vectaraft"]

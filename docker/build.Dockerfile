FROM rust:1.91.1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake ninja-build g++ pkg-config ca-certificates python3 python3-pip python3-venv && apt-get clean
WORKDIR /src
COPY . .
RUN python3 -m venv /venv && /venv/bin/pip install --no-cache-dir -r python/requirements.txt && cmake --preset dev && cmake --build --preset dev -j2
ENV PATH="/venv/bin:${PATH}"
CMD ["ctest", "--test-dir", "build/dev", "--output-on-failure", "-LE", "integration"]

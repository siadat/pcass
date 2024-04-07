FROM alpine:3

RUN apk add git cargo nodejs g++

WORKDIR /work
RUN git clone --depth=1 https://github.com/tree-sitter/tree-sitter.git
RUN git clone --depth=1 https://github.com/tree-sitter/tree-sitter-java.git

WORKDIR /work/tree-sitter
RUN cargo build --verbose --release
RUN /work/tree-sitter/target/release/tree-sitter init-config

WORKDIR /work/tree-sitter-java
RUN /work/tree-sitter/target/release/tree-sitter generate
ENTRYPOINT ["/work/tree-sitter/target/release/tree-sitter"]

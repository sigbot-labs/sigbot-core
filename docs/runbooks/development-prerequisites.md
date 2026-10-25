# Prerequisites for Development

[Documentation index](../index.md)

## Install Rust 1.94

```bash
rustup toolchain install 1.94.0 --component rustfmt --component clippy --component llvm-tools
```

The repository pins this version in `rust-toolchain.toml`.

## [Install rustfmt VsCode plugin](https://marketplace.visualstudio.com/items?itemName=statiolake.vscode-rustfmt)

## Run LLM models with Ollama (Optional, if you want to locally run LLM models)

- Run embeddding model

```bash
export OLLAMA_HOST='0.0.0.0'
nohup ollama start >/dev/null &
```

- Verify embeddding model. [See more ollama API docs](https://github.com/ollama/ollama/blob/main/docs/api.md#generate-a-completion)

```bash
curl -X POST http://localhost:11434/api/embed \
-H "Content-Type: application/json" \
-d '{"model": "bge-m3:latest", "input": "Hello, world!"}'
```

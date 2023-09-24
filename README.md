
## Usage


1. **Node Setup**: In one terminal, run the following command to start a file provider node:
   ```sh
   cargo run --bin libp2p_test \
        -- --secret-key-seed 1 \
        --listen-address /ip4/127.0.0.1/tcp/42000 \
        --peer /ip4/127.0.0.1/tcp/43000/p2p/12D3KooWH3uVF6wv47WnArKHk5p6cvgCJEb74UTmxztmQDc298L3

    cargo run --bin libp2p_test \
        -- --secret-key-seed 2 \
        --listen-address /ip4/127.0.0.1/tcp/43000 \
        --peer /ip4/127.0.0.1/tcp/42000/p2p/12D3KooWPjceQrSwdWXPyLLeABRXmuqt69Rg3sBYbU1Nft9HyQ6X
   ```

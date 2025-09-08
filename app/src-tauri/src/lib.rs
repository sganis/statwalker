# .github/workflows/build-statwaker-fedora.yml
name: Build statwaker (Fedora latest, app/)

on:
  push:
    branches: [ master ]
  workflow_dispatch:

jobs:
  build-fedora:
    name: Fedora latest (x86_64)
    runs-on: ubuntu-latest
    container:
      image: fedora:latest
      # optional: harden DNS on GH runners
      options: --dns 1.1.1.1 --dns 8.8.8.8
    env:
      CARGO_HOME: /github/home/.cargo
      TAURI_SIGNING_PRIVATE_KEY: ${{ secrets.TAURI_SIGNING_PRIVATE_KEY }}
      TAURI_SIGNING_PRIVATE_KEY_PASSWORD: ${{ secrets.TAURI_SIGNING_PRIVATE_KEY_PASSWORD }}

    steps:
      # install git BEFORE checkout (we're inside the container)
      - name: Prepare container (git + build deps)
        shell: bash
        run: |
          dnf -y update
          dnf -y install --setopt=install_weak_deps=False \
            git which xz tar gzip pkgconf-pkg-config \
            gcc-c++ make patchelf rpm-build \
            gtk3-devel librsvg2-devel openssl-devel \
            libappindicator-gtk3 libappindicator-gtk3-devel
          # Tauri v2 needs WebKitGTK 4.1; fallback to 4.0 if 4.1 is unavailable
          dnf -y install webkit2gtk4.1-devel || dnf -y install webkit2gtk4.0-devel

      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: "20"
          cache: "npm"
          cache-dependency-path: app/package-lock.json

      - name: Setup Rust (stable)
        uses: dtolnay/rust-toolchain@stable

      - name: Cache Cargo
        uses: actions/cache@v4
        with:
          path: |
            /github/home/.cargo/registry
            /github/home/.cargo/git
            app/src-tauri/target
          key: fedora-cargo-${{ hashFiles('**/Cargo.lock') }}
          restore-keys: fedora-cargo-

      - name: Install JS deps
        working-directory: app
        run: npm ci

      - name: Build frontend
        working-directory: app
        run: npm run build --if-present

      - name: Build Tauri bundle
        working-directory: app
        run: npx tauri build

      - name: Upload artifacts
        uses: actions/upload-artifact@v4
        with:
          name: statwaker-fedora
          path: app/src-tauri/target/**/release/bundle/**/*
          if-no-files-found: error


          
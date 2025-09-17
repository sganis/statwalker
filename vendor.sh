#!/bin/sh
#
# run from project root dir
cd rs
cargo vendor --versioned-dirs --no-delete ../vendor
cargo clean
cd ../browser
rm -rf nodes_modules
rm -f package-lock.json
cd ../desktop
rm -rf build
rm -rf nodes_modules
rm -f package-lock.json
cd src-tauri
cargo clean
cargo vendor --versioned-dirs --no-delete ../../vendor
cd ../..
echo "compressing..."
zip -qr ../statwalker.zip .



{ pkgs ? import <nixpkgs> { } }:
(pkgs.buildFHSUserEnv {
  name = "uvdev";
  targetPkgs = pkgs: (with pkgs; [
    uv
    rustc
    cargo
    cmake
    clang
  ]);
  runScript = "bash";
}).env

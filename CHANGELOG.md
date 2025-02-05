# CHANGELOG


## v0.4.0 (2025-02-05)

### Chores

- **ci**: Fix pipeline.yml typo in release job
  ([`445701c`](https://github.com/Diatonika/baikal-template/commit/445701c4d1ce2254e8a7b63b39514adebcc112ce))

### Features

- **docs**: Support customizable automatic release notes generation
  ([`68df72d`](https://github.com/Diatonika/baikal-template/commit/68df72d567f8da2a7ab7eb3b961c0e95be539abc))

### Refactoring

- Change PYTHON_APP to PYTHON_PUBLISH to clarify flag intention
  ([`d23efad`](https://github.com/Diatonika/baikal-template/commit/d23efad26947832b978e291947b1a8de8c60f4bc))


## v0.3.5 (2025-02-05)

### Bug Fixes

- **ci**: Extract latest release notes from CHANGELOG.md
  ([`16d21cd`](https://github.com/Diatonika/baikal-template/commit/16d21cda355327a87b3a35a6cfcc70125777a3e8))


## v0.3.4 (2025-02-04)

### Bug Fixes

- **build**: Fix installation of all dev dependencies for linter runs
  ([`cd6ea60`](https://github.com/Diatonika/baikal-template/commit/cd6ea60aa493977aed268897ae8fdda245141a16))


## v0.3.3 (2025-02-04)

### Bug Fixes

- **ci**: Add required permissions to create GitHub release
  ([`f6fdcf3`](https://github.com/Diatonika/baikal-template/commit/f6fdcf3b65c19db996f45956bf3c50e0e5a20816))


## v0.3.2 (2025-02-04)

### Bug Fixes

- **ci**: Fix typo in GH_TOKEN
  ([`524b6c3`](https://github.com/Diatonika/baikal-template/commit/524b6c3ff3d78a85420a61b291d4cac1b542eb0c))

- **ci**: Fix unsigned release tags
  ([`8cd4140`](https://github.com/Diatonika/baikal-template/commit/8cd4140cc83121cbc83d400e441556654d3417fc))


## v0.3.1 (2025-02-04)

### Bug Fixes

- **ci**: Disable automatic semver github commit
  ([`e28945e`](https://github.com/Diatonika/baikal-template/commit/e28945efcabaf1ecc42a5e3015bf42256feb931f))

- **ci**: Remove poetry.toml from commit glob
  ([`487ef4d`](https://github.com/Diatonika/baikal-template/commit/487ef4d420d654fe660e36b720119893e5c4596e))


## v0.3.0 (2025-02-04)

### Bug Fixes

- **build**: Fix nexus password environment variable typo
  ([`6f9a88d`](https://github.com/Diatonika/baikal-template/commit/6f9a88d8fef38542ba9c4a5fbd3b2db08ed4fdc5))

- **ci**: Checkout repository using automation ssh key
  ([`fdbe220`](https://github.com/Diatonika/baikal-template/commit/fdbe220c3c6822137c651c055a1a2e4cfd85eaab))

- **ci**: Fix ssh signing using separate action
  ([`bcffefa`](https://github.com/Diatonika/baikal-template/commit/bcffefadb51b2f170590b6cb5770121e059d294d))

### Features

- **style**: Replace linters and formatters with ruff
  ([`1f6a5ad`](https://github.com/Diatonika/baikal-template/commit/1f6a5ad4a7de7c1c3f654073f713127bccc32ed8))


## v0.2.0 (2025-02-03)

### Documentation

- Add README.md
  ([`bf26481`](https://github.com/Diatonika/baikal-template/commit/bf2648131883ce278e3117faf825d4ddf056cd09))

### Features

- **ci**: Support optional build and publish skip for standalone python apps
  ([`09410b4`](https://github.com/Diatonika/baikal-template/commit/09410b41f81abffc9719615a417bd443779e64eb))


## v0.1.0 (2025-02-03)

### Bug Fixes

- **ci**: Add nexus access variables into linter environment
  ([`877950a`](https://github.com/Diatonika/baikal-template/commit/877950ae55489af31a8e56936e59033a63b7c141))

- **ci**: Fix app installation token usage inside CI
  ([`a3ccce7`](https://github.com/Diatonika/baikal-template/commit/a3ccce7cc1dd4dbf7ec61e5fb2d87d24e59962b5))

- **ci**: Fix build artifact search location
  ([`f350e12`](https://github.com/Diatonika/baikal-template/commit/f350e12e0b611216d1a68ee1997a04073c5a20f0))

- **ci**: Fix invalid git_committer_name parameter
  ([`47bab77`](https://github.com/Diatonika/baikal-template/commit/47bab774bffd141520580896cf9a099fcdd6d0e9))

- **ci**: Fix invalid justfile parameters in linter ci
  ([`6e9c7bf`](https://github.com/Diatonika/baikal-template/commit/6e9c7bf37303e389cf30637a31c5c76f9b5ecc8c))

- **ci**: Fix missing build step in semantic release
  ([`f8e4991`](https://github.com/Diatonika/baikal-template/commit/f8e49915e13ba4df7b121db83db228a997733a0f))

- **ci**: Fix skipped build in semantic release
  ([`956b48a`](https://github.com/Diatonika/baikal-template/commit/956b48a17ef7da1548ecd38410530a2334fe4682))

### Continuous Integration

- Create first CI version
  ([`f1bb7ae`](https://github.com/Diatonika/baikal-template/commit/f1bb7aeb549360ce038b516c2fdbcd51b2312fa2))

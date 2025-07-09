# CHANGELOG


## v0.14.0 (2025-07-09)

### Features

- **trade**: Make OHLCV non-nullable and present basic sequential parquet dataset TimeSeries writer
  ([#7](https://github.com/Diatonika/baikal-common/pull/7),
  [`0b975d0`](https://github.com/Diatonika/baikal-common/commit/0b975d07693c7064d10ca22b4b8650af7765930f))

* feat(trade): make OHLCV non-nullable and present basic sequential parquet dataset TimeSeries
  writer

* return explicit polars dependency


## v0.13.0 (2025-06-28)

### Features

- **rich**: Support utilities for time-based progress reporting
  ([#6](https://github.com/Diatonika/baikal-common/pull/6),
  [`88855f4`](https://github.com/Diatonika/baikal-common/commit/88855f422a3b081d8d6338eda3b981e278b97577))


## v0.12.0 (2025-06-27)

### Features

- **trade**: Support TradeModel to Polars Schema conversion
  ([#5](https://github.com/Diatonika/baikal-common/pull/5),
  [`8476f1a`](https://github.com/Diatonika/baikal-common/commit/8476f1a6a1ef248ba578df637342903d0637cdf5))


## v0.11.0 (2025-06-26)

### Features

- **trade**: Rename OHLC(V) fields and allow nullables
  ([#4](https://github.com/Diatonika/baikal-common/pull/4),
  [`26be5aa`](https://github.com/Diatonika/baikal-common/commit/26be5aa984a5bba41e2695d8239d85085c54ec49))


## v0.10.0 (2025-06-26)

### Features

- **trade**: Introduce base TradeModel utility class
  ([#3](https://github.com/Diatonika/baikal-common/pull/3),
  [`ba56f28`](https://github.com/Diatonika/baikal-common/commit/ba56f28a1fdd9b591e268aa923a78de019892843))

* feat(trade): introduce base TradeModel utility class

* move coercion config into TradeModel class


## v0.9.0 (2025-06-25)

### Features

- **trade**: Introduce basic time series models for trading
  ([#2](https://github.com/Diatonika/baikal-common/pull/2),
  [`c609618`](https://github.com/Diatonika/baikal-common/commit/c6096188d7b70c4534b51c138f4dcafaf037eaa9))

* feat(trade): introduce basic time series models for trading

* fix pandera warning about missing pandas


## v0.8.0 (2025-06-23)

### Refactoring

- **os**: Enclose file system util into separate namespace
  ([#1](https://github.com/Diatonika/baikal-common/pull/1),
  [`d6e8a62`](https://github.com/Diatonika/baikal-common/commit/d6e8a62ed311e4bd721d9a75d15839a3bb98bc41))


## v0.7.0 (2025-03-21)

### Features

- **rich**: Rework rich helper interfaces
  ([`7e74b48`](https://github.com/Diatonika/baikal-common/commit/7e74b48a64dda2ace4449264a8470c06a7caa55c))


## v0.6.0 (2025-02-24)

### Features

- **rich**: Support no-argument overload for ConsoleContext.from_parameters()
  ([`e81cf63`](https://github.com/Diatonika/baikal-common/commit/e81cf639201aaf36f4fe2f76602c97f317b57794))


## v0.5.0 (2025-02-24)

### Features

- **rich**: Support console decorator
  ([`be5ad58`](https://github.com/Diatonika/baikal-common/commit/be5ad5863d6a0d2f82369ed1b9066444c50d7516))


## v0.4.0 (2025-02-19)

### Features

- Support logger context wrapper
  ([`22dbfa1`](https://github.com/Diatonika/baikal-common/commit/22dbfa1da3c525fc5150cf2daef0b86a2e33578a))


## v0.3.0 (2025-02-11)

### Features

- **build**: Transit to namespace package structure
  ([`1015175`](https://github.com/Diatonika/baikal-common/commit/10151758f70a4f9b104e6c712b25978f31a1d44c))

- **docs**: Include example settings.toml into distribution
  ([`2868d44`](https://github.com/Diatonika/baikal-common/commit/2868d4423769eb6cefecdd4688c2a534946ec6ab))


## v0.2.1 (2025-02-10)

### Bug Fixes

- Change common.rich theme config to baikal.common.rich.styles
  ([`0d35dc6`](https://github.com/Diatonika/baikal-common/commit/0d35dc69e6e56f088a9819fdc11a2dbe4305462f))


## v0.2.0 (2025-02-10)

### Bug Fixes

- **build**: Rename os to system to avoid stdlib shadowing
  ([`f37fa61`](https://github.com/Diatonika/baikal-common/commit/f37fa61762bd8c76e41b169de16019a283d4900d))

- **style**: Add test dependencies into lint requisites
  ([`67dee4d`](https://github.com/Diatonika/baikal-common/commit/67dee4d7b5b6dd39bb1efaf98eaa66f3857a60ca))

- **style**: Fix linter test source dir typo
  ([`128d9b5`](https://github.com/Diatonika/baikal-common/commit/128d9b56cbb80fafa28b9e8af672a319b4cf0445))

### Chores

- **build**: Apply latest template repository changes
  ([`62da934`](https://github.com/Diatonika/baikal-common/commit/62da934e799e2759c510d901c2716587e7b0602e))

- **style**: Sort and prettify pyproject.toml
  ([`b2412e3`](https://github.com/Diatonika/baikal-common/commit/b2412e342be08fee477ba4158d6da2cc61017111))

### Features

- **ci**: Support test stage on ci
  ([`be8a3cf`](https://github.com/Diatonika/baikal-common/commit/be8a3cfcc741a62148fe438e3a6603d34e863624))

- **style**: Add justfile support for tests
  ([`922277c`](https://github.com/Diatonika/baikal-common/commit/922277c931d16d5159c5f995d286d8e46fa5371a))

- **test**: Add pytest support
  ([`aa9e0f9`](https://github.com/Diatonika/baikal-common/commit/aa9e0f938169a0ccffcd65dd831c924d2f057335))

- **test**: Add tests for common.rich
  ([`214c5fc`](https://github.com/Diatonika/baikal-common/commit/214c5fcf2ee7c45c3d868a61a3cdda6931d176e2))


## v0.1.0 (2025-02-05)

### Bug Fixes

- **style**: Fix linter and formatter errors
  ([`eec7914`](https://github.com/Diatonika/baikal-common/commit/eec7914c0909dbd531ae4351d8ea00f7908c9df6))

### Features

- **build**: Initialize repository
  ([`943495b`](https://github.com/Diatonika/baikal-common/commit/943495bcd092f716180d8a3550216fa88093e6c6))


## v0.0.0 (2025-02-05)

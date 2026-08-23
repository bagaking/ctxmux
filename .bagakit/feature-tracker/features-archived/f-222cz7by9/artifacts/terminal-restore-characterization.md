# Terminal restoration characterization

During T-010, a real pseudo-terminal fixture exercised interactive CLI attach from a non-default termios state through clean detach and a recoverable protocol error.

On macOS, crossterm restored the stable user settings, but `stty -g` after raw-to-canonical restoration differed from the pre-attach snapshot by transient `PENDIN` (`0x20000000`). A byte-identical oracle would therefore fail the current system while conflating a driver transition flag with durable user configuration.

Decision:

- remove the attempted blocking test rather than normalize the difference silently;
- keep `CLI-01` and `CLI-02` as future fixtures in `fixtures/wrong-cases.json`;
- activate them only after ctxmux chooses a direct termios owner or documents and reviews a cross-platform normalization policy;
- keep `CLI-03` active through deterministic prefix-router tests.

This characterization proves a contract gap. It does not prove that terminal restoration is generally broken or that `PENDIN` is harmless on every platform.

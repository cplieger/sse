# Third-party notices

This package has no runtime dependencies. The parse loop in `src/parser.ts` follows the structure of `eventsource-parser`, whose license is reproduced below.

Three further designs are followed without any of their code being included; each is named at the line that follows it:

- The silence watchdog in `src/timing.ts` (`max(3 keepalive beats, 15 s)`) follows [Yaffle/EventSource](https://github.com/Yaffle/EventSource).
- The resume handshake's structural pessimism in `src/wire.ts` (only `resumed === true` resumes; every other verdict reconciles), and the matching cursor and verdict shape on the Go side, follow [Centrifugo](https://github.com/centrifugal/centrifugo)'s recovery handshake.
- The epoch-bound version map in `src/versions.ts` follows IMAP CONDSTORE ([RFC 7162](https://www.rfc-editor.org/rfc/rfc7162)): a change of the server's epoch discards every cached version, as a `UIDVALIDITY` change discards cached mod-sequences.

## eventsource-parser

<https://github.com/rexxars/eventsource-parser>

```text
MIT License

Copyright (c) 2026 Espen Hovlandsdal <espen@hovlandsdal.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

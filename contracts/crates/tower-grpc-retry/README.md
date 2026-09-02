This was adapted from [linkerd2-proxy].

The changes made are:

- Make the error type generic instead of using linkerd2-proxy's error type
- Specialize towards tonic instead of generic http body (or linkerd2-proxy's `Box<dyn Body<Data = Box<dyn Buf>>>`)
- Remove dependencies on linkerd2-proxy's internal crates

[linkerd2-proxy]: https://github.com/linkerd/linkerd2-proxy/tree/main/linkerd/http/retry

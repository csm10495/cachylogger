# cachylogger

## What?

Cachylogger is a pet project that allows structured logs to be sent to a receiver for aggregation and other processing.

### Components
There is a `CachyLoggerHandler` that can be added to a regular `logging.Logger` to send log messages via a `Sender` to a `Receiver`. The `Receiver` can be on either the same or a different host.

## Why?
I've had various issues with logging:

- Logs aren't enabled when I needed them
- Logs aren't saved to the correct file
- Logs aren't easily aggregate-able across processes
- Log lines can have the same timestamp.. and later on be sorted incorrectly as a result

.. So now I have this pet project where I try to make it better.

## License

MIT License (C) 2023 - Charles Machalow

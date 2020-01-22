use std::fmt;
use std::sync::{Arc, Mutex};

use mio::{self, Evented};
use once_cell::sync::Lazy;
use slab::Slab;

use crate::io;
use crate::task::{Context, Poll, Waker};
use crate::utils::abort_on_panic;

/// Data associated with a registered I/O handle.
#[derive(Debug)]
struct Entry {
    /// A unique identifier.
    token: mio::Token,

    /// Tasks that are blocked on reading from this I/O handle.
    readers: Mutex<Vec<Waker>>,

    /// Thasks that are blocked on writing to this I/O handle.
    writers: Mutex<Vec<Waker>>,
}

/// The state of a networking driver.
struct Reactor {
    /// A mio instance that polls for new events.
    poller: mio::Poll,

    /// A collection of registered I/O handles.
    entries: Mutex<Slab<Arc<Entry>>>,

    /// Dummy I/O handle that is only used to wake up the polling thread.
    notify_reg: (mio::Registration, mio::SetReadiness),

    /// An identifier for the notification handle.
    notify_token: mio::Token,
}

impl Reactor {
    /// Creates a new reactor for polling I/O events.
    fn new() -> io::Result<Reactor> {
        let poller = mio::Poll::new()?;
        let notify_reg = mio::Registration::new2();

        let mut reactor = Reactor {
            poller,
            entries: Mutex::new(Slab::new()),
            notify_reg,
            notify_token: mio::Token(0),
        };

        // Register a dummy I/O handle for waking up the polling thread.
        let entry = reactor.register(&reactor.notify_reg.0, Interest::All, Trigger::Edge)?;
        reactor.notify_token = entry.token;

        Ok(reactor)
    }

    /// Registers an I/O event source and returns its associated entry.
    fn register(&self, source: &dyn Evented, interest: Interest, trigger: Trigger) -> io::Result<Arc<Entry>> {
        let mut entries = self.entries.lock().unwrap();

        // Reserve a vacant spot in the slab and use its key as the token value.
        let vacant = entries.vacant_entry();
        let token = mio::Token(vacant.key());

        // Allocate an entry and insert it into the slab.
        let entry = Arc::new(Entry {
            token,
            readers: Mutex::new(Vec::new()),
            writers: Mutex::new(Vec::new()),
        });
        vacant.insert(entry.clone());

        // Register the I/O event source in the poller.
        self.poller.register(source, token, interest.to_ready_set(), trigger.to_poll_opt())?;

        Ok(entry)
    }

    /// Re-register a previously registered event source with the given options.
    fn reregister(&self, source: &dyn Evented, entry: &Entry, interest: Interest, trigger: Trigger) -> io::Result<()> {
        self.poller.reregister(source, entry.token, interest.to_ready_set(), trigger.to_poll_opt())
    }

    /// Deregisters an I/O event source associated with an entry.
    fn deregister(&self, source: &dyn Evented, entry: &Entry) -> io::Result<()> {
        // Deregister the I/O object from the mio instance.
        self.poller.deregister(source)?;

        // Remove the entry associated with the I/O object.
        self.entries.lock().unwrap().remove(entry.token.0);

        Ok(())
    }

    // fn notify(&self) {
    //     self.notify_reg
    //         .1
    //         .set_readiness(mio::Ready::readable())
    //         .unwrap();
    // }
}

/// The state of the global networking driver.
static REACTOR: Lazy<Reactor> = Lazy::new(|| {
    // Spawn a thread that waits on the poller for new events and wakes up tasks blocked on I/O
    // handles.
    std::thread::Builder::new()
        .name("async-std/net".to_string())
        .spawn(move || {
            // If the driver thread panics, there's not much we can do. It is not a
            // recoverable error and there is no place to propagate it into so we just abort.
            abort_on_panic(|| {
                main_loop().expect("async networking thread has panicked");
            })
        })
        .expect("cannot start a thread driving blocking tasks");

    Reactor::new().expect("cannot initialize reactor")
});

/// Waits on the poller for new events and wakes up tasks blocked on I/O handles.
fn main_loop() -> io::Result<()> {
    let reactor = &REACTOR;
    let mut events = mio::Events::with_capacity(1000);

    loop {
        // Block on the poller until at least one new event comes in.
        reactor.poller.poll(&mut events, None)?;

        // Lock the entire entry table while we're processing new events.
        let entries = reactor.entries.lock().unwrap();

        for event in events.iter() {
            let token = event.token();

            if token == reactor.notify_token {
                // If this is the notification token, we just need the notification state.
                reactor.notify_reg.1.set_readiness(mio::Ready::empty())?;
            } else {
                // Otherwise, look for the entry associated with this token.
                if let Some(entry) = entries.get(token.0) {
                    // Set the readiness flags from this I/O event.
                    let readiness = event.readiness();

                    // Wake up reader tasks blocked on this I/O handle.
                    if !(readiness & reader_interests()).is_empty() {
                        for w in entry.readers.lock().unwrap().drain(..) {
                            w.wake();
                        }
                    }

                    // Wake up writer tasks blocked on this I/O handle.
                    if !(readiness & writer_interests()).is_empty() {
                        for w in entry.writers.lock().unwrap().drain(..) {
                            w.wake();
                        }
                    }
                }
            }
        }
    }
}

/// The kind of readiness operation to be notified about.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Interest {
    /// Notify about read readiness.
    Read,
    /// Notify about write readiness.
    Write,
    /// Notify about general readiness.
    All
}

impl Interest {
    /// Translate to mio's readiness set.
    fn to_ready_set(self) -> mio::Ready {
        match self {
            Interest::Read => reader_interests(),
            Interest::Write => writer_interests(),
            Interest::All => mio::Ready::all()
        }
    }
}

/// The trigger that causes a readiness notification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Trigger {
    /// Notify when the watched resource changes w.r.t. its interests.
    Edge,
    /// Notify as long as the watched resource matches the interests.
    Level
}

impl Trigger {
    /// Translate to mio's poll option.
    fn to_poll_opt(self) -> mio::PollOpt {
        match self {
            Trigger::Edge => mio::PollOpt::edge(),
            Trigger::Level => mio::PollOpt::level()
        }
    }
}

/// An I/O handle powered by the networking driver.
///
/// This handle wraps an I/O event source and exposes a "futurized" interface on top of it,
/// implementing traits `AsyncRead` and `AsyncWrite`.
pub struct Watcher<T: Evented> {
    /// Data associated with the I/O handle.
    entry: Arc<Entry>,

    /// The I/O event source.
    source: Option<T>,

    /// The interest of this watcher.
    interest: Interest
}

impl<T: Evented> Watcher<T> {
    /// Creates a new I/O handle.
    ///
    /// The provided I/O event source will be kept registered inside the reactor's poller for the
    /// lifetime of the returned I/O handle.
    pub fn new(source: T) -> Self {
        Watcher::new_with(source, Interest::All, Trigger::Edge)
    }

    /// Creates a new I/O handle.
    ///
    /// The provided I/O event source will be kept registered inside the reactor's poller for the
    /// lifetime of the returned I/O handle.
    ///
    /// The watcher will be notified for the given `Interest` and `Trigger`.
    ///
    /// Make sure that the methods you call on this `Watcher` are consistent with the
    /// given `Interest`. It makes no sense for example to register with `Interest::Write`
    /// and call `poll_read_with` since the I/O resource is not watched for read readiness.
    pub fn new_with(source: T, interest: Interest, trigger: Trigger) -> Self {
        Watcher {
            entry: REACTOR
                .register(&source, interest, trigger)
                .expect("cannot register an I/O event source"),
            source: Some(source),
            interest
        }
    }

    /// Returns a reference to the inner I/O event source.
    pub fn get_ref(&self) -> &T {
        self.source.as_ref().unwrap()
    }

    /// Change this Watcher's registration.
    pub fn reconfigure(&mut self, interest: Interest, trigger: Trigger) -> io::Result<()> {
        REACTOR.reregister(self.get_ref(), self.entry.as_ref(), interest, trigger)?;
        self.interest = interest;
        Ok(())
    }

    /// Polls the inner I/O source for a non-blocking read operation.
    ///
    /// If the operation returns an error of the `io::ErrorKind::WouldBlock` kind, the current task
    /// will be registered for wakeup when the I/O source becomes readable.
    pub fn poll_read_with<'a, F, R>(&'a self, cx: &mut Context<'_>, mut f: F) -> Poll<io::Result<R>>
    where
        F: FnMut(&'a T) -> io::Result<R>,
    {
        debug_assert!(self.interest == Interest::Read || self.interest == Interest::All);

        // If the operation isn't blocked, return its result.
        match f(self.source.as_ref().unwrap()) {
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
            res => return Poll::Ready(res),
        }

        // Lock the waker list.
        let mut list = self.entry.readers.lock().unwrap();

        // Try running the operation again.
        match f(self.source.as_ref().unwrap()) {
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
            res => return Poll::Ready(res),
        }

        // Register the task if it isn't registered already.
        if list.iter().all(|w| !w.will_wake(cx.waker())) {
            list.push(cx.waker().clone());
        }

        Poll::Pending
    }

    /// Polls the inner I/O source for a non-blocking write operation.
    ///
    /// If the operation returns an error of the `io::ErrorKind::WouldBlock` kind, the current task
    /// will be registered for wakeup when the I/O source becomes writable.
    pub fn poll_write_with<'a, F, R>(
        &'a self,
        cx: &mut Context<'_>,
        mut f: F,
    ) -> Poll<io::Result<R>>
    where
        F: FnMut(&'a T) -> io::Result<R>,
    {
        debug_assert!(self.interest == Interest::Write || self.interest == Interest::All);

        // If the operation isn't blocked, return its result.
        match f(self.source.as_ref().unwrap()) {
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
            res => return Poll::Ready(res),
        }

        // Lock the waker list.
        let mut list = self.entry.writers.lock().unwrap();

        // Try running the operation again.
        match f(self.source.as_ref().unwrap()) {
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
            res => return Poll::Ready(res),
        }

        // Register the task if it isn't registered already.
        if list.iter().all(|w| !w.will_wake(cx.waker())) {
            list.push(cx.waker().clone());
        }

        Poll::Pending
    }

    /// Notify the given Context's `Waker` when the I/O resource becomes
    /// available for the given `Interest`.
    pub fn notify_for(&self, cx: &mut Context<'_>, interest: Interest) {
        if interest == Interest::Read || interest == Interest::All {
            debug_assert!(self.interest == Interest::Read || self.interest == Interest::All);
            let mut list = self.entry.readers.lock().unwrap();
            if list.iter().all(|w| !w.will_wake(cx.waker())) {
                list.push(cx.waker().clone());
            }
        }
        if interest == Interest::Write || interest == Interest::All {
            debug_assert!(self.interest == Interest::Write || self.interest == Interest::All);
            let mut list = self.entry.writers.lock().unwrap();
            if list.iter().all(|w| !w.will_wake(cx.waker())) {
                list.push(cx.waker().clone());
            }
        }
    }

    /// Deregisters and returns the inner I/O source.
    ///
    /// This method is typically used to convert `Watcher`s to raw file descriptors/handles.
    #[allow(dead_code)]
    pub fn into_inner(mut self) -> T {
        let source = self.source.take().unwrap();
        REACTOR
            .deregister(&source, &self.entry)
            .expect("cannot deregister I/O event source");
        source
    }
}

impl<T: Evented> Drop for Watcher<T> {
    fn drop(&mut self) {
        if let Some(ref source) = self.source {
            REACTOR
                .deregister(source, &self.entry)
                .expect("cannot deregister I/O event source");
        }
    }
}

impl<T: Evented + fmt::Debug> fmt::Debug for Watcher<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Watcher")
            .field("entry", &self.entry)
            .field("source", &self.source)
            .finish()
    }
}

/// Returns a mask containing flags that interest tasks reading from I/O handles.
#[inline]
fn reader_interests() -> mio::Ready {
    mio::Ready::all() - mio::Ready::writable()
}

/// Returns a mask containing flags that interest tasks writing into I/O handles.
#[inline]
fn writer_interests() -> mio::Ready {
    mio::Ready::writable() | hup()
}

/// Returns a flag containing the hangup status.
#[inline]
fn hup() -> mio::Ready {
    #[cfg(unix)]
    let ready = mio::unix::UnixReady::hup().into();

    #[cfg(not(unix))]
    let ready = mio::Ready::empty();

    ready
}

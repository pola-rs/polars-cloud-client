use std::backtrace::{Backtrace, BacktraceStatus};
use std::panic::PanicHookInfo;

pub fn install_panic_hook() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        report_panic(panic_info);
        default_hook(panic_info);
    }));
}

pub fn report_panic(panic_info: &PanicHookInfo<'_>) {
    let payload = panic_info.payload();
    let payload = payload
        .downcast_ref::<&str>()
        .map(|s| &**s)
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str));

    let location = panic_info.location().map(|l| l.to_string());
    let backtrace = Backtrace::capture();
    let backtrace_display = tracing::field::display(&backtrace);
    let note = (backtrace.status() == BacktraceStatus::Disabled)
        .then_some("run with RUST_BACKTRACE=1 environment variable to display a backtrace");

    tracing::error!(
        exception.message = payload,
        exception.stacktrace = backtrace_display,
        panic.payload = payload,
        panic.location = location,
        panic.backtrace = backtrace_display,
        panic.note = note,
        "A panic occurred"
    );
}

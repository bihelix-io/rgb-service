use super::*;
use std::cell::Cell;

thread_local! {
    static LIVE_PAYLOADS: Cell<usize> = const { Cell::new(0) };
}

struct Payload(Vec<u8>);

impl Drop for Payload {
    fn drop(&mut self) {
        // Reading the buffer also makes it clear that the probe owns real data.
        assert_eq!(self.0.len(), 8192);
        LIVE_PAYLOADS.with(|live| live.set(live.get() - 1));
    }
}

fn tracked_result() -> Dynamic {
    LIVE_PAYLOADS.with(|live| live.set(live.get() + 1));
    let result = ok(json!({
        "results": (0..100).map(|index| json!({
            "address": format!("mock-address-{index}"), "deposits": []
        })).collect::<Vec<_>>(),
        "deposits": []
    }));
    result.insert("_lifetime_probe", Dynamic::custom(Payload(vec![0; 8192])));
    result
}

extern "C" fn no_arg() -> *const Dynamic {
    native_result(|| Ok(tracked_result()))
}

extern "C" fn with_arg(input: *const Dynamic) -> *const Dynamic {
    native_dynamic_result(input, |_| Ok(tracked_result()))
}

extern "C" fn with_string(input: *const Dynamic) -> *const Dynamic {
    native_string_dynamic_result(input, |_| Ok(tracked_result()))
}

extern "C" fn plain_error() -> *const Dynamic {
    native_result(|| bail!("mock native error"))
}

extern "C" fn input_error(input: *const Dynamic) -> *const Dynamic {
    native_dynamic_result(input, |_| bail!("mock input error"))
}

extern "C" fn invalid_string(input: *const Dynamic) -> *const Dynamic {
    native_string_dynamic_result(input, |_| bail!("unexpected string"))
}

#[test]
fn actual_native_wrappers_release_each_round_and_preserve_escaped_values() -> Result<()> {
    let vm = Vm::with_all()?;
    {
        let mut jit = vm.jit.write();
        for (name, args, callback) in [
            ("no_arg", &[][..], no_arg as *const u8),
            ("with_arg", &[Type::Any][..], with_arg as *const u8),
            ("with_string", &[Type::Str][..], with_string as *const u8),
            ("plain_error", &[][..], plain_error as *const u8),
            ("input_error", &[Type::Any][..], input_error as *const u8),
            ("invalid_string", &[Type::Any][..], invalid_string as *const u8),
        ] {
            jit.add_native_module_ptr("lifetime_probe", name, args, Type::Any, callback)?;
        }
    }
    vm.import_source("native_lifetime", r#"
        pub fn nested() { lifetime_probe::no_arg() }
        pub fn round() {
            let mut count = 0;
            while count < 32 {
                let plain = lifetime_probe::no_arg();
                let input = lifetime_probe::with_arg({ page: count });
                let text = lifetime_probe::with_string("mock-batch");
                let nested = nested();
                if plain.ok != true || input.ok != true || text.ok != true || nested.ok != true {
                    return -1 as i64;
                }
                if plain.results.len() != 100 || nested.results.len() != 100 { return -2 as i64; }
                count += 1;
            }
            count as i64
        }
        pub fn escaped() { nested() }
        pub fn errors() {
            let plain = lifetime_probe::plain_error();
            let input = lifetime_probe::input_error({});
            let text = lifetime_probe::invalid_string(7);
            if plain.ok == false && plain.error == "mock native error"
                && input.ok == false && input.error == "mock input error"
                && text.ok == false && text.error == "expected string argument" { 1 as i64 } else { 0 as i64 }
        }
    "#)?;
    let (compiled, return_type) = vm.jit.write().get_fn_ptr("native_lifetime::round", &[])?;
    assert_eq!(return_type, Type::I64);
    let round: extern "C" fn() -> i64 = unsafe { std::mem::transmute(compiled) };
    for cycle in 0..64 {
        assert_eq!(round(), 32, "mock scan cycle {cycle}");
        LIVE_PAYLOADS.with(|live| assert_eq!(live.get(), 0, "payload leaked in cycle {cycle}"));
    }
    let (compiled, return_type) = vm.jit.write().get_fn_ptr("native_lifetime::escaped", &[])?;
    assert_eq!(return_type, Type::Any);
    let escaped: extern "C" fn() -> *const Dynamic = unsafe { std::mem::transmute(compiled) };
    let value = unsafe { vm::take_dynamic_return(escaped()) };
    assert_eq!(value.get_dynamic("ok").unwrap().to_string(), "true");
    LIVE_PAYLOADS.with(|live| assert_eq!(live.get(), 1));
    drop(value);
    LIVE_PAYLOADS.with(|live| assert_eq!(live.get(), 0));

    let (compiled, return_type) = vm.jit.write().get_fn_ptr("native_lifetime::errors", &[])?;
    assert_eq!(return_type, Type::I64);
    let errors: extern "C" fn() -> i64 = unsafe { std::mem::transmute(compiled) };
    for _ in 0..256 {
        assert_eq!(errors(), 1, "error payload contract must stay unchanged");
    }
    Ok(())
}

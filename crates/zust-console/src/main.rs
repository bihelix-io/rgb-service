mod modules;

use std::{env, fs, path::PathBuf};

use anyhow::{Context, Result};
use dynamic::{Dynamic, FromJson, ToJson};

fn main() {
    if let Err(err) = run() {
        eprintln!("zust-console error: {err:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let mut args = env::args().skip(1);
    let Some(script) = args.next() else {
        print_help();
        return Ok(());
    };
    if script == "--help" || script == "-h" {
        print_help();
        return Ok(());
    }
    let arg = match args.next() {
        Some(json) => json_to_dynamic(&json).context("decode arg JSON")?,
        None => Dynamic::Null,
    };
    let result = run_script(PathBuf::from(script), arg)?;
    let json = dynamic_to_json(&result);
    println!("{}", serde_json::to_string_pretty(&serde_json::from_str::<serde_json::Value>(&json)?)?);
    Ok(())
}

fn print_help() {
    eprintln!("usage: zust-console <script.zs> <daemon-config-json>");
    eprintln!(
        "{}",
        r#"example: cargo run -p zust-console -- crates/zust-console/examples/rgb-service-flow.zs '{"daemon_url":"http://127.0.0.1:8787","btc_addr":"bcrt1..."}'"#
    );
}

fn run_script(path: PathBuf, arg: Dynamic) -> Result<Dynamic> {
    modules::configure_console(&arg).context("configure Zust console")?;
    let vm = vm::Vm::with_all().context("initialize Zust VM")?;
    modules::register_console_modules(&vm).context("register Zust console modules")?;
    let code = fs::read(&path).with_context(|| format!("read {}", path.display()))?;
    let (fn_ptr, ty) = vm
        .jit
        .write()
        .map_err(|_| anyhow::anyhow!("Zust VM JIT lock poisoned"))?
        .load(code, "arg".into())
        .context("compile Zust script")?;
    let result = dynamic::call_fn(fn_ptr, ty, Box::new(arg)).context("execute Zust script")?;
    Ok(*result)
}

fn json_to_dynamic(value: &str) -> Result<Dynamic> {
    let (dynamic, consumed) = Dynamic::from_json(value.as_bytes())?;
    ensure_consumed(value, consumed)?;
    Ok(dynamic)
}

fn dynamic_to_json(value: &Dynamic) -> String {
    let mut json = String::new();
    value.to_json(&mut json);
    json
}

fn ensure_consumed(input: &str, consumed: usize) -> Result<()> {
    let rest = input
        .as_bytes()
        .get(consumed..)
        .context("invalid consumed length from Zust JSON decoder")?;
    if rest.iter().all(|byte| byte.is_ascii_whitespace()) {
        Ok(())
    } else {
        anyhow::bail!("trailing data after JSON input")
    }
}

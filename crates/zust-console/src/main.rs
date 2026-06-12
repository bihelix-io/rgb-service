mod modules;

use std::{env, fs, path::PathBuf};

use anyhow::{Context, Result};
use dynamic::{Dynamic, ToJson};

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
    if args.next().is_some() {
        anyhow::bail!("unexpected extra argument; usage: zust-console <script.zs>");
    }
    let result = run_script(PathBuf::from(script), Dynamic::Null)?;
    let json = dynamic_to_json(&result);
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::from_str::<serde_json::Value>(&json)?)?
    );
    Ok(())
}

fn print_help() {
    eprintln!("usage: zust-console <script.zs>");
    eprintln!(
        "{}",
        r#"example: cargo run -p zust-console -- crates/zust-console/start.zs"#
    );
}

fn run_script(path: PathBuf, arg: Dynamic) -> Result<Dynamic> {
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

fn dynamic_to_json(value: &Dynamic) -> String {
    let mut json = String::new();
    value.to_json(&mut json);
    json
}

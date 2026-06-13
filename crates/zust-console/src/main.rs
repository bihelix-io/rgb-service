mod modules;

use std::{
    env, fs,
    io::{self, Write},
    path::PathBuf,
};

use anyhow::{Context, Result};
use dynamic::Dynamic;
use vm::Vm;

fn main() {
    if let Err(err) = run() {
        eprintln!("zust-console error: {err:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if matches!(args.first().map(String::as_str), Some("--help" | "-h")) {
        print_help();
        return Ok(());
    }

    match args.as_slice() {
        [] => run_repl(None),
        [flag, code] if flag == "--eval" || flag == "-e" => {
            let vm = init_vm()?;
            let result = eval_code(&vm, code, Dynamic::Null)?;
            print_dynamic(&result)
        }
        [flag, path] if flag == "--once" => {
            let vm = init_vm()?;
            let result = run_file(&vm, PathBuf::from(path), Dynamic::Null)?;
            print_dynamic(&result)
        }
        [flag, path] if flag == "--repl" => run_repl(Some(PathBuf::from(path))),
        [script] => run_repl(Some(PathBuf::from(script))),
        _ => anyhow::bail!(
            "unexpected arguments; usage: zust-console [start.zs] | zust-console --once <script.zs> | zust-console -e <code>"
        ),
    }
}

fn print_help() {
    eprintln!("usage: zust-console [start.zs]");
    eprintln!(
        "{}",
        r#"examples:
  cargo run -p zust-console -- crates/zust-console/start.zs
  cargo run -p zust-console -- --once crates/zust-console/start.zs
  cargo run -p zust-console -- -e 'ln::status()'"#
    );
}

fn init_vm() -> Result<Vm> {
    let vm = Vm::with_all().context("initialize Zust VM")?;
    modules::register_console_modules(&vm).context("register Zust console modules")?;
    Ok(vm)
}

fn run_repl(init_script: Option<PathBuf>) -> Result<()> {
    let mut vm = init_vm()?;
    if let Some(path) = init_script {
        let result = run_file(&vm, path.clone(), Dynamic::Null)
            .with_context(|| format!("run init script {}", path.display()))?;
        print!("init ");
        print_dynamic(&result)?;
    }

    eprintln!("zust-console REPL. Type :help for commands, :quit to exit.");
    let stdin = io::stdin();
    let mut buffer = String::new();
    loop {
        if buffer.is_empty() {
            print!("zust> ");
        } else {
            print!("....> ");
        }
        io::stdout().flush()?;

        let mut line = String::new();
        if stdin.read_line(&mut line)? == 0 {
            println!();
            break;
        }
        let trimmed = line.trim_end();
        if buffer.is_empty() && trimmed.starts_with(':') {
            if handle_repl_command(trimmed, &mut vm)? {
                break;
            }
            continue;
        }
        if buffer.is_empty() && trimmed.trim().is_empty() {
            continue;
        }

        let explicit_continue = trimmed.ends_with('\\');
        if explicit_continue {
            buffer.push_str(trimmed.trim_end_matches('\\'));
            buffer.push('\n');
            continue;
        }
        buffer.push_str(&line);
        if needs_more_input(&buffer) {
            continue;
        }

        match eval_code(&vm, &buffer, Dynamic::Null) {
            Ok(result) => print_dynamic(&result)?,
            Err(err) => eprintln!("error: {err:#}"),
        }
        buffer.clear();
    }
    Ok(())
}

fn handle_repl_command(command: &str, vm: &mut Vm) -> Result<bool> {
    let (name, arg) = command
        .split_once(char::is_whitespace)
        .map(|(name, arg)| (name, arg.trim()))
        .unwrap_or((command, ""));
    match name {
        ":q" | ":quit" | ":exit" => Ok(true),
        ":h" | ":help" => {
            eprintln!(
                "{}",
                r#"commands:
  :load <path>   run a .zs file in this REPL VM
  :reset         reset the VM and native console modules
  :quit          exit

Any other input is compiled and executed as Zust code."#
            );
            Ok(false)
        }
        ":load" => {
            if arg.is_empty() {
                eprintln!("error: :load needs a path");
            } else {
                match run_file(vm, PathBuf::from(arg), Dynamic::Null) {
                    Ok(result) => print_dynamic(&result)?,
                    Err(err) => eprintln!("error: {err:#}"),
                }
            }
            Ok(false)
        }
        ":reset" => {
            *vm = init_vm()?;
            eprintln!("VM reset");
            Ok(false)
        }
        _ => {
            eprintln!("unknown command: {name}");
            Ok(false)
        }
    }
}

fn run_file(vm: &Vm, path: PathBuf, arg: Dynamic) -> Result<Dynamic> {
    let code = fs::read(&path).with_context(|| format!("read {}", path.display()))?;
    eval_code(
        vm,
        &String::from_utf8(code).context("Zust script is not UTF-8")?,
        arg,
    )
}

fn eval_code(vm: &Vm, code: &str, arg: Dynamic) -> Result<Dynamic> {
    let (fn_ptr, ty) = vm
        .jit
        .write()
        .map_err(|_| anyhow::anyhow!("Zust VM JIT lock poisoned"))?
        .load(code.as_bytes().to_vec(), "arg".into())
        .context("compile Zust code")?;
    let result = dynamic::call_fn(fn_ptr, ty, Box::new(arg)).context("execute Zust script")?;
    Ok(*result)
}

fn print_dynamic(value: &Dynamic) -> Result<()> {
    println!("{}", value.to_string());
    Ok(())
}

fn needs_more_input(code: &str) -> bool {
    let mut stack = Vec::new();
    let mut chars = code.chars().peekable();
    let mut in_string = false;
    let mut escaped = false;
    while let Some(ch) = chars.next() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        if ch == '"' {
            in_string = true;
            continue;
        }
        if ch == '/' && matches!(chars.peek(), Some('/')) {
            for next in chars.by_ref() {
                if next == '\n' {
                    break;
                }
            }
            continue;
        }
        match ch {
            '(' | '[' | '{' => stack.push(ch),
            ')' => {
                stack.pop();
            }
            ']' => {
                stack.pop();
            }
            '}' => {
                stack.pop();
            }
            _ => {}
        }
    }
    in_string || !stack.is_empty()
}

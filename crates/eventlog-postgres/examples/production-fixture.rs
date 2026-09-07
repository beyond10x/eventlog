//! Configure TLS only on an explicitly named disposable PostgreSQL container.
use clap::Parser;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};
#[derive(Parser)]
struct Args {
    #[arg(long)]
    container: String,
    #[arg(long)]
    directory: PathBuf,
}
fn command(program: &str, args: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(program).args(args).output()?;
    if !output.status.success() {
        return Err(format!("fixture {program} failed with {:?}", output.status.code()).into());
    }
    Ok(())
}
fn path(path: &Path) -> Result<&str, Box<dyn std::error::Error>> {
    path.to_str()
        .ok_or_else(|| "fixture path must be UTF-8".into())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    eventlog_postgres::PostgresConfig::isolated(
        &std::env::var("EVENTLOG_TEST_POSTGRES_URL")?,
        "fixture",
    )?;
    fs::create_dir(&args.directory)?;
    fs::set_permissions(&args.directory, fs::Permissions::from_mode(0o700))?;
    let directory = fs::canonicalize(args.directory)?;
    let ca_key = directory.join("ca.key");
    let ca = directory.join("ca.crt");
    let key = directory.join("server.key");
    let request = directory.join("server.csr");
    let cert = directory.join("server.crt");
    let extensions = directory.join("server.ext");
    fs::write(
        &extensions,
        b"subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n",
    )?;
    command(
        "openssl",
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "2",
            "-subj",
            "/CN=Eventlog Disposable Test CA",
            "-keyout",
            path(&ca_key)?,
            "-out",
            path(&ca)?,
        ],
    )?;
    command(
        "openssl",
        &[
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-subj",
            "/CN=localhost",
            "-keyout",
            path(&key)?,
            "-out",
            path(&request)?,
        ],
    )?;
    command(
        "openssl",
        &[
            "x509",
            "-req",
            "-in",
            path(&request)?,
            "-CA",
            path(&ca)?,
            "-CAkey",
            path(&ca_key)?,
            "-CAcreateserial",
            "-days",
            "2",
            "-extfile",
            path(&extensions)?,
            "-out",
            path(&cert)?,
        ],
    )?;
    fs::set_permissions(&ca_key, fs::Permissions::from_mode(0o600))?;
    fs::set_permissions(&key, fs::Permissions::from_mode(0o600))?;
    command(
        "docker",
        &[
            "exec",
            &args.container,
            "mkdir",
            "-p",
            "/var/lib/postgresql/test-tls",
        ],
    )?;
    command(
        "docker",
        &[
            "cp",
            path(&cert)?,
            &format!("{}:/var/lib/postgresql/test-tls/server.crt", args.container),
        ],
    )?;
    command(
        "docker",
        &[
            "cp",
            path(&key)?,
            &format!("{}:/var/lib/postgresql/test-tls/server.key", args.container),
        ],
    )?;
    command(
        "docker",
        &[
            "exec",
            &args.container,
            "chown",
            "postgres:postgres",
            "/var/lib/postgresql/test-tls/server.key",
            "/var/lib/postgresql/test-tls/server.crt",
        ],
    )?;
    command(
        "docker",
        &[
            "exec",
            &args.container,
            "chmod",
            "600",
            "/var/lib/postgresql/test-tls/server.key",
        ],
    )?;
    let config: tokio_postgres::Config = std::env::var("EVENTLOG_TEST_POSTGRES_URL")?.parse()?;
    let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
    let driver = tokio::spawn(connection);
    for sql in [
        "ALTER SYSTEM SET ssl_cert_file='/var/lib/postgresql/test-tls/server.crt'",
        "ALTER SYSTEM SET ssl_key_file='/var/lib/postgresql/test-tls/server.key'",
        "ALTER SYSTEM SET ssl=on",
        "ALTER SYSTEM SET shared_preload_libraries='pg_stat_statements'",
        "ALTER SYSTEM SET track_io_timing=on",
    ] {
        client.batch_execute(sql).await?;
    }
    client.simple_query("SELECT pg_reload_conf()").await?;
    drop(client);
    driver.await??;
    command("docker", &["restart", "--time", "1", &args.container])?;
    let inspected = Command::new("docker")
        .args([
            "inspect",
            "--format",
            "{{(index (index .NetworkSettings.Ports \"5432/tcp\") 0).HostPort}}",
            &args.container,
        ])
        .output()?;
    if !inspected.status.success() {
        return Err("test port discovery failed".into());
    }
    let port = String::from_utf8(inspected.stdout)?.trim().parse::<u16>()?;
    let url = format!("postgresql://postgres@127.0.0.1:{port}/postgres");
    let started = std::time::Instant::now();
    loop {
        if let Ok(Ok((client, connection))) = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            tokio_postgres::connect(&url, tokio_postgres::NoTls),
        )
        .await
        {
            let driver = tokio::spawn(connection);
            client
                .batch_execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
                .await?;
            drop(client);
            driver.await??;
            break;
        }
        if started.elapsed() > std::time::Duration::from_secs(15) {
            return Err("test fixture restart deadline".into());
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    println!(
        "{}",
        serde_json::json!({"fixture":"disposable-postgres-tls","certificate_authority":ca,"observed_local_port":port,"private_keys":"retained only in the explicit private fixture directory and disposable container"})
    );
    Ok(())
}

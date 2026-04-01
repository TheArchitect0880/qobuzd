use anyhow::Result;
use clap::{Parser, Subcommand};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, Level};
use tracing_subscriber::FmtSubscriber;

use qobuzd::api::QobuzApi;
use qobuzd::auth::QobuzAuth;
use qobuzd::config::Config;
use qobuzd::qconnect::QConnect;

#[derive(Parser)]
#[command(name = "qobuzd")]
#[command(about = "Qobuz Connect client for Linux")]
struct Cli {
    #[arg(short, long, default_value = "qobuzd")]
    name: String,

    #[command(subcommand)]
    command: Commands,

    #[arg(short, long, default_value = "info")]
    log_level: String,
}

#[derive(Subcommand)]
enum Commands {
    Login {
        #[arg(short, long)]
        email: String,
        #[arg(short, long)]
        password: String,
    },
    Logout,
    Status,
    User,
    Serve,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let level = match cli.log_level.as_str() {
        "debug" | "trace" => Level::DEBUG,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };
    tracing::subscriber::set_global_default(
        FmtSubscriber::builder()
            .with_max_level(level)
            .with_target(false)
            .with_thread_ids(false)
            .with_file(true)
            .with_line_number(true)
            .finish(),
    )
    .expect("failed to set subscriber");

    let config = Config::new(cli.name.clone())?;
    let auth = Arc::new(Mutex::new(QobuzAuth::new(config.clone())));

    match cli.command {
        Commands::Login { email, password } => {
            println!("Logging in as {}...", email);
            let auth_guard = auth.lock().await;
            match auth_guard.login_with_credentials(&email, &password).await {
                Ok(user) => {
                    println!(
                        "Logged in as: {} (id: {})",
                        user.display_name.unwrap_or_default(),
                        user.id
                    );
                }
                Err(e) => {
                    error!("Login failed: {}", e);
                    std::process::exit(1);
                }
            }
        }

        Commands::Logout => {
            let guard = auth.lock().await;
            guard.logout().await?;
            println!("Logged out");
        }

        Commands::Status => {
            let guard = auth.lock().await;
            if guard.is_linked().await {
                println!("Device is linked");
            } else {
                println!("Device is not linked");
            }
        }

        Commands::User => {
            let guard = auth.lock().await;
            let token = guard.get_valid_token().await?;
            drop(guard);
            let api = QobuzApi::new(&config);
            match api.get_user(&token).await {
                Ok(user) => {
                    println!("User: {}", user.display_name.unwrap_or_default());
                    println!("Email: {}", user.email);
                    if let Some(sub) = &user.subscription {
                        println!("Subscription: {}", sub.offer);
                    }
                }
                Err(e) => {
                    error!("Failed: {}", e);
                    std::process::exit(1);
                }
            }
        }

        Commands::Serve => {
            let guard = auth.lock().await;
            let token = match guard.get_valid_token().await {
                Ok(t) => t,
                Err(e) => {
                    error!("Not logged in: {}", e);
                    println!("Run 'qobuzd login' first.");
                    std::process::exit(1);
                }
            };
            drop(guard);

            let device_id = config.device_id.clone();
            let device_name = config.device_name.clone();

            println!("Starting QobuzD as '{}'...", device_name);

            let qconnect = QConnect::start(token, device_id, device_name);

            println!("QobuzD is running. Select it in the Qobuz app to play music.");
            println!("Press Ctrl+C to stop.");

            // QConnect handles commands internally; nothing to poll.
            let _ = qconnect;

            tokio::signal::ctrl_c().await?;
            println!("\nStopped.");
        }
    }

    Ok(())
}

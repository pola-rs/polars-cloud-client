use std::io;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use clap::builder::{NonEmptyStringValueParser, TypedValueParser, ValueParser};
use clap::{Args, CommandFactory, FromArgMatches, Parser, Subcommand};
use client_core::{AutoRefreshApiControlPlaneClient, Client, RUNTIME};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use crate::compute::{
    print_compute_cluster_details, print_compute_clusters, start_compute_cluster,
    stop_compute_cluster,
};
use crate::organization::{
    delete_organization, print_organization_details, print_organizations, set_up_organization,
};
use crate::service_account::create_service_account;
use crate::setup::{WorkspaceKind, setup};
use crate::workspace::{
    delete_workspace, print_workspace_details, print_workspaces, verify_workspace,
};

pub mod compute;
pub mod organization;
pub mod service_account;
pub mod setup;
pub mod workspace;

async fn get_user_input(prompt: &str) -> anyhow::Result<String> {
    print!("{prompt}");
    io::stdout().flush()?;
    let mut input = String::new();
    let mut reader = tokio::io::BufReader::new(tokio::io::stdin());
    tokio::select! {
        result = tokio::io::AsyncBufReadExt::read_line(&mut reader, &mut input) => {
            result?;
            Ok(input.trim().to_string())
        },
        _ = tokio::signal::ctrl_c() => {
            std::process::exit(130)
        },
    }
}

#[derive(Parser)]
#[command(name = "pc")]
#[command(about = "Command line interface for Polars Cloud", long_about = None)]
#[command(version)]
#[command(arg_required_else_help = true)]
struct Cli {
    #[arg(short, long, global = true, help = "Output debug logging messages.")]
    verbose: bool,
    #[arg(
        short,
        long,
        global = true,
        help = "Authentication token to override other auth methods"
    )]
    token: Option<String>,
    #[arg(
        short = 'p',
        long,
        global = true,
        help = "Path to authentication token file"
    )]
    token_path: Option<String>,
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Authenticate with Polars Cloud
    Authenticate,
    /// Login through the browser
    Login,
    /// Set up organization and workspace
    Setup {
        #[arg(short, long)]
        organization_name: Option<String>,
        #[arg(short, long)]
        workspace_name: Option<String>,
        #[arg(long)]
        workspace_type: Option<WorkspaceKind>,
        #[arg(long, default_value_t = false)]
        no_verify: bool,
    },
    /// Manage Polars Cloud organizations
    Organization(OrganizationArgs),
    /// Manage Polars Cloud workspaces
    Workspace(WorkspaceArgs),
    /// Manage Polars Cloud compute clusters
    Compute(ComputeArgs),
    /// Manage Polars Cloud service accounts
    ServiceAccount(ServiceAccountArgs),
}

// --- Organization ---

#[derive(Args)]
struct OrganizationArgs {
    #[command(subcommand)]
    command: OrganizationCommands,
}

#[derive(Subcommand)]
enum OrganizationCommands {
    /// List all active organizations
    List,
    /// Set up an organization
    Setup {
        #[arg(short, long)]
        name: Option<String>,
    },
    /// Delete an organization
    Delete {
        #[arg(short, long, required = true)]
        name: String,
    },
    /// Print details of an organization
    Details {
        #[arg(short, long, required = true)]
        name: String,
    },
}

// --- Workspace ---

#[derive(Args)]
struct WorkspaceArgs {
    #[command(subcommand)]
    command: WorkspaceCommands,
}

#[derive(Subcommand)]
enum WorkspaceCommands {
    /// List all active workspaces
    List,
    /// Set up a workspace
    Setup {
        #[arg(short, long)]
        workspace_name: Option<String>,
        #[arg(short, long)]
        organization_name: Option<String>,
        #[arg(short = 't', long)]
        workspace_type: Option<WorkspaceKind>,
        #[arg(long, default_value_t = false)]
        no_verify: bool,
    },
    /// Verify workspace setup
    Verify {
        #[arg(short, long)]
        organization_name: Option<String>,
        #[arg(short, long, required = true)]
        workspace_name: String,
        #[arg(short, long)]
        interval: Option<u64>,
        #[arg(short = 'd', long)]
        timeout: Option<u64>,
    },
    /// Delete a workspace
    Delete {
        #[arg(short, long)]
        organization_name: Option<String>,
        #[arg(short, long, required = true)]
        workspace_name: String,
    },
    /// Print details of a workspace
    Details {
        #[arg(short, long)]
        organization_name: Option<String>,
        #[arg(short, long, required = true)]
        workspace_name: String,
    },
}

// --- Service Account ---

#[derive(Args)]
struct ServiceAccountArgs {
    #[command(subcommand)]
    command: ServiceAccountCommands,
}

#[derive(Subcommand)]
enum ServiceAccountCommands {
    /// Create a new service account for a workspace
    Create {
        #[arg(short, long)]
        organization_name: Option<String>,
        #[arg(short, long, required = true)]
        workspace_name: String,
        #[arg(short, long, required = true)]
        name: String,
        #[arg(short, long)]
        description: Option<String>,
    },
}

// --- Compute ---

#[derive(Args)]
struct ComputeArgs {
    #[command(subcommand)]
    command: ComputeCommands,
}

fn parse_env_override() -> ValueParser {
    NonEmptyStringValueParser::new()
        .try_map(|value| {
            let (key, value) = value
                .split_once("=")
                .and_then(|(key, value)| {
                    (!key.is_empty() && !value.is_empty()).then_some((key, value))
                })
                .ok_or("Expected `<KEY>=<value>`")?;
            if key.is_empty() || value.is_empty() {
                return Err("");
            }
            Ok::<(String, String), &'static str>((key.to_owned(), value.to_owned()))
        })
        .into()
}

#[derive(Subcommand)]
enum ComputeCommands {
    /// List available compute clusters
    List,
    /// Start a compute cluster
    Start {
        #[arg(short, long)]
        organization_name: Option<String>,
        #[arg(short, long)]
        workspace_name: Option<String>,
        #[arg(short, long)]
        cpus: Option<u32>,
        #[arg(short, long)]
        memory: Option<u32>,
        #[arg(short = 'n', long)]
        instance_type: Option<String>,
        #[arg(short, long)]
        storage: Option<u32>,
        #[arg(long, default_value_t = 1)]
        cluster_size: u32,
        #[arg(short, long, value_parser = parse_env_override())]
        env_override: Vec<(String, String)>,
        #[arg(long)]
        wait: bool,
    },
    /// Stop a compute cluster
    Stop {
        #[arg(short, long)]
        organization_name: Option<String>,
        #[arg(short, long)]
        workspace_name: String,
        #[arg(short, long, required = true)]
        id: Uuid,
    },
    /// Print details of a compute cluster
    Details {
        #[arg(short, long)]
        organization_name: Option<String>,
        #[arg(short, long)]
        workspace_name: String,
        #[arg(short, long, required = true)]
        id: Uuid,
    },
}

pub fn entrypoint(args: Vec<String>) -> anyhow::Result<()> {
    RUNTIME.0.block_on(async {
        tokio::select! {
            result = async_main(args) => result,
            _ = tokio::signal::ctrl_c() => {
                std::process::exit(130)
            },
        }
    })
}

async fn async_main(args: Vec<String>) -> anyhow::Result<()> {
    let matches = Cli::command().get_matches_from(args);
    let cli = Cli::from_arg_matches(&matches)?;

    let Some(command) = cli.command else {
        println!("No command provided. Use --help for usage.");
        return Ok(());
    };

    if cli.verbose {
        tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug")),
            )
            .init();
        println!("Logging level set to DEBUG");
    }

    let client = AutoRefreshApiControlPlaneClient::default();

    if let Some(token) = cli.token {
        client.set_token_override(token);
    }

    if let Some(token_path) = cli.token_path {
        client.set_token_path_override(PathBuf::from(token_path));
    }

    let client: Client = Arc::new(client);

    match command {
        Commands::Authenticate => client.authenticate(None, None, true).await?,
        Commands::Login => client.login().await?,
        Commands::Setup {
            organization_name,
            workspace_name,
            workspace_type,
            no_verify,
        } => {
            setup(
                &client,
                organization_name,
                workspace_name,
                workspace_type,
                !no_verify,
            )
            .await?
        },
        Commands::Organization(args) => match args.command {
            OrganizationCommands::List => print_organizations(&client).await?,
            OrganizationCommands::Setup { name } => {
                set_up_organization(&client, name).await?;
            },
            OrganizationCommands::Delete { name } => delete_organization(&client, name).await?,
            OrganizationCommands::Details { name } => {
                print_organization_details(&client, name).await?
            },
        },
        Commands::Workspace(args) => match args.command {
            WorkspaceCommands::List => print_workspaces(&client).await?,
            WorkspaceCommands::Setup {
                organization_name,
                workspace_name,
                workspace_type,
                no_verify,
            } => {
                setup(
                    &client,
                    organization_name,
                    workspace_name,
                    workspace_type,
                    !no_verify,
                )
                .await?
            },
            WorkspaceCommands::Verify {
                organization_name,
                workspace_name,
                interval,
                timeout,
            } => {
                verify_workspace(
                    &client,
                    organization_name,
                    workspace_name,
                    interval,
                    timeout,
                )
                .await?
            },
            WorkspaceCommands::Delete {
                organization_name,
                workspace_name,
            } => delete_workspace(&client, organization_name, workspace_name).await?,
            WorkspaceCommands::Details {
                organization_name,
                workspace_name,
            } => print_workspace_details(&client, organization_name, workspace_name).await?,
        },
        Commands::ServiceAccount(args) => match args.command {
            ServiceAccountCommands::Create {
                organization_name,
                workspace_name,
                name,
                description,
            } => {
                create_service_account(
                    &client,
                    organization_name,
                    workspace_name,
                    name,
                    description,
                )
                .await?
            },
        },
        Commands::Compute(args) => match args.command {
            ComputeCommands::Start {
                organization_name,
                workspace_name,
                cpus,
                memory,
                instance_type,
                storage,
                cluster_size,
                env_override,
                wait,
            } => {
                start_compute_cluster(
                    &client,
                    organization_name,
                    workspace_name,
                    cpus,
                    memory,
                    instance_type,
                    storage,
                    cluster_size,
                    env_override.into_iter().collect(),
                    wait,
                )
                .await?
            },
            ComputeCommands::Stop {
                organization_name,
                workspace_name,
                id,
            } => stop_compute_cluster(&client, organization_name, workspace_name, id).await?,
            ComputeCommands::Details {
                organization_name,
                workspace_name,
                id,
            } => {
                print_compute_cluster_details(&client, organization_name, workspace_name, id)
                    .await?
            },
            ComputeCommands::List => print_compute_clusters(&client).await?,
        },
    };

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_env_vars() {
        let cli = Cli::parse_from([
            "pc",
            "compute",
            "start",
            "--env-override",
            "TEST_ARG=3",
            "--env-override",
            "TEST_ARG2=hello",
        ]);
        let command = cli.command.unwrap();
        let Commands::Compute(ComputeArgs {
            command: ComputeCommands::Start { env_override, .. },
        }) = command
        else {
            panic!();
        };
        assert_eq!(
            env_override,
            vec![
                ("TEST_ARG".into(), "3".into()),
                ("TEST_ARG2".into(), "hello".into())
            ]
        )
    }
}

use std::io;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use clap::builder::{NonEmptyStringValueParser, TypedValueParser, ValueParser};
use clap::{Args, CommandFactory, FromArgMatches, Parser, Subcommand};
use client_core::{ApiResult, AutoRefreshApiControlPlaneClient, Client, RUNTIME};
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
use crate::setup::setup;
use crate::workspace::{
    create_workspace, delete_workspace, print_workspace_details, print_workspaces,
};

pub mod compute;
pub mod organization;
pub mod service_account;
pub mod setup;
pub mod workspace;
pub mod workspace_aws;

#[cfg(test)]
mod test_fixtures;

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

fn warn_deprecated(what: &str, use_instead: &str) {
    eprintln!("warning: `{what}` is deprecated since 0.11.0, use `{use_instead}` instead");
}

/// Names an existing workspace, for the commands that operate on one.
#[derive(Args)]
struct WorkspaceRef {
    #[arg(short, long)]
    organization_name: Option<String>,
    #[arg(short, long)]
    workspace_name: String,
}

/// `pc setup` and the deprecated `pc workspace setup` take the same arguments.
#[derive(Args)]
struct SetupArgs {
    #[arg(short, long)]
    organization_name: Option<String>,
    #[arg(short, long)]
    workspace_name: Option<String>,
    #[arg(long, default_value_t = false)]
    connect_aws: bool,
    #[arg(long, default_value_t = false)]
    no_verify: bool,
}

async fn run_setup(client: &Client, args: SetupArgs) -> ApiResult<()> {
    setup(
        client,
        args.organization_name,
        args.workspace_name,
        args.connect_aws.then_some(true),
        !args.no_verify,
    )
    .await
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
    Setup(SetupArgs),
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
        #[arg(short, long)]
        name: String,
    },
    /// Print details of an organization
    Details {
        #[arg(short, long)]
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
    List {
        #[arg(short, long, help = "Only list workspaces in this organization")]
        organization_name: Option<String>,
    },
    /// Create a workspace
    Create {
        #[command(flatten)]
        workspace: WorkspaceRef,
        #[arg(long, default_value_t = false)]
        connect_aws: bool,
        #[arg(long, default_value_t = false)]
        no_verify: bool,
    },
    /// Manage the AWS connection of a workspace
    Aws(WorkspaceAwsArgs),
    /// (deprecated) Set up a workspace, use `create --connect-aws` instead
    Setup(SetupArgs),
    /// (deprecated) Report the AWS connection, use `aws verify` instead
    Verify(WorkspaceRef),
    /// Delete a workspace
    Delete(WorkspaceRef),
    /// Print details of a workspace
    Details(WorkspaceRef),
}

// --- Workspace (AWS) ---

#[derive(Args)]
struct WorkspaceAwsArgs {
    #[command(subcommand)]
    command: WorkspaceAwsCommands,
}

#[derive(Subcommand)]
enum WorkspaceAwsCommands {
    /// Connect an AWS account to a workspace
    Connect {
        #[command(flatten)]
        workspace: WorkspaceRef,
        #[arg(long, default_value_t = false)]
        no_verify: bool,
    },
    /// Disconnect the AWS account from a workspace
    Disconnect(WorkspaceRef),
    /// Report whether AWS is connected to a workspace
    Verify(WorkspaceRef),
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
        #[command(flatten)]
        workspace: WorkspaceRef,
        #[arg(short, long)]
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
    List {
        #[arg(short, long, help = "Only list compute in this organization")]
        organization_name: Option<String>,
        #[arg(short, long, help = "Only list compute in this workspace")]
        workspace_name: Option<String>,
    },
    /// Start a compute cluster
    Start {
        #[command(flatten)]
        workspace: WorkspaceRef,
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
        #[command(flatten)]
        workspace: WorkspaceRef,
        #[arg(short, long)]
        id: Uuid,
    },
    /// Print details of a compute cluster
    Details {
        #[command(flatten)]
        workspace: WorkspaceRef,
        #[arg(short, long)]
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
        let installed = tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug")),
            )
            .try_init()
            .is_ok();
        if installed {
            println!("Logging level set to DEBUG");
        } else {
            println!("Logging already initialized; set RUST_LOG=debug to raise the level");
        }
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
        Commands::Authenticate => {
            client.authenticate(None, None, true).await?;
            println!("Successfully logged in.");
        },
        Commands::Login => {
            client.login().await?;
            println!("Successfully logged in.");
        },
        Commands::Setup(args) => run_setup(&client, args).await?,
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
            WorkspaceCommands::List { organization_name } => {
                print_workspaces(&client, organization_name).await?
            },
            WorkspaceCommands::Create {
                workspace,
                connect_aws,
                no_verify,
            } => {
                create_workspace(
                    &client,
                    workspace.organization_name,
                    workspace.workspace_name,
                    connect_aws,
                    !no_verify,
                )
                .await?
            },
            WorkspaceCommands::Aws(args) => match args.command {
                WorkspaceAwsCommands::Connect {
                    workspace,
                    no_verify,
                } => {
                    workspace_aws::connect(
                        &client,
                        workspace.organization_name,
                        workspace.workspace_name,
                        !no_verify,
                    )
                    .await?
                },
                WorkspaceAwsCommands::Disconnect(workspace) => {
                    workspace_aws::disconnect(
                        &client,
                        workspace.organization_name,
                        workspace.workspace_name,
                    )
                    .await?
                },
                WorkspaceAwsCommands::Verify(workspace) => {
                    workspace_aws::verify(
                        &client,
                        workspace.organization_name,
                        workspace.workspace_name,
                    )
                    .await?
                },
            },
            WorkspaceCommands::Setup(args) => run_setup(&client, args).await?,
            WorkspaceCommands::Verify(workspace) => {
                warn_deprecated("pc workspace verify", "pc workspace aws verify");
                workspace_aws::verify(
                    &client,
                    workspace.organization_name,
                    workspace.workspace_name,
                )
                .await?
            },
            WorkspaceCommands::Delete(workspace) => {
                delete_workspace(
                    &client,
                    workspace.organization_name,
                    workspace.workspace_name,
                )
                .await?
            },
            WorkspaceCommands::Details(workspace) => {
                print_workspace_details(
                    &client,
                    workspace.organization_name,
                    workspace.workspace_name,
                )
                .await?
            },
        },
        Commands::ServiceAccount(args) => match args.command {
            ServiceAccountCommands::Create {
                workspace,
                name,
                description,
            } => {
                create_service_account(
                    &client,
                    workspace.organization_name,
                    workspace.workspace_name,
                    name,
                    description,
                )
                .await?
            },
        },
        Commands::Compute(args) => match args.command {
            ComputeCommands::Start {
                workspace,
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
                    workspace.organization_name,
                    workspace.workspace_name,
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
            ComputeCommands::Stop { workspace, id } => {
                stop_compute_cluster(
                    &client,
                    workspace.organization_name,
                    workspace.workspace_name,
                    id,
                )
                .await?
            },
            ComputeCommands::Details { workspace, id } => {
                print_compute_cluster_details(
                    &client,
                    workspace.organization_name,
                    workspace.workspace_name,
                    id,
                )
                .await?
            },
            ComputeCommands::List {
                organization_name,
                workspace_name,
            } => print_compute_clusters(&client, organization_name, workspace_name).await?,
        },
    };

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_conflicting_args() {
        Cli::command().debug_assert();
    }

    /// `-o` is a disambiguator everywhere, including here -- it used to be the one required one.
    #[test]
    fn test_workspace_create_infers_the_organization() {
        let cli = Cli::parse_from(["pc", "workspace", "create", "-w", "my-ws"]);
        let Some(Commands::Workspace(WorkspaceArgs {
            command: WorkspaceCommands::Create { workspace, .. },
        })) = cli.command
        else {
            panic!();
        };
        assert_eq!(workspace.workspace_name, "my-ws");
        assert_eq!(workspace.organization_name, None);
    }

    #[test]
    fn test_parse_workspace_create() {
        let cli = Cli::parse_from([
            "pc",
            "workspace",
            "create",
            "-w",
            "my-ws",
            "-o",
            "my-org",
            "--connect-aws",
        ]);
        let Some(Commands::Workspace(WorkspaceArgs {
            command:
                WorkspaceCommands::Create {
                    workspace,
                    connect_aws,
                    no_verify,
                },
        })) = cli.command
        else {
            panic!();
        };
        assert_eq!(workspace.workspace_name, "my-ws");
        assert_eq!(workspace.organization_name.as_deref(), Some("my-org"));
        assert!(connect_aws);
        assert!(!no_verify);
    }

    #[test]
    fn test_parse_workspace_aws_subcommands() {
        for (args, expect_no_verify) in [
            (vec!["pc", "workspace", "aws", "connect", "-w", "ws"], false),
            (
                vec![
                    "pc",
                    "workspace",
                    "aws",
                    "connect",
                    "-w",
                    "ws",
                    "--no-verify",
                ],
                true,
            ),
        ] {
            let cli = Cli::parse_from(args);
            let Some(Commands::Workspace(WorkspaceArgs {
                command:
                    WorkspaceCommands::Aws(WorkspaceAwsArgs {
                        command: WorkspaceAwsCommands::Connect { no_verify, .. },
                    }),
            })) = cli.command
            else {
                panic!();
            };
            assert_eq!(no_verify, expect_no_verify);
        }

        let cli = Cli::parse_from(["pc", "workspace", "aws", "disconnect", "-w", "ws"]);
        assert!(matches!(
            cli.command,
            Some(Commands::Workspace(WorkspaceArgs {
                command: WorkspaceCommands::Aws(WorkspaceAwsArgs {
                    command: WorkspaceAwsCommands::Disconnect(..)
                })
            }))
        ));

        let cli = Cli::parse_from(["pc", "workspace", "aws", "verify", "-w", "ws"]);
        let Some(Commands::Workspace(WorkspaceArgs {
            command:
                WorkspaceCommands::Aws(WorkspaceAwsArgs {
                    command: WorkspaceAwsCommands::Verify(workspace),
                }),
        })) = cli.command
        else {
            panic!();
        };
        assert_eq!(workspace.workspace_name, "ws");
    }

    /// Omitting `--connect-aws` has to stay distinct from passing it, so that `setup` knows to
    /// fall back to the interactive prompt rather than assuming "no".
    #[test]
    fn test_setup_connect_aws_is_tri_state() {
        for (argv, expected) in [
            (vec!["pc", "setup"], None),
            (vec!["pc", "setup", "--connect-aws"], Some(true)),
        ] {
            let cli = Cli::parse_from(argv);
            let Some(Commands::Setup(args)) = cli.command else {
                panic!();
            };
            assert_eq!(args.connect_aws.then_some(true), expected);
        }
    }

    #[test]
    fn test_parse_env_vars() {
        let cli = Cli::parse_from([
            "pc",
            "compute",
            "start",
            "-w",
            "my-ws",
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

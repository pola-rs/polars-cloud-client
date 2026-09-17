use anyhow::anyhow;
use client_core::{ApiResult, Client};
use comfy_table::Table;
use comfy_table::presets::NOTHING;
use polars_axum_models::{OrganizationCreateArgs, OrganizationModel};

use crate::get_user_input;

pub async fn get_all_organizations(
    client: &Client,
    name: Option<String>,
) -> ApiResult<Vec<OrganizationModel>> {
    client.get_organizations(name).await
}

/// The organization with exactly this name. Names are not unique, so more than one match is an
/// error the user has to resolve by picking one of them by id.
pub async fn get_organization_by_name(
    client: &Client,
    name: String,
) -> ApiResult<OrganizationModel> {
    let mut matches: Vec<OrganizationModel> = get_all_organizations(client, Some(name.clone()))
        .await?
        .into_iter()
        .filter(|o| o.name == name)
        .collect();

    match matches.len() {
        0 => Err(anyhow!(
            "No organization with the name {name} was found.\n\n\
             Hint: Run `pc organization list` to see the ones you have, or \
             `pc organization setup --name {name}` to create this one."
        )
        .into()),
        1 => Ok(matches.remove(0)),
        // Renaming needs admin, so lead with the id form: that is the one way out
        // that does not depend on someone else's permissions. Wording mirrors
        // `Organization._load_by_name` in the Python client.
        n => {
            let ids = matches
                .iter()
                .map(|o| o.id.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            Err(anyhow!(
                "You have {n} organizations named {name}: {ids}.\n\n\
                 Hint: Refer to the organization by ID with \
                 `pc.Organization(id=UUID('...'))`, or ask an admin of one of them to \
                 rename it in the dashboard under Organization > Settings."
            )
            .into())
        },
    }
}

/// The organization to act in: the named one, or the only one you have when there is no ambiguity.
pub async fn resolve_organization(
    client: &Client,
    name: Option<String>,
) -> ApiResult<OrganizationModel> {
    let Some(name) = name else {
        let mut organizations = get_all_organizations(client, None).await?;

        return match organizations.len() {
            0 => Err(anyhow!(
                "You have no organizations yet. Run `pc organization setup --name <name>` first."
            )
            .into()),
            1 => {
                let organization = organizations.remove(0);
                println!("Using organization {}.", organization.name);
                Ok(organization)
            },
            _ => {
                let names: Vec<_> = organizations.into_iter().map(|o| o.name).collect();
                Err(anyhow!(
                    "You have several organizations. Add `-o <name>` to say which one: {}.",
                    names.join(", ")
                )
                .into())
            },
        };
    };

    get_organization_by_name(client, name).await
}

pub async fn set_up_organization(
    client: &Client,
    organization_name: Option<String>,
) -> ApiResult<OrganizationModel> {
    let name = if let Some(name) = organization_name {
        name
    } else {
        get_user_input("Enter organization name: ").await?
    };

    let organization = client
        .create_organization(OrganizationCreateArgs { name })
        .await?;

    println!("Created organization {}.", organization.name);

    Ok(organization)
}

pub async fn print_organizations(client: &Client) -> ApiResult<()> {
    let organizations = get_all_organizations(client, None).await?;

    if organizations.is_empty() {
        println!("No organizations yet. Run `pc organization setup --name <name>` to create one.");
        return Ok(());
    }

    let mut table = Table::new();
    table.load_preset(NOTHING).set_header(vec!["NAME", "ID"]);

    for org in organizations {
        table.add_row(vec![org.name, org.id.to_string()]);
    }

    println!("{table}");

    Ok(())
}

pub async fn print_organization_details(client: &Client, name: String) -> ApiResult<()> {
    let organization = get_organization_by_name(client, name).await?;

    let mut table = Table::new();
    table.load_preset(NOTHING);
    table.add_row(vec!["Name", &organization.name]);
    table.add_row(vec!["ID", &organization.id.to_string()]);
    table.add_row(vec!["Description", &organization.description]);
    table.add_row(vec!["Tier", &format!("{:?}", organization.tier)]);
    table.add_row(vec!["Created at", &organization.created_at.to_string()]);

    println!("{table}");

    Ok(())
}

pub async fn delete_organization(client: &Client, name: String) -> ApiResult<()> {
    let organization = get_organization_by_name(client, name).await?;

    client.delete_organization(organization.id).await?;

    println!("Deleted organization {}.", organization.name);

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use client_core::MockControlPlaneClient;

    use super::*;
    use crate::test_fixtures::organization;

    #[tokio::test]
    async fn resolve_errors_when_two_organizations_share_the_name() {
        let mut mock = MockControlPlaneClient::new();
        mock.expect_get_organizations()
            .returning(|_| Ok(vec![organization("org"), organization("org")]));
        let client: Client = Arc::new(mock);

        let error = resolve_organization(&client, Some("org".into()))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("2 organizations named org"));
    }

    #[tokio::test]
    async fn resolve_picks_the_exact_match_among_substring_hits() {
        let mut mock = MockControlPlaneClient::new();
        mock.expect_get_organizations()
            .returning(|_| Ok(vec![organization("org"), organization("org-2")]));
        let client: Client = Arc::new(mock);

        let organization = resolve_organization(&client, Some("org".into()))
            .await
            .unwrap();
        assert_eq!(organization.name, "org");
    }
}
